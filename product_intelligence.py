"""
Module Name: product_intelligence.py

Purpose:
    Product-centric enrichment layer for Phase 3A of the pipeline. For every
    selected product, this module fetches that product's own page HTML
    exactly once and derives every enrichment field - name, brand, category,
    image, price, availability, specifications, FAQ, aggregate rating,
    rating count, and website (on-page) reviews - from that single
    downloaded payload. No field in this module ever triggers a second
    request to the same product URL.

Responsibilities:
    - Fetch each product's page with a single bounded HTTP GET.
    - Parse the page once (BeautifulSoup + JSON-LD flattening) and reuse
      that parsed representation across every extractor.
    - Extract core product fields from schema.org Product JSON-LD, falling
      back to OpenGraph/product meta tags on the same page when no usable
      JSON-LD is present.
    - Extract specifications, FAQ entries, aggregate rating/rating count,
      and on-page website reviews from the same parsed payload.
    - Discover and, when present, fetch a single structured review-provider
      endpoint (Judge.me, Yotpo, Loox, Stamped, Bazaarvoice, PowerReviews,
      or a native store endpoint) already referenced on the product page.
    - Fall back to a generic, provider-agnostic HTML review scan when no
      structured endpoint is discoverable.
    - Merge and de-duplicate reviews collected from JSON-LD, the structured
      provider, and the generic HTML scan into a single capped list.
    - Degrade gracefully to already-known discovery fields whenever a
      fetch fails, times out, or extraction raises, without ever leaving
      the caller with an empty or incomplete object.
    - Run this enrichment concurrently across a batch of products, bounded
      by config.MAX_PARALLEL_TASKS.

Architecture:
    Public entry points (the only things other modules should call):
        await build_product_intelligence(product) -> ProductIntelligence
        await build_product_intelligence_batch(products) -> List[ProductIntelligence]

    `product` may be a `SelectedProduct` dataclass instance (see app.py), a
    plain dict with name/url/brand/image/category keys, or any object
    exposing those as attributes - only `.name`/`.url`/`.brand`/`.image`/
    `.category` (or dict equivalents) are read.

    Internally the module is organized into: networking (one GET per
    product page, plus at most one additional GET to a discovered
    structured review endpoint), JSON-LD flattening/lookup, core field
    extraction with a meta-tag fallback, specification/FAQ extraction,
    rating extraction, and website-review extraction (JSON-LD, structured
    provider, and generic HTML, merged and de-duplicated).

    Explicitly out of scope for this module (handled in later phases):
    YouTube/Reddit scraping, the Recommendation Engine, Aspect Sentiment,
    and Confidence Score. This module does not touch browser pooling,
    sentiment analysis, PDF generation, or the Flask/FastAPI routes - it is
    a self-contained, additive enrichment step that analyze_selected()
    calls once per batch of selected products.

Enrichment Pipeline:
    1. Resolve fallback fields (name/brand/category/url/image) from the
       caller-supplied product.
    2. If no URL is present, return immediately with fetch_status="skipped".
    3. Fetch the product page HTML exactly once, bounded by
       PRODUCT_PAGE_TIMEOUT_SECONDS.
    4. Parse the HTML once into a BeautifulSoup tree and a flattened list
       of JSON-LD nodes.
    5. Extract core fields from the schema.org Product node, or from
       OpenGraph/product meta tags when no usable Product JSON-LD exists.
    6. Extract price (JSON-LD offers, else a price-classed element),
       category (JSON-LD, else breadcrumb, else discovery fallback),
       specifications, FAQ entries, and aggregate rating/rating count.
    7. Extract website reviews in priority order: schema.org Product.review
       JSON-LD, then a discovered structured review-provider endpoint
       (at most one extra request), then a generic HTML review scan -
       merging and de-duplicating the results.
    8. Populate and return a ProductIntelligence instance, marking
       fetch_status as "success", "failed", or "skipped" as appropriate.

Inputs:
    A `SelectedProduct`-like object, dict, or any object exposing
    name/url/brand/image/category (single product), or a list of such
    objects (batch). Network input is the HTML of each product's own page,
    and - at most once per product - the JSON response of a discovered
    structured review-provider endpoint.

Outputs:
    A single `ProductIntelligence` instance, or a `List[ProductIntelligence]`
    in the same order as the input batch. Each instance exposes name,
    brand, category, url, image, price, availability, specifications, faq,
    aggregate_rating, rating_count, website_reviews, and a diagnostic-only
    fetch_status ("success" | "failed" | "skipped").

Dependencies:
    asyncio, json, logging, re, dataclasses, typing, urllib.parse.urljoin,
    httpx, bs4.BeautifulSoup, config (MAX_PARALLEL_TASKS), and
    product_extraction (extract_price, parse_breadcrumb_category).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

import config
from product_extraction import extract_price, parse_breadcrumb_category

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
FETCH_TIMEOUT_SECONDS = 10.0
FETCH_CONNECT_TIMEOUT_SECONDS = 6.0

PRODUCT_PAGE_TIMEOUT_SECONDS = 15

MAX_SPECIFICATIONS = 30
MAX_FAQ_ITEMS = 20
MAX_WEBSITE_REVIEWS = 20
MAX_REVIEW_BODY_CHARS = 500

STRUCTURED_REVIEW_TIMEOUT_SECONDS = 8.0


@dataclass
class ProductIntelligence:
    name: str = ""
    brand: str = ""
    category: str = ""
    url: str = ""
    image: str = ""
    price: str = ""
    availability: str = ""
    specifications: Dict[str, str] = field(default_factory=dict)
    faq: List[Dict[str, str]] = field(default_factory=list)
    aggregate_rating: Optional[float] = None
    rating_count: Optional[int] = None
    website_reviews: List[Dict[str, str]] = field(default_factory=list)
    fetch_status: str = "skipped"

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


async def build_product_intelligence(product: Any) -> ProductIntelligence:
    fallback = _extract_fallback_fields(product)
    fallback_name = fallback["name"]
    fallback_url = fallback["url"]
    fallback_brand = fallback["brand"]
    fallback_image = fallback["image"]
    fallback_category = fallback["category"]

    intel = ProductIntelligence(**fallback)

    if not fallback_url:
        intel.fetch_status = "skipped"
        return intel

    try:
        html = await asyncio.wait_for(
            _fetch_product_html(fallback_url),
            timeout=PRODUCT_PAGE_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Product Intelligence fetch for %r exceeded its %.0fs hard "
            "timeout - falling back to known discovery fields.",
            fallback_name or fallback_url, PRODUCT_PAGE_TIMEOUT_SECONDS,
        )
        html = ""
    except Exception:
        logger.exception(
            "Product Intelligence fetch failed unexpectedly for %r",
            fallback_name or fallback_url,
        )
        html = ""

    if not html:
        intel.fetch_status = "failed"
        return intel

    try:
        soup = BeautifulSoup(html, "html.parser")
        nodes = _flatten_jsonld_nodes(html)
        product_node = _find_product_node(nodes)
        faq_nodes = _find_faqpage_nodes(nodes)

        core = _extract_core_fields(product_node, fallback_url) if product_node else {}
        if not any(core.get(k) for k in ("name", "image", "price")):
            core = _merge_fields(core, _extract_meta_fallback(soup, fallback_url))

        price = core.get("price") or _extract_price_from_page(soup)
        category = core.get("category") or parse_breadcrumb_category(html) or fallback_category
        rating_info = _extract_rating(product_node)

        intel.name = fallback_name or core.get("name", "")
        intel.brand = core.get("brand") or fallback_brand
        intel.category = category
        intel.image = core.get("image") or fallback_image
        intel.price = price
        intel.availability = core.get("availability", "")
        intel.specifications = _extract_specifications(soup)
        intel.faq = _extract_faq(faq_nodes, soup)
        intel.aggregate_rating = rating_info["aggregate_rating"]
        intel.rating_count = rating_info["rating_count"]

        jsonld_reviews = _extract_website_reviews(product_node)
        structured_reviews: List[Dict[str, str]] = []
        try:
            endpoint = _discover_structured_review_endpoint(html)
            if endpoint:
                structured_reviews = await asyncio.wait_for(
                    _fetch_structured_reviews(endpoint),
                    timeout=STRUCTURED_REVIEW_TIMEOUT_SECONDS,
                )
        except Exception:
            logger.info(
                "Structured review lookup failed for %r - falling back to "
                "generic HTML extraction.", fallback_name or fallback_url,
            )
            structured_reviews = []

        generic_reviews: List[Dict[str, str]] = []
        if not structured_reviews:
            generic_reviews = _extract_generic_html_reviews(soup)

        intel.website_reviews = _merge_website_reviews(
            jsonld_reviews, structured_reviews, generic_reviews,
        )
        intel.fetch_status = "success"
    except Exception:
        logger.exception(
            "Product Intelligence extraction failed for %r after a "
            "successful fetch - returning discovery fallback fields only.",
            fallback_name or fallback_url,
        )
        intel.fetch_status = "failed"

    return intel


async def build_product_intelligence_batch(products: List[Any]) -> List["ProductIntelligence"]:
    if not products:
        return []

    semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)

    async def _one(item: Any) -> ProductIntelligence:
        async with semaphore:
            try:
                return await build_product_intelligence(item)
            except Exception:
                logger.exception(
                    "Product Intelligence batch job failed unexpectedly "
                    "for %r - degrading to known discovery fields.", item,
                )
                return ProductIntelligence(**_extract_fallback_fields(item), fetch_status="failed")

    return list(await asyncio.gather(*(_one(p) for p in products)))


def _get_field(product: Any, field_name: str) -> str:
    if isinstance(product, dict):
        return str(product.get(field_name) or "").strip()
    return str(getattr(product, field_name, "") or "").strip()


def _extract_fallback_fields(product: Any) -> Dict[str, str]:
    return {
        "name": _get_field(product, "name"),
        "brand": _get_field(product, "brand"),
        "category": _get_field(product, "category"),
        "url": _get_field(product, "url"),
        "image": _get_field(product, "image"),
    }


def _merge_fields(primary: Dict[str, str], fallback: Dict[str, str]) -> Dict[str, str]:
    merged = dict(fallback)
    merged.update({k: v for k, v in primary.items() if v})
    return merged


async def _fetch_product_html(url: str) -> str:
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(FETCH_TIMEOUT_SECONDS, connect=FETCH_CONNECT_TIMEOUT_SECONDS),
            follow_redirects=True,
            headers=HEADERS,
            verify=False,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
    except Exception as exc:
        logger.info("Product Intelligence fetch failed for %s: %s", url, exc)
        return ""


def _flatten_jsonld_nodes(html: str) -> List[Dict]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    nodes: List[Dict] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (tag.string or tag.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        nodes.extend(_flatten(data))
    return nodes


def _flatten(data: Any) -> List[Dict]:
    out: List[Dict] = []
    if isinstance(data, list):
        for item in data:
            out.extend(_flatten(item))
    elif isinstance(data, dict):
        out.append(data)
        if isinstance(data.get("@graph"), list):
            for item in data["@graph"]:
                out.extend(_flatten(item))
        for key in ("itemListElement", "mainEntity", "hasVariant"):
            if key in data:
                out.extend(_flatten(data[key]))
    return out


def _schema_type(node: Dict) -> str:
    t = node.get("@type", "")
    if isinstance(t, list):
        t = t[0] if t else ""
    return str(t).lower()


def _find_product_node(nodes: List[Dict]) -> Dict:
    for node in nodes:
        if _schema_type(node) == "product":
            return node
    return {}


def _find_faqpage_nodes(nodes: List[Dict]) -> List[Dict]:
    return [n for n in nodes if _schema_type(n) == "faqpage"]


def _extract_core_fields(product_node: Dict, page_url: str) -> Dict[str, str]:
    name = str(product_node.get("name") or "").strip()

    brand = product_node.get("brand")
    if isinstance(brand, dict):
        brand = brand.get("name", "")
    brand = str(brand or "").strip()

    image = product_node.get("image")
    if isinstance(image, list):
        image = image[0] if image else ""
    if isinstance(image, dict):
        image = image.get("url", "")
    image = str(image or "").strip()
    if image:
        image = urljoin(page_url, image)

    category = str(product_node.get("category") or "").strip()

    offers = product_node.get("offers")
    price, availability = "", ""
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        price_val = offers.get("price") or offers.get("lowPrice")
        currency = offers.get("priceCurrency", "")
        if price_val:
            price = f"{currency} {price_val}".strip()
        availability = str(offers.get("availability") or "").rsplit("/", 1)[-1]

    return {
        "name": name,
        "brand": brand,
        "image": image,
        "category": category,
        "price": price,
        "availability": availability,
    }


def _extract_meta_fallback(soup: BeautifulSoup, page_url: str) -> Dict[str, str]:

    def _meta(*names: str) -> str:
        for n in names:
            tag = soup.find("meta", attrs={"property": n}) or soup.find("meta", attrs={"name": n})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return ""

    image = _meta("og:image", "twitter:image")
    if image:
        image = urljoin(page_url, image)

    price = _meta("product:price:amount", "og:price:amount")
    currency = _meta("product:price:currency", "og:price:currency")
    if price and currency:
        price = f"{currency} {price}"

    return {
        "name": _meta("og:title"),
        "brand": _meta("product:brand", "og:brand"),
        "image": image,
        "category": _meta("product:category", "og:type"),
        "price": price,
        "availability": _meta("product:availability", "og:availability"),
    }


def _extract_price_from_page(soup: BeautifulSoup) -> str:
    price_el = soup.find(class_=re.compile(r"price", re.IGNORECASE))
    if price_el is not None:
        price = extract_price(price_el.get_text(" ", strip=True))
        if price:
            return price
    return ""


def _extract_specifications(soup: BeautifulSoup) -> Dict[str, str]:
    specs: Dict[str, str] = {}

    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            if len(specs) >= MAX_SPECIFICATIONS:
                return specs
            key = dt.get_text(" ", strip=True)
            val = dd.get_text(" ", strip=True)
            if key and val and key not in specs:
                specs[key] = val

    spec_tables = soup.select(
        "[class*=spec i] table, [id*=spec i] table, "
        "[class*=specification i] table, [class*=attribute i] table, "
        "table[class*=spec i]"
    )
    for table in spec_tables:
        for row in table.find_all("tr"):
            if len(specs) >= MAX_SPECIFICATIONS:
                return specs
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                key = cells[0].get_text(" ", strip=True)
                val = cells[1].get_text(" ", strip=True)
                if key and val and key not in specs:
                    specs[key] = val

    if len(specs) < 3:
        for container in soup.select("[class*=spec i], [id*=spec i], [class*=detail i]"):
            for row in container.select("li, tr, div"):
                if len(specs) >= MAX_SPECIFICATIONS:
                    return specs
                text = row.get_text(" ", strip=True)
                if ":" in text and 3 < len(text) < 120:
                    key, _, val = text.partition(":")
                    key, val = key.strip(), val.strip()
                    if key and val and key not in specs:
                        specs[key] = val

    return specs


def _extract_faq(faq_nodes: List[Dict], soup: BeautifulSoup) -> List[Dict[str, str]]:
    faqs: List[Dict[str, str]] = []
    seen: set = set()

    for node in faq_nodes:
        for item in node.get("mainEntity", []) or []:
            if not isinstance(item, dict):
                continue
            question = str(item.get("name") or "").strip()
            answer_node = item.get("acceptedAnswer") or {}
            answer = str(answer_node.get("text") or "").strip() if isinstance(answer_node, dict) else ""
            if question and answer and question.lower() not in seen:
                seen.add(question.lower())
                faqs.append({"question": question, "answer": answer})
            if len(faqs) >= MAX_FAQ_ITEMS:
                return faqs

    if faqs:
        return faqs

    for container in soup.select("[class*=faq i], [id*=faq i]"):
        for q_el in container.select("[class*=question i], summary, h3, h4, dt"):
            if len(faqs) >= MAX_FAQ_ITEMS:
                return faqs
            question = q_el.get_text(" ", strip=True)
            if not question or len(question) > 200:
                continue
            answer_el = q_el.find_next_sibling()
            answer = answer_el.get_text(" ", strip=True) if answer_el is not None else ""
            if question and answer and question.lower() not in seen:
                seen.add(question.lower())
                faqs.append({"question": question, "answer": answer})

    return faqs


def _extract_rating(product_node: Dict) -> Dict[str, Optional[float]]:
    agg = product_node.get("aggregateRating") if product_node else None
    if isinstance(agg, list):
        agg = agg[0] if agg else {}
    if not isinstance(agg, dict):
        return {"aggregate_rating": None, "rating_count": None}

    rating_value = _to_float(agg.get("ratingValue"))
    count = _to_int(agg.get("reviewCount") or agg.get("ratingCount"))
    return {"aggregate_rating": rating_value, "rating_count": count}


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _truncate_review_text(text: str) -> str:
    if len(text) > MAX_REVIEW_BODY_CHARS:
        return text[: MAX_REVIEW_BODY_CHARS - 3].rstrip() + "..."
    return text


def _build_review(author: str, rating: str, text: str, date_val: str) -> Dict[str, str]:
    return {
        "author": author or "Anonymous",
        "rating": rating,
        "text": text,
        "date": date_val,
    }


def _extract_website_reviews(product_node: Dict) -> List[Dict[str, str]]:
    if not product_node:
        return []
    reviews_raw = product_node.get("review")
    if isinstance(reviews_raw, dict):
        reviews_raw = [reviews_raw]
    if not isinstance(reviews_raw, list):
        return []

    reviews: List[Dict[str, str]] = []
    for r in reviews_raw:
        if not isinstance(r, dict):
            continue
        author = r.get("author")
        if isinstance(author, dict):
            author = author.get("name", "")
        author = str(author or "").strip()

        body = str(r.get("reviewBody") or r.get("description") or "").strip()
        body = _truncate_review_text(body)

        rating = ""
        rating_node = r.get("reviewRating")
        if isinstance(rating_node, dict):
            rating = str(rating_node.get("ratingValue") or "").strip()

        date_published = str(r.get("datePublished") or "").strip()

        if body or author:
            reviews.append(_build_review(author, rating, body, date_published))
        if len(reviews) >= MAX_WEBSITE_REVIEWS:
            break

    return reviews


REVIEW_PROVIDER_DOMAINS = (
    "judge.me",
    "yotpo.com",
    "loox.io",
    "stamped.io",
    "bazaarvoice.com",
    "powerreviews.com",
)

_REVIEW_ENDPOINT_RE = re.compile(
    r"https?://[^\s\"'<>]*(?:" + "|".join(re.escape(d) for d in REVIEW_PROVIDER_DOMAINS) + r")[^\s\"'<>]*",
    re.IGNORECASE,
)

_STATIC_ASSET_RE = re.compile(r"\.(?:js|css|png|jpe?g|gif|svg|woff2?|ttf)(?:\?|$)", re.IGNORECASE)

_ENDPOINT_HINT_RE = re.compile(r"(?:/api/|/widgets?/|\.json|\?)", re.IGNORECASE)


def _discover_structured_review_endpoint(html: str) -> str:
    if not html:
        return ""
    for match in _REVIEW_ENDPOINT_RE.finditer(html):
        candidate = match.group(0).rstrip(").,;\\'\"")
        if _STATIC_ASSET_RE.search(candidate):
            continue
        if _ENDPOINT_HINT_RE.search(candidate):
            return candidate
    return ""


async def _fetch_structured_reviews(endpoint: str) -> List[Dict[str, str]]:
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(STRUCTURED_REVIEW_TIMEOUT_SECONDS, connect=FETCH_CONNECT_TIMEOUT_SECONDS),
            follow_redirects=True,
            headers=HEADERS,
            verify=False,
        ) as client:
            resp = await client.get(endpoint)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.info("Structured review endpoint fetch failed for %s: %s", endpoint, exc)
        return []
    return _parse_generic_review_payload(data)


_REVIEW_TEXT_KEYS = ("body", "content", "review", "comment", "text", "description", "reviewBody", "review_body")
_REVIEW_AUTHOR_KEYS = ("author", "user", "name", "reviewer", "nickname", "display_name", "reviewer_name", "user_name")
_REVIEW_RATING_KEYS = ("rating", "score", "stars", "review_rating", "star_rating")
_REVIEW_DATE_KEYS = ("date", "created_at", "date_published", "timestamp", "review_date")


def _first_value(item: Dict, keys: "tuple[str, ...]", require_not_none: bool = False) -> str:
    if require_not_none:
        return next((str(item[k]).strip() for k in keys if item.get(k) is not None), "")
    return next((str(item[k]).strip() for k in keys if item.get(k)), "")


def _find_review_list(node: Any, depth: int = 0) -> List[Dict]:
    if depth > 6:
        return []
    if isinstance(node, list):
        dict_items = [i for i in node if isinstance(i, dict)]
        if dict_items and any(any(k in i for k in _REVIEW_TEXT_KEYS) for i in dict_items):
            return dict_items
        for item in node:
            found = _find_review_list(item, depth + 1)
            if found:
                return found
    elif isinstance(node, dict):
        for value in node.values():
            found = _find_review_list(value, depth + 1)
            if found:
                return found
    return []


def _parse_generic_review_payload(data: Any) -> List[Dict[str, str]]:
    raw_reviews = _find_review_list(data)
    reviews: List[Dict[str, str]] = []
    for item in raw_reviews:
        text = _first_value(item, _REVIEW_TEXT_KEYS)
        if not text:
            continue
        text = _truncate_review_text(text)
        author = _first_value(item, _REVIEW_AUTHOR_KEYS)
        rating = _first_value(item, _REVIEW_RATING_KEYS, require_not_none=True)
        date_val = _first_value(item, _REVIEW_DATE_KEYS)
        reviews.append(_build_review(author, rating, text, date_val))
        if len(reviews) >= MAX_WEBSITE_REVIEWS:
            break
    return reviews


def _extract_generic_html_reviews(soup: BeautifulSoup) -> List[Dict[str, str]]:
    candidates = soup.select('[itemprop="review"], [itemtype*="Review" i]')
    if not candidates:
        candidates = soup.select(
            '[class*="spr-review" i], '
            '[class*="review-item" i], [class*="review-card" i], '
            '[class*="review-entry" i], [class*="review-content" i], '
            '[class*="review_item" i], [id*="review-item" i]'
        )

    reviews: List[Dict[str, str]] = []
    seen: set = set()
    text_hints = ("reviewbody", "body", "content", "text", "description")
    author_hints = ("author", "reviewer", "nickname", "byline", "name", "user")
    date_hints = ("datepublished", "date", "time")

    for block in candidates:
        text = _find_block_text(block, text_hints) or block.get_text(" ", strip=True)
        text = text.strip()
        if len(text) < 15:
            continue
        key = text.lower()[:120]
        if key in seen:
            continue
        seen.add(key)

        author = _find_block_text(block, author_hints)
        rating = _find_block_rating(block)
        date_val = _find_block_text(block, date_hints)
        text = _truncate_review_text(text)

        reviews.append(_build_review(author, rating, text, date_val))
        if len(reviews) >= MAX_WEBSITE_REVIEWS:
            break

    return reviews


def _find_block_text(block: Any, hints: "tuple[str, ...]") -> str:
    for hint in hints:
        el = block.find(attrs={"itemprop": re.compile(hint, re.IGNORECASE)})
        if el is not None:
            text = (el.get("content") or el.get_text(" ", strip=True) or "").strip()
            if text:
                return text
    for hint in hints:
        el = block.find(class_=re.compile(hint, re.IGNORECASE))
        if el is not None:
            text = el.get_text(" ", strip=True)
            if text:
                return text
    return ""


def _find_block_rating(block: Any) -> str:
    el = block.find(attrs={"itemprop": re.compile("ratingvalue", re.IGNORECASE)})
    if el is not None:
        val = el.get("content") or el.get_text(" ", strip=True)
        if val:
            return str(val).strip()

    for el in block.find_all(attrs={"aria-label": True}):
        m = re.search(r"(\d(?:\.\d)?)\s*(?:out of|/)\s*5", el["aria-label"], re.IGNORECASE)
        if m:
            return m.group(1)

    rated = block.find(attrs={"data-rating": True})
    if rated is not None:
        return str(rated["data-rating"]).strip()

    star_els = block.select('[class*="star" i]')
    if star_els:
        filled = [
            s for s in star_els
            if not re.search(r"empty|blank|outline|off\b", " ".join(s.get("class", [])), re.IGNORECASE)
        ]
        if filled:
            return str(len(filled))

    return ""


def _merge_website_reviews(*groups: List[Dict[str, str]]) -> List[Dict[str, str]]:
    merged: List[Dict[str, str]] = []
    seen: set = set()
    for group in groups:
        for review in group:
            text = (review.get("text") or "").strip()
            if not text:
                continue
            author = (review.get("author") or "").strip().lower()
            key = (text.lower()[:120], author)
            if key in seen:
                continue
            seen.add(key)
            merged.append(review)
            if len(merged) >= MAX_WEBSITE_REVIEWS:
                return merged
    return merged
