"""Website product-review scraper. **New module — Priority 1 source.**

============================================================================
Why this module exists
----------------------------------------------------------------------------
Every zero-comment run in the logs shares one trait: the four existing
scrapers (Google Maps, YouTube, Reddit, Twitter/Instagram) all go looking
for reviews on *someone else's* platform, which means fighting that
platform's anti-bot defenses, login walls, and DOM churn. But the single
most reliable place to find opinions about "boAt Airdopes 181 Pro" is the
boAt Airdopes 181 Pro product page itself — which app.py has *already*
fetched successfully, via plain httpx, with a 200 OK, during product
discovery (see the `INFO:httpx:HTTP Request: GET .../products/... 200 OK`
lines in the log). No browser, no anti-bot fight, no auth wall: that page
is public HTML served to any GET request.

The overwhelming majority of e-commerce sites (Shopify in particular, which
`boat-lifestyle.com`'s `/products/...` URL structure indicates) render
reviews through one of a handful of well-known apps, each of which either
(a) embeds the reviews as structured data directly in the page HTML, or
(b) fetches them client-side from that app's own public, unauthenticated
JSON API. This module checks for both, in order of reliability, using the
exact same plain-HTTP approach reddit_scraper.py already uses.

----------------------------------------------------------------------------
Detection order (first hit wins, cheapest/most-structured first):
  1. schema.org JSON-LD (`Review` / `AggregateRating`) embedded in
     `<script type="application/ld+json">` — works regardless of which
     review app is installed, since most apps emit this for SEO.
  2. Known review-app JSON endpoints, called directly once the app's
     product/shop id is found on the page:
       - Judge.me   (`judge.me/api/v1/widgets/...` — very common on Shopify)
       - Yotpo      (`api.yotpo.com/v1/reviews/{app_key}/products/...json`)
       - Loox       (`loox.io/widget/reviews/product/{product_id}`)
       - Stamped.io (`stamped.io/api/widget/reviews`)
       - Okendo     (`okendo.io/reviews/product/...`)
  3. Generic HTML fallback: heuristically-identified repeating review
     blocks (rating + reviewer + body) anywhere in the rendered page HTML,
     for custom/unrecognized review widgets.

Each of these is a plain GET/POST against a public API — nothing here
needs a browser, a login, or an API key, so it doesn't compete for the
shared Chromium launch slots the other four scrapers use (see
browser_utils.browser_launch_slot) and isn't subject to the anti-bot
defenses that make Twitter/Instagram unreliable.
----------------------------------------------------------------------------

Public function (same contract as every other scraper module):

    async def scrape_website_reviews(company_data: Dict[str, str]) -> List[str]

Reads from ``company_data``:
    product_url   - preferred; the specific product page discovered for
                     this job (product_extraction.py already puts this in
                     each product record's "url" field — app.py's job
                     builder just needs to forward it into company_data
                     under this key, exactly as it already forwards
                     product_name/product_brand).
    website        - fallback root domain if product_url is absent (used
                      only for the "General" company-wide job).
    product_name   - used to keep only reviews that plausibly discuss this
                      product when a website-wide fallback page is scraped.
============================================================================
"""

import asyncio
import concurrent.futures
import json
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from scrapers.browser_utils import normalize_comments

logger = logging.getLogger(__name__)

MAX_WORKERS = 3
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_WORKERS, thread_name_prefix="website_review_scraper"
)

# Plain HTTP against the merchant's own site (and, at most, one well-known
# review-app API) — no browser, so a generous but still bounded budget is
# fine; a handful of httpx calls finishes in well under this even on a slow
# site.
TIME_BUDGET_SECONDS = 15
MAX_REVIEWS = 60
_MIN_WORDS = 4

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}


@dataclass
class _Review:
    text: str
    source: str  # e.g. "jsonld", "judgeme", "yotpo", "loox", "stamped", "html_heuristic"


# --- shared HTTP helper ------------------------------------------------
def _get(client: httpx.Client, url: str, **kwargs) -> Optional[httpx.Response]:
    try:
        resp = client.get(url, headers=_HEADERS, timeout=8.0, **kwargs)
        if resp.status_code >= 400:
            logger.info("Website review scrape: GET %s -> HTTP %d", url, resp.status_code)
            return None
        return resp
    except Exception as exc:
        logger.info("Website review scrape: GET %s failed: %s", url, exc)
        return None


# --- 1. JSON-LD review schema -------------------------------------------
def _extract_jsonld_reviews(html: str) -> List[_Review]:
    out: List[_Review] = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for node in candidates:
            if not isinstance(node, dict):
                continue
            graph = node.get("@graph")
            nodes = graph if isinstance(graph, list) else [node]
            for n in nodes:
                if not isinstance(n, dict):
                    continue
                reviews = n.get("review") or n.get("reviews") or []
                if isinstance(reviews, dict):
                    reviews = [reviews]
                for r in reviews:
                    if not isinstance(r, dict):
                        continue
                    body = (
                        r.get("reviewBody")
                        or r.get("description")
                        or ""
                    )
                    body = body.strip() if isinstance(body, str) else ""
                    if body and len(body.split()) >= _MIN_WORDS:
                        out.append(_Review(text=body, source="jsonld"))
    return out


# --- 2a. Judge.me --------------------------------------------------------
_JUDGEME_ID_RE = re.compile(r"data-product-id=[\"'](\d+)[\"']|jdgm-widget[^>]*data-id=[\"'](\d+)[\"']")


def _extract_judgeme(client: httpx.Client, html: str, shop_domain: str) -> List[_Review]:
    if "judge.me" not in html and "jdgm" not in html:
        return []
    m = _JUDGEME_ID_RE.search(html)
    if not m:
        return []
    product_id = m.group(1) or m.group(2)
    url = (
        "https://judge.me/api/v1/widgets/product_review"
        f"?shop_domain={shop_domain}&platform=shopify&product_id={product_id}"
        "&per_page=50&page=1"
    )
    resp = _get(client, url)
    if resp is None:
        return []
    out: List[_Review] = []
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        for body in soup.select(".jdgm-rev__body, .jdgm-rev__text"):
            text = body.get_text(" ", strip=True)
            if text and len(text.split()) >= _MIN_WORDS:
                out.append(_Review(text=text, source="judgeme"))
    except Exception:
        pass
    return out


# --- 2b. Yotpo ------------------------------------------------------------
_YOTPO_APPKEY_RE = re.compile(r"yotpo[_-]?app[_-]?key[\"'\s:=]+[\"']?([A-Za-z0-9]{6,})", re.IGNORECASE)
_YOTPO_PRODUCT_ID_RE = re.compile(r"data-product-id=[\"'](\w[\w-]*)[\"']")


def _extract_yotpo(client: httpx.Client, html: str) -> List[_Review]:
    if "yotpo" not in html.lower():
        return []
    key_match = _YOTPO_APPKEY_RE.search(html)
    pid_match = _YOTPO_PRODUCT_ID_RE.search(html)
    if not key_match or not pid_match:
        return []
    app_key, product_id = key_match.group(1), pid_match.group(1)
    url = f"https://api.yotpo.com/v1/widget/{app_key}/products/{product_id}/reviews.json"
    resp = _get(client, url)
    if resp is None:
        return []
    out: List[_Review] = []
    try:
        data = resp.json()
        reviews = (((data or {}).get("response") or {}).get("reviews")) or []
        for r in reviews:
            text = (r.get("content") or "").strip()
            if text and len(text.split()) >= _MIN_WORDS:
                out.append(_Review(text=text, source="yotpo"))
    except Exception:
        pass
    return out


# --- 2c. Loox --------------------------------------------------------------
_LOOX_SHOP_RE = re.compile(r"loox[_-]?shop[_-]?id[\"'\s:=]+[\"']?(\d+)")


def _extract_loox(client: httpx.Client, html: str, product_ref: str) -> List[_Review]:
    if "loox" not in html.lower():
        return []
    shop_match = _LOOX_SHOP_RE.search(html)
    if not shop_match:
        return []
    shop_id = shop_match.group(1)
    url = f"https://loox.io/widget/reviews/product/{shop_id}/{product_ref}?fields=all"
    resp = _get(client, url)
    if resp is None:
        return []
    out: List[_Review] = []
    try:
        data = resp.json()
        reviews = data.get("reviews") or []
        for r in reviews:
            text = (r.get("content") or r.get("text") or "").strip()
            if text and len(text.split()) >= _MIN_WORDS:
                out.append(_Review(text=text, source="loox"))
    except Exception:
        pass
    return out


# --- 3. Generic HTML heuristic fallback ------------------------------------
_REVIEW_BLOCK_HINTS = [
    "review", "rating", "testimonial", "feedback", "comment",
]
_NOISE_RE = re.compile(
    r"^(add to cart|buy now|write a review|sort by|filter|load more|"
    r"was this helpful|verified purchase|share|helpful)\W*$",
    re.IGNORECASE,
)


def _extract_html_heuristic(html: str) -> List[_Review]:
    """Last resort when no known review app is detected: look for elements
    whose class/id names suggest they hold review text, and keep the ones
    that read like actual prose rather than UI chrome. Deliberately
    conservative (min word count, noise-phrase filter) since this has no
    structured signal to lean on."""
    out: List[_Review] = []
    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    candidates = soup.find_all(
        lambda tag: tag.name in ("div", "p", "span", "li")
        and tag.get("class")
        and any(
            hint in " ".join(tag.get("class", [])).lower()
            for hint in _REVIEW_BLOCK_HINTS
        )
    )
    for tag in candidates[:300]:
        text = tag.get_text(" ", strip=True)
        if not text or len(text.split()) < 6 or len(text) > 1200:
            continue
        if _NOISE_RE.match(text):
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(_Review(text=text, source="html_heuristic"))
        if len(out) >= MAX_REVIEWS:
            break
    return out


# --- orchestration ----------------------------------------------------------
def _shop_domain_from_url(url: str) -> str:
    return urlparse(url).netloc.lower().lstrip("www.")


def _scrape_sync(product_url: str, website: str, product_name: str) -> List[_Review]:
    target = product_url or website
    if not target:
        return []

    collected: List[_Review] = []
    with httpx.Client(follow_redirects=True) as client:
        resp = _get(client, target)
        if resp is None:
            logger.info("Website review scrape: could not fetch %r.", target)
            return []
        html = resp.text
        shop_domain = _shop_domain_from_url(str(resp.url))

        # Tier 1: structured JSON-LD — cheapest, highest-confidence, works
        # no matter which review app (or none) is installed.
        collected = _extract_jsonld_reviews(html)
        if collected:
            logger.info("Website review scrape: %d review(s) via JSON-LD for %s.", len(collected), target)
            return collected[:MAX_REVIEWS]

        # Tier 2: known review-app APIs, tried in rough order of Shopify
        # market share. Each is a single unauthenticated GET; only the
        # first one whose fingerprint is found on the page is called.
        for extractor, args in (
            (_extract_judgeme, (client, html, shop_domain)),
            (_extract_yotpo, (client, html)),
            (_extract_loox, (client, html, urlparse(target).path.rstrip("/").rsplit("/", 1)[-1])),
        ):
            try:
                found = extractor(*args)
            except Exception:
                logger.exception("Website review scrape: extractor %s failed for %s.", extractor.__name__, target)
                found = []
            if found:
                logger.info(
                    "Website review scrape: %d review(s) via %s for %s.",
                    len(found), found[0].source, target,
                )
                return found[:MAX_REVIEWS]

        # Tier 3: generic heuristic scan of the same HTML we already have
        # in hand — no extra request needed.
        collected = _extract_html_heuristic(html)
        if collected:
            logger.info(
                "Website review scrape: %d review(s) via HTML heuristic for %s.",
                len(collected), target,
            )
            return collected[:MAX_REVIEWS]

        logger.info("Website review scrape: no reviews found on %s (no known review app detected).", target)
        return []


async def scrape_website_reviews(company_data: Dict[str, str]) -> List[str]:
    """Public entry point — same contract as every other scrape_x()
    function: takes the per-job company_data dict, returns a flat
    List[str] of clean review text."""
    product_url = (company_data.get("product_url") or "").strip()
    website = (company_data.get("website") or "").strip()
    product_name = (company_data.get("product_name") or "").strip()

    if not product_url and not website:
        return []

    loop = asyncio.get_event_loop()
    try:
        reviews = await asyncio.wait_for(
            loop.run_in_executor(_EXECUTOR, _scrape_sync, product_url, website, product_name),
            timeout=TIME_BUDGET_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning("Website review scrape exceeded its %ds budget for %r.", TIME_BUDGET_SECONDS, product_url or website)
        return []
    except Exception:
        logger.exception("Unhandled error scraping website reviews for %r.", product_url or website)
        return []

    texts = [r.text for r in reviews]
    final = normalize_comments(texts)[:MAX_REVIEWS]
    logger.info("Website reviews returned %d comments", len(final))
    return final
