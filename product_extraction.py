"""
product_extraction.py
----------------------
Helper module for the Product Discovery pipeline (see product_discovery.py).

This module contains everything that is NOT orchestration:
    * The `Product` data model + confidence scoring
    * Site-type detection (ecommerce / brand / corporate / saas / marketplace)
    * JSON-LD / microdata structured-data parsing (schema.org Product, Offer,
      ItemList, BreadcrumbList)
    * Static-HTML "product card" parsing
    * Category/collection link discovery
    * Strict noise / junk filtering (this is what removes "Access Denied",
      "Login", "Home", footer links, cookie banners, etc.)

Nothing here talks to the network directly except where explicitly noted -
network I/O (httpx / Playwright) stays in product_discovery.py so this module
can be unit tested with plain strings.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

import config

# --------------------------------------------------------------------------
# Site type detection
# --------------------------------------------------------------------------

class SiteType(str, Enum):
    ECOMMERCE = "ecommerce"
    BRAND = "brand"
    CORPORATE = "corporate"
    SAAS = "saas"
    MARKETPLACE = "marketplace"
    UNKNOWN = "unknown"


_ECOMMERCE_HINTS = (
    "add to cart", "add to bag", "add to basket", "buy now", "in stock",
    "out of stock", "shopify", "woocommerce", "magento", "bigcommerce",
    "shopping cart", "checkout", "free shipping", "product-grid",
    "product-card", "sku", "size guide",
)
_SAAS_HINTS = (
    "free trial", "start free trial", "pricing plans", "book a demo",
    "request a demo", "api documentation", "sign up free", "per month",
    "per user", "subscription plans", "changelog", "integrations",
)
_MARKETPLACE_HINTS = (
    "sold by", "seller rating", "marketplace", "third-party seller",
    "compare sellers", "ships from and sold by", "become a seller",
)
_CORPORATE_HINTS = (
    "investor relations", "annual report", "press release", "our mission",
    "board of directors", "corporate governance", "csr", "sustainability report",
)


def detect_site_type(homepage_html: str) -> SiteType:
    """Lightweight heuristic classification of the target website."""
    text = (homepage_html or "").lower()
    if not text:
        return SiteType.UNKNOWN

    scores = {
        SiteType.ECOMMERCE: sum(text.count(h) for h in _ECOMMERCE_HINTS),
        SiteType.SAAS: sum(text.count(h) for h in _SAAS_HINTS),
        SiteType.MARKETPLACE: sum(text.count(h) for h in _MARKETPLACE_HINTS),
        SiteType.CORPORATE: sum(text.count(h) for h in _CORPORATE_HINTS),
    }
    best_type, best_score = max(scores.items(), key=lambda kv: kv[1])
    if best_score == 0:
        # No strong signal either way - if the page has any product-schema
        # markup at all we lean "brand", otherwise "corporate".
        return SiteType.BRAND if "product" in text else SiteType.CORPORATE
    if best_type is SiteType.ECOMMERCE and scores[SiteType.MARKETPLACE] >= scores[SiteType.ECOMMERCE]:
        return SiteType.MARKETPLACE
    return best_type


# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------

@dataclass
class Product:
    name: str
    url: str = ""
    category: str = ""
    image: str = ""
    description: str = ""
    price: str = ""
    availability: str = ""
    brand: str = ""
    model: str = ""
    sku: str = ""
    variant: str = ""
    confidence: float = 0.0
    source: Set[str] = field(default_factory=set)
    occurrences: int = 1
    is_service: bool = False

    def key(self) -> str:
        """Stable de-duplication key: prefer canonical URL, else normalised name."""
        if self.url:
            parsed = urlparse(self.url)
            return f"url:{parsed.netloc}{parsed.path.rstrip('/').lower()}"
        return f"name:{_normalise_name(self.name)}"

    def merge(self, other: "Product") -> None:
        """Merge a duplicate observation of the same product into this one."""
        self.occurrences += 1
        self.source |= other.source
        self.name = self.name if len(self.name) >= len(other.name) else other.name
        self.url = self.url or other.url
        self.image = self.image or other.image
        self.description = self.description or other.description
        self.price = self.price or other.price
        self.availability = self.availability or other.availability
        self.brand = self.brand or other.brand
        self.model = self.model or other.model
        self.sku = self.sku or other.sku
        self.variant = self.variant or other.variant
        self.category = self.category or other.category

    def score(self) -> float:
        """Compute a 0..1 confidence score from the corroborating signals."""
        s = 0.0
        if _looks_like_product_url(self.url):
            s += 0.30
        if self.image:
            s += 0.15
        if self.price:
            s += 0.20
        if self.sku:
            s += 0.10
        if self.description:
            s += 0.10
        if "jsonld" in self.source:
            s += 0.20
        if self.occurrences > 1:
            s += min(0.15, 0.05 * (self.occurrences - 1))
        if not self.url and not self.image and not self.price and "jsonld" not in self.source:
            # Bare heading/text with zero corroboration - heavily penalise.
            s -= 0.35
        self.confidence = max(0.0, min(1.0, round(s, 3)))
        return self.confidence

    def as_dict(self) -> Dict:
        return {
            "name": self.name,
            "url": self.url,
            "category": self.category,
            "image": self.image,
            "description": self.description,
            "price": self.price,
            "availability": self.availability,
            "brand": self.brand,
            "model": self.model,
            "sku": self.sku,
            "variant": self.variant,
            "confidence": self.confidence,
            "source": sorted(self.source),
            "is_service": self.is_service,
        }


def _normalise_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()


# --------------------------------------------------------------------------
# Noise / junk filtering
# --------------------------------------------------------------------------
# This is the part directly responsible for fixing the "Access Denied",
# "Login", "Home", "About Us" style bugs observed in testing. A candidate
# must fail ALL of the checks below to be considered further, and - unlike
# the previous implementation - a bare nav/heading string with none of these
# problems still is NOT enough on its own; it also needs a corroborating
# signal (see Product.score / MIN_CONFIDENCE in product_discovery.py).

_HARD_NOISE_TERMS: Set[str] = {
    "home", "homepage", "cart", "checkout", "login", "log in", "sign in",
    "sign up", "register", "logout", "sign out", "my account", "account",
    "wishlist", "compare", "search", "menu", "navigation", "close", "back",
    "next", "previous", "read more", "learn more", "view all", "see all",
    "shop all", "explore", "subscribe", "newsletter", "language", "currency",
    "about", "about us", "contact", "contact us", "help", "faq", "faqs",
    "terms", "terms of service", "terms & conditions", "privacy",
    "privacy policy", "cookie policy", "cookies", "careers", "jobs", "press",
    "media", "investors", "sitemap", "blog", "news", "support", "helpdesk",
    "helpline", "skip to content", "skip to main content", "404",
    "page not found", "not found", "error", "access denied", "forbidden",
    "unauthorized", "unauthorised", "session expired", "server error",
    "something went wrong", "internal server error", "service unavailable",
    "index of", "click here", "learn how", "get started", "book now",
    "download now", "all rights reserved", "follow us", "share this",
    "add to cart", "add to bag", "buy now", "quick view", "quick shop",
    "filters", "filter", "sort", "sort by", "load more", "show more",
    "loading", "please wait", "we use cookies", "accept cookies",
    "manage preferences", "return to shop", "continue shopping",
}

_NOISE_SUBSTRINGS = (
    "access denied", "403 forbidden", "404 not found", "page not found",
    "we could not find", "we couldn't find", "does not exist",
    "has been removed", "under maintenance", "coming soon", "session has expired",
    "please enable javascript", "please try again", "captcha",
    "are you a robot", "verify you are human", "all rights reserved",
    "©", "copyright", "unsubscribe", "opt out", "opt-out",
)

_ERROR_STATUS_RE = re.compile(r"\b(40[0-9]|50[0-9])\b")
_ALL_DIGITS_RE = re.compile(r"^\d+([.,]\d+)?$")
_BADGE_ONLY_RE = re.compile(
    r"^(new|sale|sold out|best seller|bestseller|featured|trending|hot|"
    r"limited edition|exclusive|top rated|popular|most popular|"
    r"in stock|out of stock|low stock|coming soon|just in|back in stock)$"
)
_PRICE_RANGE_RE = re.compile(r"[₹$€£]\s*\d[\d,]*\s*(?:-|–|to)\s*[₹$€£]?\s*\d")
_PRICE_RE = re.compile(r"[₹$€£]\s?\d[\d,]*(?:\.\d{1,2})?")


def is_noise_text(text: str) -> bool:
    """Return True if `text` is boilerplate/navigation/error noise, not a product."""
    if not text:
        return True
    lower = re.sub(r"\s+", " ", text).strip().lower().strip(" \u2022-|·:")
    if not lower:
        return True
    if lower in _HARD_NOISE_TERMS:
        return True
    if any(sub in lower for sub in _NOISE_SUBSTRINGS):
        return True
    if _BADGE_ONLY_RE.match(lower):
        return True
    if _ALL_DIGITS_RE.match(lower):
        return True
    if _ERROR_STATUS_RE.search(lower) and len(lower.split()) <= 4:
        return True
    if any(word in lower for word in config.BLACKLIST_WORDS):
        return True
    # Long sentence-like strings (ending in punctuation, many words) are
    # almost never a product name - they are usually page copy or an
    # error/status message that leaked through.
    words = lower.split()
    if len(words) > 7:
        return True
    if lower.endswith((".", "!", "?")) and len(words) > 4:
        return True
    return False


def is_valid_candidate_name(text: str) -> bool:
    text = (text or "").strip()
    if not (config.MIN_CANDIDATE_LEN <= len(text) <= config.MAX_CANDIDATE_LEN):
        return False
    if is_noise_text(text):
        return False
    # Require at least one alphabetic character (rejects pure symbols/prices).
    if not re.search(r"[A-Za-z]", text):
        return False
    return True


def extract_price(text: str) -> str:
    if not text:
        return ""
    match = _PRICE_RE.search(text)
    return match.group(0).strip() if match else ""


# --------------------------------------------------------------------------
# JSON-LD / structured data parsing
# --------------------------------------------------------------------------

_PRODUCT_TYPES = {"product"}
_ITEMLIST_TYPES = {"itemlist", "offercatalog"}


def parse_jsonld_products(html: str, page_url: str) -> List[Product]:
    """Extract schema.org Product entries from <script type=application/ld+json>."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    products: List[Product] = []

    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue

        for node in _flatten_jsonld(data):
            product = _jsonld_node_to_product(node, page_url)
            if product is not None:
                products.append(product)

    return products


def _flatten_jsonld(data) -> Iterable[Dict]:
    """Yield every dict node found in a JSON-LD payload, following @graph/arrays."""
    if isinstance(data, list):
        for item in data:
            yield from _flatten_jsonld(item)
    elif isinstance(data, dict):
        yield data
        if "@graph" in data and isinstance(data["@graph"], list):
            for item in data["@graph"]:
                yield from _flatten_jsonld(item)
        for key in ("itemListElement", "mainEntity", "hasVariant"):
            if key in data:
                yield from _flatten_jsonld(data[key])


def _schema_type(node: Dict) -> str:
    t = node.get("@type", "")
    if isinstance(t, list):
        t = t[0] if t else ""
    return str(t).lower()


def _jsonld_node_to_product(node: Dict, page_url: str) -> Optional[Product]:
    if not isinstance(node, dict):
        return None

    node_type = _schema_type(node)

    # ItemList entries often wrap the real product under "item".
    if node_type in _ITEMLIST_TYPES or "item" in node and node_type not in _PRODUCT_TYPES:
        inner = node.get("item")
        if isinstance(inner, dict):
            return _jsonld_node_to_product(inner, page_url)
        return None

    if node_type not in _PRODUCT_TYPES:
        return None

    name = str(node.get("name") or "").strip()
    if not name or not is_valid_candidate_name(name):
        return None

    url = node.get("url") or ""
    if url:
        url = urljoin(page_url, url)

    image = node.get("image")
    if isinstance(image, list):
        image = image[0] if image else ""
    if isinstance(image, dict):
        image = image.get("url", "")
    image = str(image or "")

    brand = node.get("brand")
    if isinstance(brand, dict):
        brand = brand.get("name", "")
    brand = str(brand or "")

    offers = node.get("offers")
    price, availability = "", ""
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        price_val = offers.get("price") or offers.get("lowPrice")
        currency = offers.get("priceCurrency", "")
        if price_val:
            price = f"{currency} {price_val}".strip()
        availability = str(offers.get("availability") or "").rsplit("/", 1)[-1]

    description = str(node.get("description") or "").strip()
    if len(description) > 200:
        description = description[:197].rstrip() + "..."

    sku = str(node.get("sku") or node.get("mpn") or "").strip()
    model = str(node.get("model") or "").strip()

    product = Product(
        name=name,
        url=url,
        image=image,
        description=description,
        price=price,
        availability=availability,
        brand=brand,
        model=model,
        sku=sku,
        source={"jsonld"},
    )
    return product


def parse_breadcrumb_category(html: str) -> str:
    """Best-effort category name from a schema.org BreadcrumbList, if present."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (tag.string or tag.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        for node in _flatten_jsonld(data):
            if _schema_type(node) == "breadcrumblist":
                items = node.get("itemListElement") or []
                names = []
                for item in items:
                    if isinstance(item, dict):
                        nm = item.get("name") or (item.get("item") or {}).get("name")
                        if nm and nm.lower() not in ("home", "homepage"):
                            names.append(str(nm).strip())
                if names:
                    return names[-1]
    return ""


# --------------------------------------------------------------------------
# Static HTML "product card" parsing
# --------------------------------------------------------------------------

_PRODUCT_URL_PATTERNS = re.compile(
    r"/(products?|shop|store|item|items|dp|catalog|collections/[^/]+/products?)/[^/?#]+",
    re.IGNORECASE,
)
_EXCLUDED_URL_PATTERNS = re.compile(
    r"/(cart|checkout|login|signin|signup|register|account|wishlist|search|"
    r"pages?|policies?|blogs?|legal|terms|privacy|careers|press|about|contact|"
    r"sitemap|help|faq)(/|$|\?)",
    re.IGNORECASE,
)

_CARD_CLASS_RE = re.compile(
    r"product[-_]?(card|item|tile|grid-item|list-item)|grid[-_]?item|"
    r"collection[-_]?item",
    re.IGNORECASE,
)


def _looks_like_product_url(url: str) -> bool:
    if not url:
        return False
    path = urlparse(url).path
    if _EXCLUDED_URL_PATTERNS.search(path):
        return False
    return bool(_PRODUCT_URL_PATTERNS.search(path))


def parse_html_product_cards(html: str, page_url: str, category: str = "") -> List[Product]:
    """Extract product-like cards from static HTML using structural heuristics.

    A "card" is only accepted if it has BOTH a plausible product link AND at
    least one of (title text, image, price) - this dual requirement is what
    prevents generic nav/footer links from being picked up.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Strip obvious chrome before scanning so nav/footer/breadcrumb text can
    # never reach the candidate list in the first place.
    for tag_name in ("nav", "header", "footer", "aside"):
        for el in soup.find_all(tag_name):
            el.decompose()
    for el in soup.select(
        "[class*=breadcrumb i], [class*=footer i], [class*=menu i], "
        "[class*=cookie i], [class*=modal i], [class*=popup i], "
        "[class*=newsletter i], [role=navigation]"
    ):
        el.decompose()

    products: List[Product] = []

    candidate_containers = soup.select(
        "[class*=product-card i], [class*=product-item i], [class*=product-tile i], "
        "[class*=grid-item i], [class*=collection-item i], li.product, "
        "[data-product-id], [data-product-handle], [itemtype*='schema.org/Product' i]"
    )

    anchors = soup.find_all("a", href=True)
    # Fallback: any anchor whose href already looks like a product URL, even
    # if the site doesn't use one of the common card class names.
    anchor_only = [a for a in anchors if _looks_like_product_url(urljoin(page_url, a["href"]))]

    seen_nodes = set()

    def _handle(container, anchor_hint=None) -> None:
        if id(container) in seen_nodes:
            return
        seen_nodes.add(id(container))

        link = anchor_hint or container if container.name == "a" else container.find("a", href=True)
        href = ""
        if link is not None and link.has_attr("href"):
            href = urljoin(page_url, link["href"])

        name = _extract_card_title(container)
        if not name and link is not None:
            name = (link.get("title") or link.get_text(" ", strip=True) or "").strip()
        if not name:
            img = container.find("img")
            if img is not None:
                name = (img.get("alt") or "").strip()

        if not is_valid_candidate_name(name):
            return
        if not href and not container.find("img"):
            # No link and no image at all - too weak a signal, discard.
            return
        if href and not _looks_like_product_url(href):
            # Has a link but it clearly points somewhere non-product (e.g. a
            # "Shop Men" banner linking to a category, not a product).
            href = ""

        img_tag = container.find("img")
        image = ""
        if img_tag is not None:
            image = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-srcset", "").split(" ")[0]
            if image:
                image = urljoin(page_url, image)

        price_text = ""
        price_el = container.find(class_=re.compile(r"price", re.IGNORECASE))
        if price_el is not None:
            price_text = extract_price(price_el.get_text(" ", strip=True))
        if not price_text:
            price_text = extract_price(container.get_text(" ", strip=True))

        products.append(
            Product(
                name=name,
                url=href,
                image=image,
                price=price_text,
                category=category,
                source={"html-card"},
            )
        )

    for container in candidate_containers:
        _handle(container)

    for a in anchor_only:
        _handle(a, anchor_hint=a)

    return products


def _extract_card_title(container) -> str:
    for sel in (
        "[class*=title i]", "[class*=name i]", "h1", "h2", "h3", "h4",
        "[itemprop=name]",
    ):
        el = container.select_one(sel) if hasattr(container, "select_one") else None
        if el is not None:
            text = el.get_text(" ", strip=True)
            if text:
                return text
    return ""


# --------------------------------------------------------------------------
# Category / collection link discovery
# --------------------------------------------------------------------------

_CATEGORY_HINT_WORDS = (
    "shop", "products", "product", "collection", "collections", "category",
    "categories", "shoes", "footwear", "apparel", "clothing", "accessories",
    "men", "women", "kids", "boys", "girls", "unisex", "electronics", "audio",
    "headphones", "earphones", "laptop", "laptops", "phone", "phones",
    "mobiles", "watches", "wearables", "gear", "equipment", "gaming",
    "home", "kitchen", "beauty", "outdoor", "running", "training", "sport",
    "sports", "lifestyle", "new arrivals", "best sellers", "sale",
    "services", "solutions", "plans", "pricing", "store", "catalog",
)

_CATEGORY_EXCLUDE_WORDS = (
    "login", "account", "cart", "checkout", "wishlist", "search", "help",
    "faq", "about", "contact", "privacy", "terms", "careers", "blog",
    "press", "investor", "sitemap", "cookie",
)


def discover_category_links(html: str, page_url: str, limit: int = 15) -> List[str]:
    """Find nav/menu links that plausibly lead to a product category page."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    root = _root_url(page_url)

    links: List[str] = []
    seen: Set[str] = set()

    scopes = soup.select("nav, header, [role=navigation], [class*=menu i], [class*=nav i]") or [soup]

    for scope in scopes:
        for a in scope.find_all("a", href=True):
            text = (a.get_text(" ", strip=True) or "").lower()
            href = a["href"]
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full = urljoin(page_url, href)
            if not full.startswith(root):
                continue
            if any(w in text for w in _CATEGORY_EXCLUDE_WORDS):
                continue
            if any(w in text for w in _CATEGORY_HINT_WORDS) or any(
                w in full.lower() for w in _CATEGORY_HINT_WORDS
            ):
                if full not in seen:
                    seen.add(full)
                    links.append(full)
            if len(links) >= limit:
                break
        if len(links) >= limit:
            break

    return links


def _root_url(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


# --------------------------------------------------------------------------
# Product vs. Service classification
# --------------------------------------------------------------------------

_SERVICE_KEYWORDS = (
    "membership", "support", "delivery", "returns", "return policy",
    "subscription", "warranty", "installation", "consultation", "service",
    "services", "customer support", "customer care", "live chat",
    "financing", "insurance", "repair", "maintenance", "rental", "loyalty",
    "rewards", "gift card", "gift cards", "shipping", "exchange", "app",
    "helpdesk", "helpline", "booking", "reservation", "consulting",
    "training", "onboarding", "plan", "plans", "trial",
)


def classify_as_service(product: Product) -> bool:
    lower = f"{product.name} {product.category}".lower()
    return any(keyword in lower for keyword in _SERVICE_KEYWORDS)


# --------------------------------------------------------------------------
# Sitemap discovery (spec Step 5: robots.txt -> Sitemap: -> sitemap.xml)
# --------------------------------------------------------------------------
# Pure text/regex parsing on purpose - no XML parser dependency, and it is
# tolerant of the malformed/oversized sitemap files real-world sites ship.

_SITEMAP_LOC_RE = re.compile(r"<loc>\s*([^<\s][^<]*?)\s*</loc>", re.IGNORECASE)


def parse_robots_sitemaps(robots_txt: str) -> List[str]:
    """Extract every 'Sitemap: <url>' line from a robots.txt body."""
    if not robots_txt:
        return []
    sitemaps: List[str] = []
    for line in robots_txt.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            if url and url not in sitemaps:
                sitemaps.append(url)
    return sitemaps


def extract_sitemap_locs(xml_text: str, limit: int = 500) -> List[str]:
    """Pull <loc> URLs out of a sitemap or sitemap-index XML body."""
    if not xml_text:
        return []
    return _SITEMAP_LOC_RE.findall(xml_text)[:limit]


def is_sitemap_index(xml_text: str) -> bool:
    """True if this sitemap is itself an index of other sitemaps."""
    return bool(xml_text) and "<sitemapindex" in xml_text.lower()


def filter_category_urls_from_sitemap(urls: List[str], root: str, limit: int = 15) -> List[str]:
    """Keep only sitemap URLs that plausibly point at a category/collection
    page - not an individual product, blog post, or legal/account page.
    Individual products are left for the product-card / JSON-LD stages;
    this function exists purely to seed Step 6's category list."""
    picked: List[str] = []
    for url in urls:
        if not url.startswith(root):
            continue
        path = urlparse(url).path.lower()
        if any(w in path for w in _CATEGORY_EXCLUDE_WORDS):
            continue
        if _looks_like_product_url(url):
            continue
        if any(w in path for w in _CATEGORY_HINT_WORDS):
            if url not in picked:
                picked.append(url)
        if len(picked) >= limit:
            break
    return picked
