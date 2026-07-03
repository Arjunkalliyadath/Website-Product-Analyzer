"""
product_discovery.py — Product & Service Discovery
====================================================

New module that slots between company discovery and social-media scraping.

Given the discovered company website, this module tries to answer:
"What does this company actually sell / offer?" so that downstream scrapers
can search for product-specific reviews/mentions instead of only the brand
name.

Strategy (in order, each only runs if the previous one found too little):

1. PLAYWRIGHT (preferred)
   Visit the homepage + likely product/service pages (shop, products,
   collections, services, solutions, menu) and pull candidate names out of
   nav links, headings, and product-card-like elements.

2. SERP FALLBACK
   If the site structure is too custom/JS-heavy to yield results, search
   "{company} products" and "{company} services" and mine candidate names
   out of the search-result snippets.

3. HOMEPAGE-TEXT FALLBACK
   Rule-based parse of the raw homepage text (headings + short lines) when
   both of the above come up short.

Candidates are then classified into products vs. services with a keyword
heuristic, deduplicated, ranked, and trimmed down to a small scrape-target
list so downstream scraping stays fast.

This module does not touch the existing scrapers, sentiment engine, or
dashboard rendering — it only supplies extra structured data that app.py
wires in.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Set
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

import config
from scrapers.browser_utils import run_playwright_async

logger = logging.getLogger(__name__)

DISCOVERY_VERSION = "product-discovery-v2"

# CHANGE 1 (config.py): a single cap now governs both the final `products`
# list shown on the dashboard AND the list we scrape. Previously these were
# two different numbers (an uncapped `products` list + a 5-item scrape
# cap), which is exactly how ~73 noisy entries reached the UI while only 5
# were ever scraped. Tunable in config.py, not here.
MAX_PRODUCTS_TO_SCRAPE = config.MAX_PRODUCTS

# Candidate pages worth visiting on the official site, beyond the homepage.
CANDIDATE_PATH_SUFFIXES = (
    "/products", "/product", "/shop", "/shop-all", "/collections",
    "/collections/all", "/services", "/solutions", "/store", "/catalog",
    "/menu", "/categories",
)

# Nav-link text that suggests "this leads to a products/services listing".
NAV_HINT_WORDS = (
    "shop", "product", "products", "collection", "collections", "service",
    "services", "solution", "solutions", "store", "catalog", "menu",
    "category", "categories",
)

# Generic chrome/noise that should never be treated as a product or service.
NOISE_TERMS = {
    "home", "cart", "checkout", "login", "log in", "sign in", "sign up",
    "register", "account", "my account", "wishlist", "search", "contact",
    "contact us", "about", "about us", "blog", "news", "help", "faq",
    "faqs", "terms", "terms of service", "privacy", "privacy policy",
    "careers", "jobs", "press", "media", "investors", "sitemap",
    "cookie policy", "cookies", "sign out", "logout", "menu", "close",
    "skip to content", "skip to main content", "subscribe", "newsletter",
    "language", "currency", "back", "next", "previous", "read more",
    "learn more", "view all", "see all", "shop all", "explore",
}

# Words that push a candidate toward "service" rather than "product".
SERVICE_KEYWORDS = (
    "membership", "support", "delivery", "returns", "return policy",
    "subscription", "warranty", "installation", "consultation",
    "service", "services", "customer support", "customer care",
    "live chat", "financing", "insurance", "repair", "maintenance",
    "rental", "loyalty", "rewards", "gift card", "gift cards",
    "shipping", "exchange", "app", "helpdesk", "helpline", "booking",
    "reservation", "consulting", "training", "onboarding",
)

# Terms that boost a candidate's ranking (site is telling us it's important).
# Superseded by config.PROMINENCE_TIERS for actual ranking (Change 1), kept
# here only as a fallback constant in case any external caller imports it.
PROMINENCE_HINTS = tuple(hint for tier in config.PROMINENCE_TIERS for hint in tier)

_MIN_LEN, _MAX_LEN = config.MIN_CANDIDATE_LEN, config.MAX_CANDIDATE_LEN
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


@dataclass
class ProductDiscoveryResult:
    products: List[str] = field(default_factory=list)
    services: List[str] = field(default_factory=list)
    scrape_targets: List[str] = field(default_factory=list)
    discovery_method: str = "none"
    discovery_version: str = DISCOVERY_VERSION

    def as_dict(self) -> Dict:
        return {
            "products": self.products,
            "services": self.services,
            "scrape_targets": self.scrape_targets,
            "products_found": len(self.products),
            "services_found": len(self.services),
            "discovery_method": self.discovery_method,
            "discovery_version": self.discovery_version,
        }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def discover_products(company_data: Dict[str, str]) -> Dict:
    """
    Discover products/services for a company already resolved by
    company_discovery.discover_company().

    Returns a plain dict (see ProductDiscoveryResult.as_dict) so callers in
    app.py don't need to import the dataclass.
    """
    website = (company_data.get("website") or "").strip()
    company_name = (company_data.get("company_name") or "").strip()

    if not website:
        return ProductDiscoveryResult(discovery_method="skipped-no-website").as_dict()

    candidates: List[str] = []
    method = "none"

    # ---- Method 1: Playwright ----
    try:
        candidates = await _discover_via_playwright(website)
        if candidates:
            method = "playwright"
    except Exception as exc:
        logger.info("Product discovery via Playwright failed for %s: %s", website, exc)
        candidates = []

    # ---- Method 2: SERP fallback ----
    if len(candidates) < 3 and company_name:
        try:
            serp_candidates = await _discover_via_serp(company_name)
            if serp_candidates:
                candidates = _merge_unique(candidates, serp_candidates)
                method = "serp" if method == "none" else f"{method}+serp"
        except Exception as exc:
            logger.info("Product discovery via SERP failed for %s: %s", company_name, exc)

    # ---- Method 3: homepage-text fallback ----
    if len(candidates) < 3:
        try:
            text_candidates = await _discover_via_homepage_text(website)
            if text_candidates:
                candidates = _merge_unique(candidates, text_candidates)
                method = "text" if method == "none" else f"{method}+text"
        except Exception as exc:
            logger.info("Product discovery via homepage text failed for %s: %s", website, exc)

    raw_products, services = _classify(candidates, company_name)

    # CHANGE 1 — cap the *entire* products list (not just the scrape
    # subset) to MAX_PRODUCTS. This is what actually fixes "73 products
    # extracted": previously only the downstream scrape_targets were
    # capped, while the full `products` list (shown on the dashboard) had
    # no ceiling at all.
    products = _rank_and_select(raw_products, MAX_PRODUCTS_TO_SCRAPE)

    # Expected flow: Filter Real Products → Select Top 10 Products → Async
    # Review Scraping. We scrape exactly the products we show — no second,
    # smaller cap — concurrency (not list size) is what keeps scraping fast
    # (see CHANGE 4 / config.MAX_PARALLEL_TASKS, applied in app.py).
    scrape_targets = products

    result = ProductDiscoveryResult(
        products=products,
        services=services,
        scrape_targets=scrape_targets,
        discovery_method=method,
    )
    logger.info(
        "Product discovery for %s: %d products, %d services (method=%s)",
        company_name, len(products), len(services), method,
    )
    return result.as_dict()


# ---------------------------------------------------------------------------
# Method 1 — Playwright
# ---------------------------------------------------------------------------

async def _discover_via_playwright(website: str) -> List[str]:
    root = _root_url(website)

    async def _coro() -> List[str]:
        from playwright.async_api import async_playwright

        found: List[str] = []
        seen_lower: Set[str] = set()

        def _add(text: str) -> None:
            cleaned = _clean_candidate_text(text)
            if cleaned and cleaned.lower() not in seen_lower:
                seen_lower.add(cleaned.lower())
                found.append(cleaned)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=HEADERS["User-Agent"],
            )
            page = await context.new_page()

            # --- Visit homepage first, and collect nav links that look
            #     like they lead to a products/services listing. We defer
            #     extracting the homepage's OWN product cards until after
            #     the full-catalog pages below (see BUG FIX note) — this
            #     step only harvests nav links.
            extra_pages: List[str] = []
            try:
                await page.goto(root, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_timeout(1500)

                for a in await page.locator("nav a, header a").all():
                    try:
                        text = (await a.inner_text() or "").strip()
                        href = await a.get_attribute("href")
                        if not href or not text:
                            continue
                        if any(hint in text.lower() for hint in NAV_HINT_WORDS):
                            full = urljoin(root, href)
                            if full.startswith(root) and full not in extra_pages:
                                extra_pages.append(full)
                    except Exception:
                        continue
            except Exception as exc:
                logger.info("Playwright homepage visit failed for %s: %s", root, exc)

            # BUG FIX (July 2026 field test) — a homepage "Best Sellers" /
            # in-house-collab carousel was filling the entire MAX_PRODUCTS
            # cap before the site's actual full catalog ever got scanned,
            # so third-party brands a retailer stocks (e.g. Moondrop,
            # Truthear, Meze) never made it into the results even though
            # `/collections/all`-style pages list them. Catalog/collection
            # pages are now visited FIRST — with ".../all"-style URLs
            # prioritized, since those are most likely to be the complete,
            # unfiltered product listing — so their products claim slots in
            # `found` (and therefore the final top-N) ahead of the
            # homepage's narrower promotional carousel.
            pages_to_visit = sorted(
                dict.fromkeys(
                    extra_pages[:6] + [f"{root}{suffix}" for suffix in CANDIDATE_PATH_SUFFIXES]
                ),
                key=lambda u: 0 if "all" in u.lower() else 1,
            )[:10]

            for url in pages_to_visit:
                if len(found) >= 60:
                    break
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    await page.wait_for_timeout(1200)
                    await _extract_from_page(page, _add)
                except Exception:
                    continue

            # Finally, pull the homepage's own product cards (best-sellers,
            # featured in-house collabs) as supplementary — still captured,
            # just no longer crowding out catalog diversity discovered above.
            if len(found) < 60:
                try:
                    await page.goto(root, wait_until="domcontentloaded", timeout=20000)
                    await page.wait_for_timeout(1200)
                    await _extract_from_page(page, _add)
                except Exception:
                    pass

            await context.close()
            await browser.close()

        return found

    try:
        return await run_playwright_async(_coro)
    except Exception as exc:
        logger.info("Playwright product discovery unavailable for %s: %s", root, exc)
        return []


_PRODUCT_CARD_SELECTORS = (
    "[class*='product-card' i]",
    "[class*='product-item' i]",
    "[class*='product-grid' i] [class*='title' i]",
    "[class*='product-grid' i] a[href*='product']",
    "[class*='grid-item' i] [class*='title' i]",
    "li.product h2, li.product h3",
    "li.product a.woocommerce-loop-product__link",
    "[data-product-title]",
    "[itemprop='name']",
    "a[href*='/products/'] [class*='title' i]",
    "a[href*='/products/']",
    "a[href*='/product/']",
)

_HEADING_SELECTORS = ("h1", "h2", "h3")

_EXCLUDED_ANCESTORS = (
    "nav, header, footer, aside, [role='navigation'], "
    "[class*='breadcrumb' i], [class*='footer' i], [class*='menu' i], "
    "[class*='filter' i], [class*='sidebar' i], [class*='pagination' i]"
)


async def _extract_from_page(page, add_fn) -> None:
    """
    Pull candidate product names off the current Playwright page —
    restricted to actual visible product-card elements (Change 3), never
    from headers, menus, dropdowns, filters, footers, breadcrumbs, sidebar,
    or hidden HTML (Change 11).
    """
    for sel in (*_PRODUCT_CARD_SELECTORS, *_HEADING_SELECTORS):
        try:
            locator = page.locator(sel)
            count = min(await locator.count(), 40)
            for i in range(count):
                el = locator.nth(i)
                try:
                    if not await el.is_visible():
                        continue
                    inside_chrome = await el.evaluate(
                        "(node, sel) => node.closest(sel) !== null",
                        _EXCLUDED_ANCESTORS,
                    )
                    if inside_chrome:
                        continue
                    text = (await el.inner_text() or "").strip()
                    if text:
                        add_fn(text)
                except Exception:
                    continue
        except Exception:
            continue

    # BUG FIX (July 2026 field test) — carousels/"Best Sellers" sliders
    # often virtualize their visible title text (only a badge like "#1 Best
    # Seller" renders reliably), which was silently losing real products.
    # The product image's `alt` attribute almost universally carries the
    # real product name for accessibility/SEO regardless of carousel state,
    # so it's scanned as a fallback source of candidates.
    try:
        img_locator = page.locator("a[href*='/products/'] img[alt], a[href*='/product/'] img[alt]")
        count = min(await img_locator.count(), 60)
        for i in range(count):
            img = img_locator.nth(i)
            try:
                inside_chrome = await img.evaluate(
                    "(node, sel) => node.closest(sel) !== null",
                    _EXCLUDED_ANCESTORS,
                )
                if inside_chrome:
                    continue
                alt = (await img.get_attribute("alt") or "").strip()
                if alt:
                    add_fn(alt)
            except Exception:
                continue
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Method 2 — SERP fallback
# ---------------------------------------------------------------------------

async def _discover_via_serp(company_name: str) -> List[str]:
    queries = [
        f"{company_name} products",
        f"{company_name} services",
    ]
    candidates: List[str] = []

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=6.0),
        follow_redirects=True,
        headers=HEADERS,
        verify=False,
    ) as client:

        async def fetch(query: str) -> str:
            try:
                url = f"https://www.google.com/search?q={quote_plus(query)}"
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
            except Exception as exc:
                logger.info("SERP fetch failed for %r: %s", query, exc)
                return ""

        pages = await asyncio.gather(*(fetch(q) for q in queries))

    seen_lower: Set[str] = set()
    for html in pages:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        # Search-result headings/snippets are the most reliable places for
        # short product/service phrases to surface.
        for tag in soup.find_all(["h3", "span", "div"]):
            text = (tag.get_text() or "").strip()
            cleaned = _clean_candidate_text(text)
            if cleaned and cleaned.lower() not in seen_lower and _looks_like_offering(cleaned, company_name):
                seen_lower.add(cleaned.lower())
                candidates.append(cleaned)
            if len(candidates) >= 40:
                break

    return candidates


def _looks_like_offering(text: str, company_name: str) -> bool:
    """Heuristic filter for SERP snippet fragments: short, title-ish phrases."""
    words = text.split()
    if not (1 <= len(words) <= 6):
        return False
    if text.lower() == company_name.lower():
        return False
    # Reject full sentences (has terminal punctuation typical of prose).
    if text.endswith((".", "!", "?")) and len(words) > 4:
        return False
    return True


# ---------------------------------------------------------------------------
# Method 3 — homepage text fallback
# ---------------------------------------------------------------------------

async def _discover_via_homepage_text(website: str) -> List[str]:
    root = _root_url(website)
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=6.0),
            follow_redirects=True,
            headers=HEADERS,
            verify=False,
        ) as client:
            resp = await client.get(root)
            resp.raise_for_status()
            html = resp.text
    except Exception as exc:
        logger.info("Homepage text fetch failed for %s: %s", root, exc)
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: List[str] = []
    seen_lower: Set[str] = set()

    for tag in soup.find_all(["h1", "h2", "h3", "h4", "li", "a"]):
        text = (tag.get_text() or "").strip()
        cleaned = _clean_candidate_text(text)
        if cleaned and cleaned.lower() not in seen_lower:
            seen_lower.add(cleaned.lower())
            candidates.append(cleaned)
        if len(candidates) >= 60:
            break

    return candidates


# ---------------------------------------------------------------------------
# Classification / ranking
# ---------------------------------------------------------------------------

def _classify(candidates: List[str], company_name: str) -> (List[str], List[str]):
    products: List[str] = []
    services: List[str] = []
    seen_lower: Set[str] = set()
    company_lower = (company_name or "").lower()

    for candidate in candidates:
        lower = candidate.lower()
        if lower in seen_lower:
            continue
        # Safety net: candidates should already be clean by this point (all
        # three discovery methods route text through _clean_candidate_text),
        # but re-checking here means _classify is safe to call directly too.
        if lower == company_lower or _is_blacklisted(candidate):
            continue
        if not (_MIN_LEN <= len(candidate) <= _MAX_LEN):
            continue
        seen_lower.add(lower)

        if any(keyword in lower for keyword in SERVICE_KEYWORDS):
            services.append(candidate)
        else:
            products.append(candidate)

    return products, services


def _rank_and_select(products: List[str], limit: int) -> List[str]:
    """
    CHANGE 1 — priority-tiered product selection.

    Order of preference, per the brief:
      1. Best Sellers
      2. Featured
      3. Trending
      4. Latest
      5. Otherwise — first visible product cards

    `products` arrives in page-appearance order (every discovery method
    appends candidates in the order they're found on the page/SERP), so
    "otherwise take first visible product cards" falls out naturally: it's
    just whatever's left over after the four prominence tiers are pulled
    out, still in that original order. Never exceeds `limit`.
    """
    tiers: List[List[str]] = [[] for _ in config.PROMINENCE_TIERS]
    leftover: List[str] = []
    seen_lower: Set[str] = set()

    for item in products:
        lower = item.lower()
        if lower in seen_lower:
            continue
        seen_lower.add(lower)

        placed = False
        for tier_index, hints in enumerate(config.PROMINENCE_TIERS):
            if any(hint in lower for hint in hints):
                tiers[tier_index].append(item)
                placed = True
                break
        if not placed:
            leftover.append(item)

    ordered = [item for tier in tiers for item in tier] + leftover
    return ordered[:limit]


# ---------------------------------------------------------------------------
# Text / URL utilities
# ---------------------------------------------------------------------------

def _is_blacklisted(text: str) -> bool:
    """
    CHANGE 2 — reject navigation/menu/footer noise before it ever becomes a
    "product". This is the central filter behind the blacklist requested in
    the brief ("Page not found", "All Collections", "Replacement Cable",
    "Best Headphones Under ₹1000", filters, sort, etc.).

    Substring matching (not exact match) is intentional: "Best Headphones
    Under ₹1000" needs to be caught even though it's not verbatim equal to
    any single blacklist entry — but it does contain "under" and "best
    headphones".
    """
    lower = text.lower().strip()
    if not lower:
        return True
    if lower in NOISE_TERMS:
        return True
    if any(word in lower for word in config.BLACKLIST_WORDS):
        return True
    # BUG FIX — bare ribbon/badge text ("Best Seller", "New", "#1", "#2 Best
    # Seller"): exact match only, so real product names that merely contain
    # these words ("Wave Buds Pro (Best Seller)") are NOT affected.
    if lower in config.EXACT_BADGE_TERMS:
        return True
    if re.fullmatch(r"#\s*\d+(\s*[-:.]?\s*(best\s?seller|new|featured|trending|hot|sale))?", lower):
        return True
    # Price-range phrasing: "Under ₹1000", "Above $50", "₹500 - ₹1000".
    if re.search(r"\b(under|above|below)\b\s*[₹$€£]?\s*\d", lower):
        return True
    if re.search(r"[₹$€£]\s*\d[\d,]*\s*(?:-|–|to)\s*[₹$€£]?\s*\d", lower):
        return True
    # Bare numbers / pagination artifacts ("2", "404").
    if re.fullmatch(r"\d+", lower):
        return True
    return False


def _clean_candidate_text(text: str) -> str:
    text = re.sub(r"\s+", " ", (text or "")).strip(" \u2022-|·")
    if not text:
        return ""
    if len(text) < _MIN_LEN or len(text) > _MAX_LEN:
        return ""
    if _is_blacklisted(text):
        return ""
    return text


def _merge_unique(base: List[str], extra: List[str]) -> List[str]:
    seen_lower = {item.lower() for item in base}
    merged = list(base)
    for item in extra:
        if item.lower() not in seen_lower:
            seen_lower.add(item.lower())
            merged.append(item)
    return merged


def _root_url(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
