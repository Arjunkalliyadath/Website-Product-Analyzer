"""
product_discovery.py
---------------------
Product Discovery module (Phase 2 rewrite).

Public entry point (unchanged signature, unchanged output keys, so app.py /
dashboard.html / the rest of the pipeline do not need to change):

    await discover_products(company_data: Dict[str, str]) -> Dict

Returned dict keeps the original keys other modules rely on:
    products, services, scrape_targets, products_found, services_found,
    discovery_method, discovery_version

...and adds new, additive keys for future consumers (report generation,
richer dashboards, etc.) without breaking existing ones:
    site_type, categories, catalogue

`catalogue` is the full structured product list (List[dict]) described in
the spec - name, url, category, image, description, price, availability,
brand, model, sku, variant, confidence, source. `products` / `services`
remain plain name strings (top-N by confidence) because that is what
templates/dashboard.html and the sentiment/scraping pipeline key off of.

Pipeline stages (see also product_extraction.py):
    1. JSON-LD / schema.org structured data (Product, Offer, ItemList,
       BreadcrumbList) - highest-confidence source, no guessing involved.
    2. Static HTML "product card" parsing (product-card/grid containers,
       anchors whose href already looks like a product URL).
    3. Category/collection page crawl - repeat stages 1+2 on discovered
       category pages, in parallel, bounded by MAX_CATEGORY_PAGES. Nav/
       header link discovery (product_extraction.discover_category_links)
       is supplemented in this module by _discover_extra_category_links(),
       which additionally scopes into mega-menu/dropdown/flyout panels and
       footer "shop by category" widgets that a plain nav/header/[role
       =navigation] scan can miss. Each category page also walks its own
       ?page=N pagination over plain HTTP (see
       _crawl_category_static_with_pagination), stopping automatically the
       moment a page returns nothing new.
    3b. robots.txt -> Sitemap: -> sitemap.xml category discovery, used to
       widen the category list when the nav crawl (stage 3) still comes
       back thin - e.g. mega-menus that are entirely JS-rendered, or a
       site whose real category tree isn't reachable from top-nav at all.
    4. Playwright fallback for JS-rendered / bot-protected sites: scroll,
       click "Load more", walk ?page=N pagination, and - when a site
       paginates purely via a stateful "Next" control rather than a
       predictable query param - fall back to clicking that control, then
       re-run the same card extraction against the rendered DOM.
    5. De-duplication + confidence scoring + noise filtering + ranking.

Playwright session architecture (stage 4):
    _fetch_static() does one plain HTTP request for the homepage - no
    browser involved. If that comes back blocked (401/403/429/500/502/503/
    504, or a 200
    that is actually an anti-bot interstitial), the ENTIRE analysis for
    that site is handed to _run_playwright_driven_discovery(), which owns
    one browser + one context + a small pool of reused pages for the whole
    analysis, and discovers navigation/category/product links straight from
    the *rendered* DOM instead of guessing paths like /products, /shop,
    /catalog, /store, /services. If the homepage loads fine over plain
    HTTP, the static pipeline (stages 1-3) runs as before and Playwright is
    only invoked - once, via the same single session - if results still
    look thin afterwards. Either way there is exactly one browser launch
    per analysis, closed exactly once when discovery finishes. See the
    _PlaywrightSession / _run_playwright_driven_discovery docstrings below
    for why this replaced the previous per-URL browser relaunching.

    A single `visited_urls` set is threaded through the static pipeline
    (stages 3 and 3b) so a URL discovered by both the nav crawl and the
    sitemap crawl in the same analysis is only ever fetched once.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

import config
from scrapers.browser_utils import run_playwright_async
from product_extraction import (
    Product,
    SiteType,
    classify_as_service,
    detect_site_type,
    discover_category_links,
    extract_sitemap_locs,
    filter_category_urls_from_sitemap,
    is_sitemap_index,
    is_valid_candidate_name,
    parse_breadcrumb_category,
    parse_html_product_cards,
    parse_jsonld_products,
    parse_robots_sitemaps,
    _looks_like_product_url,  # noqa: reused as-is for diagnostics only, not modified
    _CATEGORY_HINT_WORDS,  # noqa: reused as-is, same hint list as discover_category_links()
    _CATEGORY_EXCLUDE_WORDS,  # noqa: reused as-is, same exclude list as discover_category_links()
)

logger = logging.getLogger(__name__)

# ============================================================================
# TEMPORARY DIAGNOSTIC INSTRUMENTATION - Adidas/AJIO zero-product investigation
# ----------------------------------------------------------------------------
# Everything gated behind DIAGNOSTICS_ENABLED (here, and the block further
# down marked "END TEMPORARY DIAGNOSTICS") is read-only instrumentation: it
# logs what Playwright actually received for each rendered page and
# optionally dumps the raw HTML to disk when a page yields zero products.
# It does NOT change discovery logic, scoring, selectors, or any pipeline
# output (products/services/catalogue are untouched). Flip
# DIAGNOSTICS_ENABLED to False (or delete the block) once done.
DIAGNOSTICS_ENABLED = True

# Where zero-product page HTML gets dumped for manual inspection.
DEBUG_HTML_DIR = "debug_html"

# Extra bot-check / interstitial markers, on top of the ones already used by
# _looks_like_anti_bot_page() below - checked with NO length cap for
# diagnostics, since a captcha/interstitial can be embedded inside an
# otherwise large SPA HTML payload (unlike the production check, which only
# looks at short responses to avoid false positives on real pages).
_DIAG_EXTRA_BOT_MARKERS = (
    "captcha", "are you a robot", "verify you are human", "/sorry/",
    "unusual traffic", "bot detection", "automated access",
    "checking if the site connection is secure", "ray id",
)

_ADD_TO_CART_MARKERS = (
    "add to cart", "add to bag", "add to basket", "buy now",
)
# ============================================================================

DISCOVERY_VERSION = "product-discovery-v6"

# How many product pages we are willing to attempt to build the *full*
# catalogue (spec target: "if the company sells ~60 products, attempt the
# complete catalogue"). This is intentionally larger than config.MAX_PRODUCTS
# (which still governs how many products get scraped for reviews downstream,
# to keep review-collection runtime unchanged).
MAX_CATALOGUE_SIZE = 60

# Confidence floor - a candidate below this is discarded outright, however
# it was discovered. This is the safety net that stops "Access Denied",
# "Login", stray headings, etc. from ever reaching the dashboard, even if a
# future selector regression re-introduces them.
MIN_CONFIDENCE = 0.35

MAX_CATEGORY_PAGES = 12
MAX_PLAYWRIGHT_PAGES = 6
MAX_SCROLL_ROUNDS = 6
MAX_PAGINATION_PAGES = 4

# Pages kept open and reused within the ONE browser/context launched per
# analysis (see _PlaywrightSession). This bounds how many pages are
# rendered concurrently - it is a page-pool size, not a page-visit limit
# (that's MAX_PLAYWRIGHT_PAGES above).
PLAYWRIGHT_POOL_SIZE = 3

# Spec Step 5 (robots.txt -> Sitemap: -> sitemap.xml). Bounded so a sitemap
# index with hundreds of child sitemaps can't blow up runtime.
MAX_SITEMAPS_TO_FOLLOW = 3
MAX_SITEMAP_INDEX_CHILDREN = 3

# Below this many accepted products after the static + nav-category stages,
# we treat the site as "still thin" and keep escalating (sitemap, then
# Playwright) rather than settling for a near-empty catalogue.
THIN_RESULT_THRESHOLD = 8

CANDIDATE_PATH_SUFFIXES = (
    "/products", "/collections/all", "/shop", "/shop-all", "/collections",
    "/store", "/catalog", "/services", "/solutions",
)

# Additional CSS scopes checked by _discover_extra_category_links() for
# mega-menu / dropdown / flyout submenu panels and footer "shop by
# category" widgets - containers discover_category_links() (product_
# extraction.py; scoped to nav/header/[role=navigation]/[class*=menu i]/
# [class*=nav i]) does not look inside. A mega-menu panel is frequently a
# sibling <div>, not itself a <nav>, and a footer category grid is
# explicitly outside that function's scope, so both routinely get missed by
# the base scan alone (Improvement 1: hidden collections / mega-menus).
EXTRA_CATEGORY_SCOPES = (
    "[class*=mega i]", "[class*=megamenu i]", "[class*=dropdown i]",
    "[class*=submenu i]", "[class*=flyout i]", "[class*=nav-panel i]",
    "footer",
)

# Best-effort "Next page" control selectors, checked by _click_next_page()
# when a plain ?page=N URL bump doesn't surface new products - i.e. the
# storefront paginates via a stateful control rather than a predictable
# query param (Improvement 2: "Next Page buttons").
_NEXT_PAGE_SELECTORS = (
    "a[rel=next]",
    "link[rel=next]",
    "a[aria-label='Next']",
    "a[aria-label*=next i]",
    "button[aria-label*=next i]",
    "[class*=pagination i] a:has-text('Next')",
    "a:has-text('Next')",
    "button:has-text('Next')",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


class ProductDiscoveryResult:
    def __init__(
        self,
        catalogue: Optional[List[Product]] = None,
        discovery_method: str = "none",
        site_type: SiteType = SiteType.UNKNOWN,
        categories: Optional[List[str]] = None,
    ) -> None:
        self.catalogue = catalogue or []
        self.discovery_method = discovery_method
        self.site_type = site_type
        self.categories = categories or []

    def as_dict(self) -> Dict:
        ranked = sorted(self.catalogue, key=lambda p: p.confidence, reverse=True)
        product_items = [p for p in ranked if not p.is_service]
        service_items = [p for p in ranked if p.is_service]

        product_names = _unique_names(product_items)[: config.MAX_PRODUCTS]
        service_names = _unique_names(service_items)[: config.MAX_PRODUCTS]

        return {
            # --- backward-compatible keys ---
            "products": product_names,
            "services": service_names,
            "scrape_targets": product_names,
            "products_found": len(product_items),
            "services_found": len(service_items),
            "discovery_method": self.discovery_method,
            "discovery_version": DISCOVERY_VERSION,
            # --- new, additive keys ---
            "site_type": self.site_type.value,
            "categories": self.categories,
            "catalogue": [p.as_dict() for p in ranked[:MAX_CATALOGUE_SIZE]],
        }


def _unique_names(products: List[Product]) -> List[str]:
    seen: Set[str] = set()
    names: List[str] = []
    for p in products:
        lower = p.name.lower()
        if lower in seen:
            continue
        seen.add(lower)
        names.append(p.name)
    return names


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------

async def discover_products(company_data: Dict[str, str]) -> Dict:
    website = (company_data.get("website") or "").strip()
    company_name = (company_data.get("company_name") or "").strip()

    if not website:
        return ProductDiscoveryResult(discovery_method="skipped-no-website").as_dict()

    root = _root_url(website)
    stages_used: List[str] = []
    registry: Dict[str, Product] = {}
    categories_seen: List[str] = []

    # Tracks every URL fetched over plain HTTP during the static pipeline
    # (stages 3 / 3b) so a link discovered by both the nav crawl and the
    # sitemap crawl in the same analysis is never fetched twice
    # (Improvement 7: avoid duplicate requests / crawling the same page
    # twice). Playwright rendering is intentionally not deduped against
    # this set - it captures JS-rendered content the static fetch cannot.
    visited_urls: Set[str] = set()

    # --- Cheap probe: one plain HTTP request, no browser involved --------
    # This single request decides which of the two paths below the rest of
    # the analysis takes. It's what keeps the HTTP-friendly case (e.g.
    # headphonezone.in) fast: if this succeeds, Playwright is never even
    # imported for this analysis.
    homepage_html = await _fetch_static(root)
    visited_urls.add(root)
    site_type = detect_site_type(homepage_html or "")

    if not homepage_html:
        logger.info(
            "Using Playwright fallback for %s: plain HTTP homepage fetch "
            "did not return usable HTML (blocked status code or anti-bot "
            "interstitial).", root,
        )
        # --- Playwright-driven path (blocked / bot-protected homepage) ---
        # robots.txt -> sitemap.xml is plain XML/text and rarely sits
        # behind the same WAF as the storefront itself, so it's still
        # fetched over plain HTTP here; its links just seed the one
        # browser session below rather than triggering a browser of their
        # own.
        try:
            seed_links = await _fetch_sitemap_category_links(root, limit=MAX_CATEGORY_PAGES)
        except Exception as exc:
            logger.info("Sitemap discovery failed for %s: %s", root, exc)
            seed_links = []
        if seed_links:
            stages_used.append("sitemap")

        pw_registry, pw_categories, pw_site_type, _ = await _run_playwright_driven_discovery(
            root, seed_links, rediscover_categories=True,
        )
        if pw_registry:
            _ingest(registry, list(pw_registry.values()))
            stages_used.append("playwright-rendered")
        if pw_site_type is not SiteType.UNKNOWN:
            site_type = pw_site_type
        for c in pw_categories:
            if c not in categories_seen:
                categories_seen.append(c)

    else:
        # --- Static/HTTP path (the common, fast case) ---------------------
        # Stage 1 + 2: homepage structured data + HTML cards.
        _ingest(registry, parse_jsonld_products(homepage_html, root))
        _ingest(registry, parse_html_product_cards(homepage_html, root))
        if registry:
            stages_used.append("jsonld" if any("jsonld" in p.source for p in registry.values()) else "html")

        # Stage 3: category / collection page crawl.
        category_links = discover_category_links(homepage_html, root, limit=MAX_CATEGORY_PAGES)

        # Improvement 1: mega-menu / dropdown / flyout / footer category
        # links that discover_category_links() doesn't scope into. Purely
        # additive - never removes anything the base scan already found.
        extra_links = _discover_extra_category_links(
            homepage_html, root, exclude=set(category_links), limit=MAX_CATEGORY_PAGES,
        )
        if extra_links:
            logger.info(
                "Mega-menu/footer discovery found %d additional category "
                "link(s) for %s (nav crawl alone found %d).",
                len(extra_links), root, len(category_links),
            )
            category_links = list(dict.fromkeys([*category_links, *extra_links]))
            stages_used.append("mega-menu")

        category_links = _merge_default_paths(root, category_links)

        if category_links:
            crawl_targets = [u for u in category_links[:MAX_CATEGORY_PAGES] if u not in visited_urls]
            semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)
            results = await asyncio.gather(
                *(
                    _crawl_category_static_with_pagination(
                        url, semaphore, registry, categories_seen, visited_urls,
                    )
                    for url in crawl_targets
                )
            )
            if any(results):
                stages_used.append("category-crawl")

        # Stage 3b: sitemap.xml category discovery (spec Step 5). Only
        # bothered with when the nav crawl left us thin, since most sites
        # are already resolved by stage 3.
        if _accepted_count(registry) < THIN_RESULT_THRESHOLD:
            try:
                sitemap_links = await _fetch_sitemap_category_links(root, limit=MAX_CATEGORY_PAGES)
            except Exception as exc:
                logger.info("Sitemap discovery failed for %s: %s", root, exc)
                sitemap_links = []

            new_links = [
                link for link in sitemap_links
                if link not in category_links and link not in visited_urls
            ]
            if new_links:
                semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)
                results = await asyncio.gather(
                    *(
                        _crawl_category_static_with_pagination(
                            link, semaphore, registry, categories_seen, visited_urls,
                        )
                        for link in new_links[:MAX_CATEGORY_PAGES]
                    )
                )
                if any(results):
                    stages_used.append("sitemap")
                category_links.extend(new_links)

        # Stage 4: Playwright fallback - only if still thin. Homepage
        # already rendered fine over plain HTTP, so this exists purely to
        # catch JS-only category grids / infinite scroll. Uses the same
        # single-session architecture as the blocked-homepage path above -
        # one browser, launched once, closed once.
        if _accepted_count(registry) < THIN_RESULT_THRESHOLD:
            logger.info(
                "Using Playwright fallback for %s: static pipeline found "
                "only %d accepted product(s), below THIN_RESULT_THRESHOLD "
                "(%d).", root, _accepted_count(registry), THIN_RESULT_THRESHOLD,
            )
            try:
                pw_registry, pw_categories, _, _ = await _run_playwright_driven_discovery(
                    root, category_links, rediscover_categories=False,
                )
                if pw_registry:
                    _ingest(registry, list(pw_registry.values()))
                    stages_used.append("playwright")
                for c in pw_categories:
                    if c not in categories_seen:
                        categories_seen.append(c)
            except Exception as exc:
                logger.info("Playwright product discovery failed for %s: %s", root, exc)

    # --- Stage 5: score, filter, classify ---------------------------------
    final_catalogue: List[Product] = []
    for product in registry.values():
        product.score()
        if product.confidence < MIN_CONFIDENCE:
            continue
        if not is_valid_candidate_name(product.name):
            continue
        product.is_service = classify_as_service(product)
        final_catalogue.append(product)

    method = "+".join(stages_used) if stages_used else "none"
    result = ProductDiscoveryResult(
        catalogue=final_catalogue,
        discovery_method=method,
        site_type=site_type,
        categories=categories_seen,
    )
    payload = result.as_dict()

    # Improvement 8: a clear, explicit reason why discovery stopped where
    # it did, in addition to the summary line below - useful when a run
    # comes back thin and someone needs to know whether that's because the
    # catalogue really is small, or because a stage was never reached.
    accepted = len(final_catalogue) - sum(1 for p in final_catalogue if p.is_service)
    if accepted >= MAX_CATALOGUE_SIZE:
        stop_reason = f"reached MAX_CATALOGUE_SIZE ({MAX_CATALOGUE_SIZE})"
    elif stages_used:
        stop_reason = f"all applicable stages exhausted (stages_used={stages_used})"
    else:
        stop_reason = "no stage produced any accepted candidate"
    logger.info("Discovery for %s stopped because: %s", root, stop_reason)

    logger.info(
        "Product discovery for %s: %d products, %d services (method=%s, site_type=%s, "
        "categories=%d, urls_fetched=%d)",
        company_name, payload["products_found"], payload["services_found"],
        method, site_type.value, len(categories_seen), len(visited_urls),
    )
    return payload


# --------------------------------------------------------------------------
# Networking helpers (plain HTTP only - no Playwright in this section)
# --------------------------------------------------------------------------
#
# Many "professional" sites (Sony, Nike, Adidas, AJIO, ConceptKart, ...) sit
# behind bot protection / transient-error responses that return
# 401 / 403 / 429 / 500 / 502 / 503 / 504 to a plain httpx request, or -
# even when it returns HTTP 200 - serve an interstitial "checking your
# browser" / captcha page instead of the real markup.
#
# _fetch_static() detects both cases and simply returns "" for them - with
# ZERO retries over plain HTTP once one of these status codes is seen,
# since retrying the same blocked/erroring endpoint over and over just
# burns time that a real browser would spend rendering the page
# successfully instead. It used to escalate to a browser right there,
# per-URL - that per-URL escalation (a fresh browser launch for every
# blocked URL) was the direct cause of the "browser launched many times" /
# 6-10 minute runtimes. That responsibility now lives one level up, in
# discover_products(): a blocked homepage hands the WHOLE analysis to
# _run_playwright_driven_discovery() (below), which launches exactly one
# browser for everything that analysis still needs. Sites that work over
# plain HTTP (e.g. HeadphoneZone) never touch Playwright at all.

ANTI_BOT_STATUS_CODES = {401, 403, 429, 500, 502, 503, 504}

# Conservative, high-precision markers for "this 200 response is actually a
# bot-check interstitial, not the real page". Only checked against short
# responses, since a real product/category page is essentially never this
# small - this keeps the heuristic from ever misfiring on legitimate pages.
ANTI_BOT_MARKERS = (
    "checking your browser before accessing",
    "just a moment...",
    "cf-browser-verification",
    "attention required! | cloudflare",
    "enable javascript and cookies to continue",
    "please verify you are a human",
    "request unsuccessful. incapsula",
    "perimeterx",
    "you have been blocked",
    "access denied",
)
ANTI_BOT_MAX_LEN = 6000


def _looks_like_anti_bot_page(html: str) -> bool:
    if not html or len(html) > ANTI_BOT_MAX_LEN:
        return False
    lowered = html.lower()
    return any(marker in lowered for marker in ANTI_BOT_MARKERS)


async def _fetch_static(url: str) -> str:
    """Plain HTTP fetch, nothing more. Returns "" on any failure, including
    a bot-blocked status code or an anti-bot interstitial disguised as a
    200 - callers decide what to do about a blocked site (see
    discover_products()); this function never launches a browser itself."""
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=6.0),
            follow_redirects=True,
            headers=HEADERS,
            verify=False,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text
            if _looks_like_anti_bot_page(text):
                logger.info("Anti-bot challenge page detected (HTTP 200): %s", url)
                return ""
            return text
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in ANTI_BOT_STATUS_CODES:
            logger.info(
                "HTTP blocked (%d) for %s - skipping HTTP entirely (no "
                "retries) and switching to Playwright.", status, url,
            )
        else:
            logger.info("Static fetch failed for %s: HTTP %d", url, status)
        return ""
    except Exception as exc:
        logger.info("Static fetch failed for %s: %s", url, exc)
        return ""


async def _fetch_many(urls: List[str]) -> Dict[str, str]:
    semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)

    async def _one(url: str) -> tuple:
        async with semaphore:
            html = await _fetch_static(url)
            return url, html

    results = await asyncio.gather(*(_one(u) for u in urls), return_exceptions=False)
    return {url: html for url, html in results}


async def _crawl_category_static_with_pagination(
    url: str,
    semaphore: asyncio.Semaphore,
    registry: Dict[str, Product],
    categories_seen: List[str],
    visited: Set[str],
) -> bool:
    """Static-HTTP counterpart to _render_category_with_pagination() below:
    fetches one category page, then walks its ?page=N pagination over plain
    HTTP (no browser) as long as each new page still yields products not
    already in the registry, stopping automatically the moment a page
    fails to load or returns nothing new, or MAX_PAGINATION_PAGES is hit
    (Improvement 2: pagination + "stop automatically when no new products
    appear"). Also enforces the shared `visited` de-dup set so this URL -
    and each of its ?page=N steps - is only ever fetched once per analysis
    (Improvement 7).

    Returns True if the base URL fetch itself succeeded (used by callers
    purely to decide whether to record the stage as "used" in
    discovery_method - it does not gate whether products were found).
    """
    if url in visited:
        return False
    visited.add(url)

    async with semaphore:
        html = await _fetch_static(url)
    if not html:
        return False

    category_label = parse_breadcrumb_category(html) or _guess_category_from_url(url)
    if category_label and category_label not in categories_seen:
        categories_seen.append(category_label)
    _ingest(registry, parse_jsonld_products(html, url))
    _ingest(registry, parse_html_product_cards(html, url, category=category_label))

    for page_num in range(2, MAX_PAGINATION_PAGES + 1):
        if _accepted_count(registry) >= MAX_CATALOGUE_SIZE:
            break
        sep = "&" if "?" in url else "?"
        paged_url = f"{url}{sep}page={page_num}"
        if paged_url in visited:
            break
        visited.add(paged_url)

        async with semaphore:
            paged_html = await _fetch_static(paged_url)
        if not paged_html:
            break

        new_products = parse_jsonld_products(paged_html, paged_url) + \
            parse_html_product_cards(paged_html, paged_url, category=category_label)
        if not new_products:
            break
        _ingest(registry, new_products)

    return True


async def _fetch_sitemap_category_links(root: str, limit: int = 15) -> List[str]:
    """Spec Step 5: robots.txt -> Sitemap: -> sitemap.xml -> category URLs.

    Falls back to the conventional /sitemap.xml path if robots.txt doesn't
    declare one (many sites omit the "Sitemap:" line but still serve the
    file at the default location). Follows a bounded number of child
    sitemaps if the top-level file turns out to be a sitemap index.
    """
    # These are plain-text/XML resources, fetched over plain HTTP only.
    # Rendering them in a browser would hand back a syntax-highlighted DOM
    # instead of raw text, which the sitemap/robots parsers below can't
    # read. A WAF blocking these is also rarely solved by JS execution
    # anyway, so there's nothing to gain by escalating here.
    robots_txt = await _fetch_static(f"{root}/robots.txt")
    sitemap_urls = parse_robots_sitemaps(robots_txt)
    if not sitemap_urls:
        sitemap_urls = [f"{root}/sitemap.xml"]

    all_locs: List[str] = []
    for sitemap_url in sitemap_urls[:MAX_SITEMAPS_TO_FOLLOW]:
        xml_text = await _fetch_static(sitemap_url)
        if not xml_text:
            continue
        locs = extract_sitemap_locs(xml_text)
        if is_sitemap_index(xml_text):
            child_htmls = await _fetch_many(locs[:MAX_SITEMAP_INDEX_CHILDREN])
            for child_xml in child_htmls.values():
                all_locs.extend(extract_sitemap_locs(child_xml))
        else:
            all_locs.extend(locs)

    return filter_category_urls_from_sitemap(all_locs, root, limit=limit)


def _merge_default_paths(root: str, discovered: List[str]) -> List[str]:
    merged = list(dict.fromkeys(discovered))
    for suffix in CANDIDATE_PATH_SUFFIXES:
        candidate = f"{root}{suffix}"
        if candidate not in merged:
            merged.append(candidate)
    return merged


def _discover_extra_category_links(
    html: str,
    page_url: str,
    exclude: Optional[Set[str]] = None,
    limit: int = MAX_CATEGORY_PAGES,
) -> List[str]:
    """Supplements product_extraction.discover_category_links() with links
    that live inside mega-menu / dropdown / flyout panels or a footer "shop
    by category" widget - containers the base function doesn't scope into
    (it only looks at nav/header/[role=navigation]/[class*=menu i]/
    [class*=nav i]). A mega-menu flyout panel is frequently rendered as a
    sibling <div> rather than nested inside a <nav>, and footer category
    grids are outside that function's scope entirely, so both routinely go
    undiscovered by the base scan alone (Improvement 1). Uses the exact
    same hint/exclude word lists as discover_category_links() so results
    are held to the same bar - this is purely additive coverage, not a
    looser filter.

    Read-only, static BeautifulSoup parsing - never touches the network.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    root = _root_url(page_url)
    seen: Set[str] = set(exclude or ())

    found: List[str] = []
    try:
        scopes = soup.select(", ".join(EXTRA_CATEGORY_SCOPES))
    except Exception:
        return []

    for scope in scopes:
        for a in scope.find_all("a", href=True):
            text = (a.get_text(" ", strip=True) or "").lower()
            href = a["href"]
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full = urljoin(page_url, href)
            if not full.startswith(root) or full in seen:
                continue
            if any(w in text for w in _CATEGORY_EXCLUDE_WORDS):
                continue
            if any(w in text for w in _CATEGORY_HINT_WORDS) or any(
                w in full.lower() for w in _CATEGORY_HINT_WORDS
            ):
                seen.add(full)
                found.append(full)
                if len(found) >= limit:
                    return found

    return found


def _guess_category_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    segment = path.split("/")[-1]
    return segment.replace("-", " ").replace("_", " ").title()[: config.MAX_CANDIDATE_LEN]


# ============================================================================
# TEMPORARY DIAGNOSTICS - helper functions
# ----------------------------------------------------------------------------
# Called from exactly one place: _PlaywrightSession.goto_and_extract(), which
# is itself the single choke point every rendered page passes through -
# homepage, every discovered category page, and every ?page=N pagination
# step (see _render_category_with_pagination and the homepage render call in
# _run_playwright_driven_discovery). So instrumenting just that one method
# gives full per-page coverage without touching orchestration logic.
#
# Read-only: these functions only ever call logger.info(...) and (on a
# zero-product page) write a file under DEBUG_HTML_DIR. They never mutate
# the registry, never change what discover_products() returns.

def _diag_is_bot_check_page(html: str) -> bool:
    """Looser, uncapped bot-check/captcha detector for diagnostics only.
    (Deliberately separate from the production _looks_like_anti_bot_page()
    above, which caps at ANTI_BOT_MAX_LEN to avoid false positives on real
    pages - here we want to know even if a captcha snippet is buried inside
    a large SPA payload.)"""
    if not html:
        return False
    lowered = html.lower()
    if _looks_like_anti_bot_page(html):
        return True
    return any(marker in lowered for marker in _DIAG_EXTRA_BOT_MARKERS)


def _safe_filename_from_url(url: str) -> str:
    """Turns a URL into a filesystem-safe filename fragment."""
    parsed = urlparse(url)
    raw = f"{parsed.netloc}{parsed.path}"
    if parsed.query:
        raw += f"_{parsed.query}"
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw).strip("_")
    return (safe or "page")[:150]


def _save_debug_html_sync(url: str, html: str) -> Optional[str]:
    """Blocking file write - always called via asyncio.to_thread() below so
    it can't stall page rendering for other pages in the same gather()."""
    try:
        os.makedirs(DEBUG_HTML_DIR, exist_ok=True)
        base = _safe_filename_from_url(url)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"{base}__{stamp}__{uuid.uuid4().hex[:6]}.html"
        path = os.path.join(DEBUG_HTML_DIR, filename)
        with open(path, "w", encoding="utf-8", errors="ignore") as f:
            f.write(html or "")
        return path
    except Exception as exc:
        logger.info("[DIAG] Failed to save debug HTML for %s: %s", url, exc)
        return None


async def _log_page_diagnostics(page, requested_url: str, html: str) -> None:
    """Logs the per-page diagnostic snapshot requested for the Adidas/AJIO
    investigation, then dumps the HTML to disk if this page's own JSON-LD +
    HTML-card extraction came back with zero products."""
    try:
        final_url = page.url or requested_url
    except Exception:
        final_url = requested_url

    status = getattr(page, "_diag_last_status", None)

    try:
        title = await page.title()
    except Exception:
        title = ""

    html_len = len(html or "")

    soup = BeautifulSoup(html or "", "html.parser")
    all_links = soup.find_all("a", href=True)
    num_links = len(all_links)

    num_candidate_product_links = 0
    for a in all_links:
        try:
            href = urljoin(final_url, a["href"])
        except Exception:
            continue
        if _looks_like_product_url(href):
            num_candidate_product_links += 1

    # Re-uses the real extraction functions as-is (imported above) so these
    # counts reflect exactly what production discovery would find on this
    # page - no separate/duplicated heuristics to keep in sync.
    try:
        jsonld_products = parse_jsonld_products(html or "", final_url)
    except Exception:
        jsonld_products = []
    try:
        card_products = parse_html_product_cards(html or "", final_url)
    except Exception:
        card_products = []

    has_jsonld_product = bool(jsonld_products)
    num_candidate_cards = len(card_products)

    lowered = (html or "").lower()
    has_cart_cta = any(marker in lowered for marker in _ADD_TO_CART_MARKERS)
    is_bot_check = _diag_is_bot_check_page(html or "")

    logger.info(
        "[DIAG] requested_url=%s final_url=%s http_status=%s title=%r "
        "html_len=%d links=%d candidate_product_links=%d "
        "candidate_product_cards=%d jsonld_product=%s add_to_cart_text=%s "
        "bot_check_page=%s",
        requested_url, final_url, status, title, html_len, num_links,
        num_candidate_product_links, num_candidate_cards, has_jsonld_product,
        has_cart_cta, is_bot_check,
    )

    total_found = num_candidate_cards + len(jsonld_products)
    if total_found == 0:
        saved_path = await asyncio.to_thread(_save_debug_html_sync, final_url, html or "")
        if saved_path:
            logger.info(
                "[DIAG] Zero products extracted from %s - saved HTML to %s (%d bytes) for inspection.",
                final_url, saved_path, html_len,
            )
# END TEMPORARY DIAGNOSTICS
# ============================================================================


# --------------------------------------------------------------------------
# Playwright session (JS-rendered / bot-protected sites)
# --------------------------------------------------------------------------
#
# This replaces the old per-URL browser relaunching (both the fetch-level
# fallback and the old _discover_via_playwright). The design has exactly
# ONE call to run_playwright_async() in this entire module -
# _run_playwright_driven_discovery(), below - and it owns the complete
# lifecycle of one browser for one analysis: launch, every render it will
# ever need, then close. Nothing about a browser/context/page is ever
# stored on a long-lived object and reused across separate
# run_playwright_async() calls, which is what previously left Playwright
# objects bound to an execution context that had already gone away
# (-> 'NoneType' has no attribute 'send', Task was destroyed but pending,
# Event loop is closed, BaseSubprocessTransport warnings).

class _PlaywrightSession:
    """One browser + one context + a small pool of reusable pages, scoped
    to a single _run_playwright_driven_discovery() call. Must only ever be
    created and torn down *inside* the same run_playwright_async()
    invocation - never held across two separate calls to it."""

    def __init__(self, playwright, browser, context, pages: List) -> None:
        self._playwright = playwright
        self._browser = browser
        self._context = context
        self._all_pages = pages
        self._pool: asyncio.Queue = asyncio.Queue()
        for page in pages:
            self._pool.put_nowait(page)

    @classmethod
    async def launch(cls, pool_size: int = PLAYWRIGHT_POOL_SIZE) -> "_PlaywrightSession":
        from playwright.async_api import async_playwright

        logger.info("Launching Playwright (one browser for this analysis)...")
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=HEADERS["User-Agent"],
            locale="en-US",
            timezone_id="Asia/Kolkata",
            device_scale_factor=1,
            is_mobile=False,
            has_touch=False,
            extra_http_headers={
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

        # --- Stealth: reduce the headless/automation fingerprint -----------
        # Scoped entirely to this method, per instruction. Applied once via
        # context.add_init_script() so it runs before any page script on
        # every page in the pool (including ones created below). Only
        # patches the handful of properties that are the most commonly
        # checked headless-Chromium "tells" - it does not touch the
        # User-Agent (left unchanged as instructed), request headers,
        # extraction logic, or diagnostics elsewhere in this file.
        stealth_script = """
        (() => {
          // 1. navigator.webdriver - the single most common automation check.
          Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

          // 2. window.chrome - missing/incomplete in headless Chromium by default.
          window.chrome = window.chrome || { runtime: {} };

          // 3. Permissions API - headless Chromium answers a Notification
          //    permission query synchronously in a way that differs from a
          //    real profile; align it with the real Notification.permission.
          if (window.navigator.permissions && window.navigator.permissions.query) {
            const originalQuery = window.navigator.permissions.query.bind(window.navigator.permissions);
            window.navigator.permissions.query = (parameters) => (
              parameters && parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
            );
          }

          // 4. navigator.plugins / mimeTypes - empty arrays in headless by default.
          Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5].map(() => ({ name: 'Chrome PDF Plugin' })),
          });
          Object.defineProperty(navigator, 'mimeTypes', {
            get: () => [1, 2].map(() => ({ type: 'application/pdf' })),
          });

          // 5. navigator.languages - keep consistent with the context locale.
          Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
          });

          // 6. WebGL vendor/renderer - default headless value ("Google
          //    SwiftShader" / software rendering) is a well-known headless
          //    tell. Report a plausible real-GPU vendor/renderer instead.
          const patchWebGL = (proto) => {
            if (!proto) return;
            const originalGetParameter = proto.getParameter;
            proto.getParameter = function (parameter) {
              if (parameter === 37445) return 'Intel Inc.';               // UNMASKED_VENDOR_WEBGL
              if (parameter === 37446) return 'Intel Iris OpenGL Engine'; // UNMASKED_RENDERER_WEBGL
              return originalGetParameter.apply(this, arguments);
            };
          };
          patchWebGL(window.WebGLRenderingContext && window.WebGLRenderingContext.prototype);
          patchWebGL(window.WebGL2RenderingContext && window.WebGL2RenderingContext.prototype);
        })();
        """
        await context.add_init_script(stealth_script)

        pages = [await context.new_page() for _ in range(pool_size)]
        logger.info("Playwright session ready: 1 browser, 1 context, %d pages.", pool_size)
        return cls(playwright, browser, context, pages)


    async def acquire_page(self):
        """Check a page out of the pool. Blocks (does not create a new
        page) if all pages are currently in use - this is what bounds
        concurrency instead of a separate semaphore."""
        return await self._pool.get()

    async def release_page(self, page) -> None:
        await self._pool.put(page)

    async def render(self, url: str) -> str:
        """Convenience wrapper: check a page out, navigate, return HTML,
        check the page back in. Use acquire_page/release_page directly
        instead when a caller needs the same page across several
        navigations (e.g. pagination), to avoid checkout/release churn."""
        page = await self.acquire_page()
        try:
            return await self.goto_and_extract(page, url)
        finally:
            await self.release_page(page)

    async def goto_and_extract(self, page, url: str) -> str:
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            # Stashed on the page object purely so the diagnostics block
            # below can read it - not used anywhere in normal discovery.
            page._diag_last_status = response.status if response else None
            try:
                await page.wait_for_load_state("networkidle", timeout=6000)
            except Exception:
                pass
            await _auto_scroll_and_expand(page)
            html = await page.content()
            if DIAGNOSTICS_ENABLED:
                try:
                    await _log_page_diagnostics(page, url, html)
                except Exception as diag_exc:
                    # Diagnostics must never be able to break real discovery.
                    logger.info("[DIAG] instrumentation failed for %s: %s", url, diag_exc)
            return html
        except Exception as exc:
            logger.info("Playwright render failed for %s: %s", url, exc)
            return ""

    async def aclose(self) -> None:
        """Closes every page, the context, the browser, and stops the
        driver - called exactly once, from the finally block inside the
        same run_playwright_async() call that created this session, so the
        browser closes only after discovery has actually finished."""
        for page in self._all_pages:
            try:
                await page.close()
            except Exception:
                pass
        try:
            await self._context.close()
        except Exception:
            pass
        try:
            await self._browser.close()
        except Exception:
            pass
        try:
            await self._playwright.stop()
        except Exception:
            pass
        logger.info("Playwright session closed.")


async def _click_next_page(page) -> bool:
    """Best-effort click of a "Next page" control. Used as a supplementary
    pagination path in _render_category_with_pagination() when a plain
    ?page=N URL bump doesn't surface any new products at all - some
    storefronts paginate purely via a stateful "Next" control rather than a
    predictable query param (Improvement 2). Tries each selector in
    _NEXT_PAGE_SELECTORS in order and stops at the first one that is
    actually visible and clickable; returns False if none are, which is
    the normal/expected outcome for the many sites that don't paginate
    this way."""
    for sel in _NEXT_PAGE_SELECTORS:
        try:
            locator = page.locator(sel).first
            if await locator.is_visible(timeout=500):
                await locator.click(timeout=1500)
                try:
                    await page.wait_for_load_state("networkidle", timeout=6000)
                except Exception:
                    pass
                return True
        except Exception:
            continue
    return False


async def _render_category_with_pagination(
    session: _PlaywrightSession,
    url: str,
    registry: Dict[str, Product],
    categories_seen: List[str],
) -> None:
    """Renders one category/product page and walks its pagination, all on a
    single page checked out once from the pool (not re-acquired per
    pagination step).

    Two pagination strategies are tried, in order:
      1. ?page=N query-param bumps (cheap, no click involved) - stops the
         moment a page returns no HTML or no new products.
      2. If (1) produced nothing new on its very first attempt (page 2),
         the site likely doesn't support query-param pagination at all, so
         fall back to clicking a "Next page" control instead
         (_click_next_page), bounded the same way.
    Either way, discovery stops automatically the first time a step yields
    no new products, no HTML, or MAX_CATALOGUE_SIZE is reached.
    """
    page = await session.acquire_page()
    try:
        html = await session.goto_and_extract(page, url)
        if not html:
            return
        category_label = parse_breadcrumb_category(html) or _guess_category_from_url(url)
        if category_label and category_label not in categories_seen:
            categories_seen.append(category_label)
        _ingest(registry, parse_jsonld_products(html, url))
        _ingest(registry, parse_html_product_cards(html, url, category=category_label))

        used_query_pagination = False
        for page_num in range(2, MAX_PAGINATION_PAGES + 1):
            if _accepted_count(registry) >= MAX_CATALOGUE_SIZE:
                return
            sep = "&" if "?" in url else "?"
            paged_url = f"{url}{sep}page={page_num}"
            paged_html = await session.goto_and_extract(page, paged_url)
            if not paged_html:
                break
            new_products = parse_jsonld_products(paged_html, paged_url) + \
                parse_html_product_cards(paged_html, paged_url, category=category_label)
            if not new_products:
                break
            _ingest(registry, new_products)
            used_query_pagination = True

        if not used_query_pagination and _accepted_count(registry) < MAX_CATALOGUE_SIZE:
            # ?page=N produced nothing new right away - go back to the
            # original (unpaged) rendering and walk "Next" clicks instead.
            html = await session.goto_and_extract(page, url)
            if not html:
                return
            for _ in range(MAX_PAGINATION_PAGES - 1):
                if _accepted_count(registry) >= MAX_CATALOGUE_SIZE:
                    break
                clicked = await _click_next_page(page)
                if not clicked:
                    break
                await _auto_scroll_and_expand(page)
                try:
                    next_html = await page.content()
                except Exception:
                    break
                new_products = parse_jsonld_products(next_html, page.url) + \
                    parse_html_product_cards(next_html, page.url, category=category_label)
                if not new_products:
                    break
                _ingest(registry, new_products)
    finally:
        await session.release_page(page)


async def _run_playwright_driven_discovery(
    root: str,
    category_urls: List[str],
    rediscover_categories: bool = False,
) -> tuple:
    """The ONLY call site of run_playwright_async() in this module.

    Launches exactly one browser + one context for this call, renders the
    homepage plus every relevant category/product page off a small pool of
    reused pages (never a new page per URL, never a new browser per URL),
    and closes the whole session before returning - so a browser never
    outlives, and is never reused across, more than one of these calls.

    rediscover_categories=True means the plain-HTTP homepage fetch never
    even worked (403 / anti-bot), so once the homepage is rendered here,
    navigation/category links and internal product-page links are
    discovered straight from the *rendered* DOM (requirement 5) - both the
    base nav/header scan (discover_category_links) and the mega-menu/
    footer supplement (_discover_extra_category_links) - instead of
    guessing paths like /products, /shop, /catalog, /store, /services;
    those blind guesses are used only as a last-resort safety net if DOM
    discovery still comes back empty. When False (the static pipeline just
    came back thin), `category_urls` is already a good list from the
    static stages and is used as-is.
    """

    async def _coro() -> tuple:
        session: Optional[_PlaywrightSession] = None
        try:
            session = await _PlaywrightSession.launch()

            registry: Dict[str, Product] = {}
            categories_seen: List[str] = []

            homepage_html = await session.render(root)
            site_type = detect_site_type(homepage_html or "")
            if homepage_html:
                _ingest(registry, parse_jsonld_products(homepage_html, root))
                _ingest(registry, parse_html_product_cards(homepage_html, root))

            pages_to_visit = list(dict.fromkeys(category_urls))
            if rediscover_categories:
                nav_links = discover_category_links(homepage_html or "", root, limit=MAX_CATEGORY_PAGES)
                extra_links = _discover_extra_category_links(
                    homepage_html or "", root, exclude=set(nav_links), limit=MAX_CATEGORY_PAGES,
                )
                if extra_links:
                    logger.info(
                        "Mega-menu/footer discovery (rendered DOM) found %d "
                        "additional category link(s) for %s.", len(extra_links), root,
                    )
                pages_to_visit = list(dict.fromkeys([*nav_links, *extra_links, *pages_to_visit]))
                if not pages_to_visit:
                    # Last-resort safety net only - real nav/category links
                    # from the rendered DOM are always tried first.
                    pages_to_visit = _merge_default_paths(root, [])
            pages_to_visit = pages_to_visit[:MAX_PLAYWRIGHT_PAGES]

            if pages_to_visit:
                await asyncio.gather(
                    *(
                        _render_category_with_pagination(session, url, registry, categories_seen)
                        for url in pages_to_visit
                    )
                )

            return registry, categories_seen, site_type, homepage_html
        finally:
            if session is not None:
                await session.aclose()

    try:
        return await run_playwright_async(_coro)
    except Exception as exc:
        logger.info("Playwright-driven discovery failed for %s: %s", root, exc)
        return {}, [], SiteType.UNKNOWN, ""


async def _auto_scroll_and_expand(page) -> None:
    """Scroll to trigger lazy-loaded grids and click any visible 'load more'
    style button, up to a bounded number of rounds."""
    load_more_selectors = (
        "button:has-text('Load more')",
        "button:has-text('Show more')",
        "a:has-text('Load more')",
        "a:has-text('Show more')",
        "[class*=load-more i]",
        "[class*=show-more i]",
    )

    last_height = 0
    for _ in range(MAX_SCROLL_ROUNDS):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(700)
            clicked = False
            for sel in load_more_selectors:
                try:
                    locator = page.locator(sel).first
                    if await locator.is_visible(timeout=500):
                        await locator.click(timeout=1500)
                        await page.wait_for_timeout(900)
                        clicked = True
                        break
                except Exception:
                    continue
            height = await page.evaluate("document.body.scrollHeight")
            if height == last_height and not clicked:
                break
            last_height = height
        except Exception:
            break


# --------------------------------------------------------------------------
# Registry / merge helpers
# --------------------------------------------------------------------------

def _ingest(registry: Dict[str, Product], products: List[Product]) -> None:
    for product in products:
        if not is_valid_candidate_name(product.name):
            continue
        key = product.key()
        if key in registry:
            registry[key].merge(product)
        else:
            registry[key] = product


def _accepted_count(registry: Dict[str, Product]) -> int:
    count = 0
    for product in registry.values():
        if product.score() >= MIN_CONFIDENCE:
            count += 1
    return count


def _root_url(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
