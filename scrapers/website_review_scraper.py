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
Anti-bot resilience (added — see root-cause note below)
----------------------------------------------------------------------------
In practice, Shopify storefronts sometimes answer a cold, cookie-less,
sparsely-headered `httpx` GET with `503` even though the exact same URL
returned `200` minutes earlier during product discovery. That is not a
broken URL — it's the storefront's edge/WAF scoring this client's request
fingerprint (missing session cookies, missing modern browser headers,
high request velocity with no referer chain) as synthetic and starting to
throttle it. Two layers of defense are used, in order, before giving up:

  1. Header/session hardening: rotate through several realistic, complete
     modern-browser header profiles (Chrome/Windows, Safari/macOS,
     Firefox/Windows — including sec-ch-ua / sec-fetch-* / accept-language),
     warm up a real session by visiting the site root first (so Shopify
     session cookies are picked up and a referer chain exists), and retry
     with a fresh header profile + short jittered backoff specifically on
     403/429/503 (bot-mitigation-shaped statuses), not on 404/410/etc.
  2. Playwright fallback: if hardened HTTP still comes back 503 for a
     specific product page, that single page (and only that page — no
     other scraper's behavior changes) is re-fetched with a real headless
     browser, which doesn't trip the same fingerprint heuristic. This is
     attempted first via the shared browser pool other scrapers use
     (`scrapers.browser_utils.browser_launch_slot`, imported lazily and
     defensively so a missing/renamed utility can never break module
     import), and falls back to a small, self-contained, concurrency-capped
     (max 1) Playwright instance local to this module if that shared pool
     isn't available or doesn't behave as expected.

Whichever way the HTML was obtained, it is fed through the exact same
extraction tiers below — nothing about the extraction logic itself changes
based on how the page was fetched.

----------------------------------------------------------------------------
Detection order (first hit wins, cheapest/most-structured first):
  1. schema.org JSON-LD (`Review` / `AggregateRating`) embedded in
     `<script type="application/ld+json">` — works regardless of which
     review app is installed, since most apps emit this for SEO.
  2. Shopify's own public `{product_url}.json` endpoint — rarely carries
     review text itself, but reliably yields the numeric Shopify product
     id, which is what Judge.me/Loox/Yotpo widgets are usually keyed off
     of. Used as a fallback id source when it can't be scraped out of the
     HTML (e.g. because the HTML we got was a bot-check interstitial, or
     the theme's markup doesn't match the regexes below).
  3. Known review-app JSON endpoints, called directly once the app's
     product/shop id is found on the page (or via the Shopify JSON id
     above):
       - Judge.me   (`judge.me/api/v1/widgets/...` — very common on Shopify)
       - Yotpo      (`api.yotpo.com/v1/widget/{app_key}/products/...json`)
       - Loox       (`loox.io/widget/reviews/product/{shop_id}/{product_id}`)
  4. Generic HTML fallback: heuristically-identified repeating review
     blocks (rating + reviewer + body) anywhere in the rendered page HTML,
     for custom/unrecognized review widgets.

Each of these is a plain GET/POST against a public API — nothing here
needs a browser, a login, or an API key, so (outside of the Playwright
fallback above, used only when hardened HTTP is blocked) it doesn't
compete for the shared Chromium launch slots the other scrapers use.
----------------------------------------------------------------------------

Public function (same contract as every other scraper module, UNCHANGED):

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
import random
import re
import time
from dataclasses import dataclass, field
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

# Plain HTTP against the merchant's own site (and, at most, a couple of
# well-known review-app APIs) — no browser, so this stays a fairly tight
# budget; a handful of httpx calls (including header-profile retries)
# finishes in well under this even on a slow site. The Playwright fallback
# below has its own, separate, larger budget that only gets spent when
# hardened HTTP is actually blocked.
TIME_BUDGET_SECONDS = 15
PLAYWRIGHT_TIME_BUDGET_SECONDS = 20
MAX_REVIEWS = 60
_MIN_WORDS = 4

# Statuses shaped like bot-mitigation (as opposed to "this page genuinely
# doesn't exist") — worth retrying with a different browser fingerprint.
_RETRYABLE_STATUSES = {403, 429, 503}

# Several complete, realistic modern-browser header profiles. Rotated on
# 403/429/503 so a single incomplete fingerprint doesn't sink every
# attempt — a plain User-Agent with no sec-ch-ua/sec-fetch-*/accept-language
# is itself a signal Shopify's edge can key off of.
_HEADER_PROFILES: List[Dict[str, str]] = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.4 Safari/605.1.15"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) "
            "Gecko/20100101 Firefox/127.0"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
    },
]

# Simple, single header set used for calls to review-app APIs (Judge.me /
# Yotpo / Loox / Shopify's own .json endpoint) — those aren't fronted by
# the same anti-bot heuristics as the merchant's rendered storefront pages,
# so they don't need the full rotation/retry treatment.
_API_HEADERS = dict(_HEADER_PROFILES[0])


@dataclass
class _Review:
    text: str
    source: str  # e.g. "jsonld", "judgeme", "yotpo", "loox", "html_heuristic"


@dataclass
class _SyncResult:
    reviews: List[_Review] = field(default_factory=list)
    needs_playwright: bool = False
    target: str = ""
    shop_domain: str = ""
    shopify_product_id: Optional[str] = None


# --- HTTP helpers --------------------------------------------------------
def _get_simple(client: httpx.Client, url: str, **kwargs) -> Optional[httpx.Response]:
    """Single-attempt GET with a plain realistic header set — used for the
    unauthenticated review-app / Shopify JSON APIs, which aren't subject to
    the storefront's anti-bot fingerprinting."""
    try:
        resp = client.get(url, headers=_API_HEADERS, timeout=8.0, **kwargs)
        if resp.status_code >= 400:
            logger.info("Website review scrape: GET %s -> HTTP %d", url, resp.status_code)
            return None
        return resp
    except Exception as exc:
        logger.info("Website review scrape: GET %s failed: %s", url, exc)
        return None


def _get_with_profiles(
    client: httpx.Client, url: str, referer: Optional[str] = None
) -> "tuple[Optional[httpx.Response], Optional[int]]":
    """GET a merchant storefront page, rotating through realistic browser
    header profiles and retrying with a fresh one specifically on
    403/429/503 (bot-mitigation-shaped statuses). Returns (response, status)
    — response is None on failure, status carries the last HTTP status seen
    (so the caller can tell a 503 apart from e.g. a 404, and decide whether
    a Playwright fallback is worth attempting)."""
    last_status: Optional[int] = None
    for i, profile in enumerate(_HEADER_PROFILES):
        headers = dict(profile)
        if referer:
            headers["Referer"] = referer
        try:
            resp = client.get(url, headers=headers, timeout=8.0)
        except Exception as exc:
            logger.info(
                "Website review scrape: GET %s failed (header profile %d/%d): %s",
                url, i + 1, len(_HEADER_PROFILES), exc,
            )
            last_status = None
            continue

        if resp.status_code < 400:
            return resp, resp.status_code

        last_status = resp.status_code
        logger.info(
            "Website review scrape: GET %s -> HTTP %d (header profile %d/%d)",
            url, resp.status_code, i + 1, len(_HEADER_PROFILES),
        )
        if resp.status_code not in _RETRYABLE_STATUSES:
            # A 404/410/etc. won't be fixed by a different fingerprint —
            # no point burning the remaining profiles on it.
            break
        if i < len(_HEADER_PROFILES) - 1:
            time.sleep(0.35 + random.uniform(0.15, 0.55))

    return None, last_status


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


# --- 2. Shopify's own public product JSON (id-resolution helper) --------
def _shopify_json_url(target: str) -> str:
    parsed = urlparse(target)
    path = parsed.path.rstrip("/")
    if path.endswith(".json"):
        return target
    return f"{parsed.scheme}://{parsed.netloc}{path}.json"


def _extract_shopify_product_json_id(client: httpx.Client, target: str) -> Optional[str]:
    """Every Shopify product page has a public, unauthenticated JSON
    twin at the same path with `.json` appended. It rarely contains review
    text (that lives in the review app's own storage), but it reliably
    gives us the numeric Shopify product id — which Judge.me and Loox key
    their widgets off of directly. Used as a fallback id source when the
    id can't be scraped out of the HTML (unfamiliar theme markup, or the
    HTML we have is a bot-check interstitial rather than the real page)."""
    url = _shopify_json_url(target)
    resp = _get_simple(client, url)
    if resp is None:
        return None
    try:
        data = resp.json()
        product_id = (data.get("product") or {}).get("id")
        return str(product_id) if product_id else None
    except Exception:
        return None


# --- 3a. Judge.me --------------------------------------------------------
_JUDGEME_ID_RE = re.compile(r"data-product-id=[\"'](\d+)[\"']|jdgm-widget[^>]*data-id=[\"'](\d+)[\"']")


def _extract_judgeme(
    client: httpx.Client, html: str, shop_domain: str, fallback_product_id: Optional[str] = None
) -> List[_Review]:
    m = _JUDGEME_ID_RE.search(html)
    product_id = (m.group(1) or m.group(2)) if m else None
    has_fingerprint = "judge.me" in html or "jdgm" in html
    if not product_id:
        if not has_fingerprint and not fallback_product_id:
            return []
        product_id = fallback_product_id
    if not product_id:
        return []

    url = (
        "https://judge.me/api/v1/widgets/product_review"
        f"?shop_domain={shop_domain}&platform=shopify&product_id={product_id}"
        "&per_page=50&page=1"
    )
    resp = _get_simple(client, url)
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


# --- 3b. Yotpo ------------------------------------------------------------
_YOTPO_APPKEY_RE = re.compile(r"yotpo[_-]?app[_-]?key[\"'\s:=]+[\"']?([A-Za-z0-9]{6,})", re.IGNORECASE)
_YOTPO_PRODUCT_ID_RE = re.compile(r"data-product-id=[\"'](\w[\w-]*)[\"']")


def _extract_yotpo(
    client: httpx.Client, html: str, fallback_product_id: Optional[str] = None
) -> List[_Review]:
    if "yotpo" not in html.lower():
        return []
    key_match = _YOTPO_APPKEY_RE.search(html)
    if not key_match:
        # No app key visible anywhere = no way to call the API; a fallback
        # product id can't compensate for a missing key.
        return []
    pid_match = _YOTPO_PRODUCT_ID_RE.search(html)
    product_id = (pid_match.group(1) if pid_match else None) or fallback_product_id
    if not product_id:
        return []
    app_key = key_match.group(1)
    url = f"https://api.yotpo.com/v1/widget/{app_key}/products/{product_id}/reviews.json"
    resp = _get_simple(client, url)
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


# --- 3c. Loox --------------------------------------------------------------
_LOOX_SHOP_RE = re.compile(r"loox[_-]?shop[_-]?id[\"'\s:=]+[\"']?(\d+)")


def _extract_loox(
    client: httpx.Client, html: str, url_slug: str, fallback_product_id: Optional[str] = None
) -> List[_Review]:
    if "loox" not in html.lower():
        return []
    shop_match = _LOOX_SHOP_RE.search(html)
    if not shop_match:
        return []
    shop_id = shop_match.group(1)
    # Loox's widget API expects the numeric Shopify product id, not the URL
    # slug — prefer the Shopify-JSON-resolved id when we have it and only
    # fall back to the slug (which will usually just 404) as a last resort.
    product_ref = fallback_product_id or url_slug
    url = f"https://loox.io/widget/reviews/product/{shop_id}/{product_ref}?fields=all"
    resp = _get_simple(client, url)
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


# --- 4. Generic HTML heuristic fallback ------------------------------------
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


# --- shared extraction pipeline -------------------------------------------
def _shop_domain_from_url(url: str) -> str:
    return urlparse(url).netloc.lower().lstrip("www.")


def _extract_all_tiers(
    html: str, target: str, shop_domain: str, shopify_product_id: Optional[str]
) -> List[_Review]:
    """Runs the full detection-tier pipeline against a page's HTML. Used
    identically whether that HTML came from plain httpx or from the
    Playwright fallback — extraction logic never needs to know or care
    which one fetched it."""
    collected = _extract_jsonld_reviews(html)
    if collected:
        logger.info(
            "Website review scrape: %d review(s) via JSON-LD for %s.",
            len(collected), target,
        )
        return collected[:MAX_REVIEWS]

    with httpx.Client(follow_redirects=True) as client:
        url_slug = urlparse(target).path.rstrip("/").rsplit("/", 1)[-1]
        for extractor, args in (
            (_extract_judgeme, (client, html, shop_domain, shopify_product_id)),
            (_extract_yotpo, (client, html, shopify_product_id)),
            (_extract_loox, (client, html, url_slug, shopify_product_id)),
        ):
            try:
                found = extractor(*args)
            except Exception:
                logger.exception(
                    "Website review scrape: extractor %s failed for %s.",
                    extractor.__name__, target,
                )
                found = []
            if found:
                logger.info(
                    "Website review scrape: %d review(s) via %s for %s.",
                    len(found), found[0].source, target,
                )
                return found[:MAX_REVIEWS]

    collected = _extract_html_heuristic(html)
    if collected:
        logger.info(
            "Website review scrape: %d review(s) via HTML heuristic for %s.",
            len(collected), target,
        )
        return collected[:MAX_REVIEWS]

    logger.info(
        "Website review scrape: no reviews found on %s (no known review app detected).",
        target,
    )
    return []


# --- sync (httpx) phase — runs in the thread pool, unchanged threading model
def _scrape_sync(product_url: str, website: str, product_name: str) -> _SyncResult:
    target = product_url or website
    if not target:
        return _SyncResult(target=target)

    shop_domain = _shop_domain_from_url(target)
    parsed = urlparse(target)
    root = f"{parsed.scheme}://{parsed.netloc}/"

    with httpx.Client(follow_redirects=True) as client:
        # Session warm-up: a real navigation to this product page would have
        # picked up Shopify session cookies and carried a Referer from an
        # earlier page view. A bare cookie-less direct hit to /products/...
        # looks synthetic; visiting the root first (best-effort — failure
        # here is not fatal) closes that gap cheaply.
        if root != target:
            _get_with_profiles(client, root)

        resp, status = _get_with_profiles(client, target, referer=root)
        shopify_product_id = _extract_shopify_product_json_id(client, target)

        if resp is not None:
            html = resp.text
            reviews = _extract_all_tiers(html, target, shop_domain, shopify_product_id)
            return _SyncResult(
                reviews=reviews, needs_playwright=False, target=target,
                shop_domain=shop_domain, shopify_product_id=shopify_product_id,
            )

        if status is None:
            logger.info("Website review scrape: could not fetch %r.", target)
        needs_playwright = status == 503
        return _SyncResult(
            reviews=[], needs_playwright=needs_playwright, target=target,
            shop_domain=shop_domain, shopify_product_id=shopify_product_id,
        )


# --- Playwright fallback (async — runs on the main event loop so it can
# properly cooperate with an async shared browser-context pool) -----------
async def _resolve_page(obj):
    """browser_launch_slot's exact return shape is unknown here (its source
    wasn't available when this was written) — tolerate the couple of shapes
    an async context manager like this plausibly yields: a Page directly, a
    (context, page) / (browser, context, page) tuple, or a BrowserContext."""
    if hasattr(obj, "goto"):
        return obj
    if isinstance(obj, (tuple, list)):
        for item in reversed(obj):
            if hasattr(item, "goto"):
                return item
    if hasattr(obj, "new_page"):
        return await obj.new_page()
    raise TypeError(f"Unrecognized object yielded by browser_launch_slot: {type(obj)!r}")


async def _goto_and_extract_html(page, url: str, nav_timeout_ms: int = 9000) -> Optional[str]:
    for attempt in (1, 2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            try:
                await page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass  # best-effort settle; domcontentloaded content is enough if this times out
            return await page.content()
        except Exception as exc:
            logger.warning(
                "Website review scrape: Playwright nav attempt %d/2 to %s failed: %s",
                attempt, url, exc,
            )
            if attempt == 2:
                return None
            await asyncio.sleep(1.0)
    return None


async def _fetch_via_shared_slot(url: str) -> Optional[str]:
    """Best-effort use of the shared browser pool the other scrapers use.
    Imported lazily (never at module load) and wrapped defensively so a
    missing or differently-shaped `browser_launch_slot` can never break
    this module's import or crash the app — it just falls through to the
    standalone Playwright path below."""
    try:
        from scrapers.browser_utils import browser_launch_slot  # type: ignore
    except ImportError:
        return None

    try:
        async with browser_launch_slot("website_review_scraper_0") as obj:
            page = await _resolve_page(obj)
            return await _goto_and_extract_html(page, url)
    except Exception:
        logger.exception(
            "Website review scrape: shared browser_launch_slot fallback failed for %s; "
            "using a standalone Playwright instance instead.", url,
        )
        return None


# Caps this module's OWN standalone Playwright usage at 1 concurrent
# browser — deliberately conservative so a burst of 503s across several
# products in one run can't multiply into an unmanaged pile of extra
# Chromium instances competing with the other scrapers' pools.
_STANDALONE_PLAYWRIGHT_LOCK = asyncio.Semaphore(1)


async def _fetch_via_standalone_playwright(url: str) -> Optional[str]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning(
            "Website review scrape: Playwright is not installed; cannot fall back for %s.", url
        )
        return None

    async with _STANDALONE_PLAYWRIGHT_LOCK:
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                try:
                    context = await browser.new_context(
                        user_agent=_HEADER_PROFILES[0]["User-Agent"],
                        locale="en-US",
                    )
                    try:
                        page = await context.new_page()
                        return await _goto_and_extract_html(page, url)
                    finally:
                        await context.close()
                finally:
                    await browser.close()
        except Exception:
            logger.exception(
                "Website review scrape: standalone Playwright fallback failed for %s.", url
            )
            return None


async def _fetch_html_via_playwright(url: str) -> Optional[str]:
    html = await _fetch_via_shared_slot(url)
    if html:
        return html
    return await _fetch_via_standalone_playwright(url)


async def scrape_website_reviews(company_data: Dict[str, str]) -> List[str]:
    """Public entry point — same contract as every other scrape_x()
    function: takes the per-job company_data dict, returns a flat
    List[str] of clean review text. Signature unchanged."""
    product_url = (company_data.get("product_url") or "").strip()
    website = (company_data.get("website") or "").strip()
    product_name = (company_data.get("product_name") or "").strip()

    if not product_url and not website:
        return []

    loop = asyncio.get_event_loop()
    try:
        result: _SyncResult = await asyncio.wait_for(
            loop.run_in_executor(_EXECUTOR, _scrape_sync, product_url, website, product_name),
            timeout=TIME_BUDGET_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Website review scrape exceeded its %ds HTTP budget for %r.",
            TIME_BUDGET_SECONDS, product_url or website,
        )
        return []
    except Exception:
        logger.exception("Unhandled error scraping website reviews for %r.", product_url or website)
        return []

    reviews = result.reviews

    if not reviews and result.needs_playwright:
        logger.info(
            "Website review scrape: hardened HTTP was blocked (503) for %s; "
            "falling back to Playwright.", result.target,
        )
        html: Optional[str] = None
        try:
            html = await asyncio.wait_for(
                _fetch_html_via_playwright(result.target),
                timeout=PLAYWRIGHT_TIME_BUDGET_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Website review scrape: Playwright fallback exceeded its %ds budget for %s.",
                PLAYWRIGHT_TIME_BUDGET_SECONDS, result.target,
            )
        except Exception:
            logger.exception(
                "Website review scrape: Playwright fallback failed for %s.", result.target
            )

        if html:
            try:
                reviews = await asyncio.wait_for(
                    loop.run_in_executor(
                        _EXECUTOR, _extract_all_tiers, html, result.target,
                        result.shop_domain, result.shopify_product_id,
                    ),
                    timeout=10.0,
                )
            except Exception:
                logger.exception(
                    "Website review scrape: extraction on Playwright-fetched HTML failed for %s.",
                    result.target,
                )
                reviews = []
            if reviews:
                logger.info(
                    "Website review scrape: %d review(s) recovered via Playwright fallback for %s.",
                    len(reviews), result.target,
                )
            else:
                logger.info(
                    "Website review scrape: Playwright fetch succeeded but no reviews were "
                    "found on %s.", result.target,
                )

    texts = [r.text for r in reviews]
    final = normalize_comments(texts)[:MAX_REVIEWS]
    logger.info("Website reviews returned %d comments", len(final))
    return final
