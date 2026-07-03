import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

import config
from scrapers.browser_utils import run_playwright_async

logger = logging.getLogger(__name__)

DISCOVERY_VERSION = "product-discovery-v2"

MAX_PRODUCTS_TO_SCRAPE = config.MAX_PRODUCTS

CANDIDATE_PATH_SUFFIXES = (
    "/products", "/product", "/shop", "/shop-all", "/collections",
    "/collections/all", "/services", "/solutions", "/store", "/catalog",
    "/menu", "/categories",
)

NAV_HINT_WORDS = (
    "shop", "product", "products", "collection", "collections", "service",
    "services", "solution", "solutions", "store", "catalog", "menu",
    "category", "categories",
)

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

SERVICE_KEYWORDS = (
    "membership", "support", "delivery", "returns", "return policy",
    "subscription", "warranty", "installation", "consultation",
    "service", "services", "customer support", "customer care",
    "live chat", "financing", "insurance", "repair", "maintenance",
    "rental", "loyalty", "rewards", "gift card", "gift cards",
    "shipping", "exchange", "app", "helpdesk", "helpline", "booking",
    "reservation", "consulting", "training", "onboarding",
)

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

async def discover_products(company_data: Dict[str, str]) -> Dict:
    website = (company_data.get("website") or "").strip()
    company_name = (company_data.get("company_name") or "").strip()

    if not website:
        return ProductDiscoveryResult(discovery_method="skipped-no-website").as_dict()

    candidates: List[str] = []
    method = "none"

    try:
        candidates = await _discover_via_playwright(website)
        if candidates:
            method = "playwright"
    except Exception as exc:
        logger.info("Product discovery via Playwright failed for %s: %s", website, exc)
        candidates = []

    if len(candidates) < 3 and company_name:
        try:
            serp_candidates = await _discover_via_serp(company_name)
            if serp_candidates:
                candidates = _merge_unique(candidates, serp_candidates)
                method = "serp" if method == "none" else f"{method}+serp"
        except Exception as exc:
            logger.info("Product discovery via SERP failed for %s: %s", company_name, exc)

    if len(candidates) < 3:
        try:
            text_candidates = await _discover_via_homepage_text(website)
            if text_candidates:
                candidates = _merge_unique(candidates, text_candidates)
                method = "text" if method == "none" else f"{method}+text"
        except Exception as exc:
            logger.info("Product discovery via homepage text failed for %s: %s", website, exc)

    raw_products, services = _classify(candidates, company_name)

    products = _rank_and_select(raw_products, MAX_PRODUCTS_TO_SCRAPE)

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
    words = text.split()
    if not (1 <= len(words) <= 6):
        return False
    if text.lower() == company_name.lower():
        return False

    if text.endswith((".", "!", "?")) and len(words) > 4:
        return False
    return True

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

def _classify(candidates: List[str], company_name: str) -> Tuple[List[str], List[str]]:
    products: List[str] = []
    services: List[str] = []
    seen_lower: Set[str] = set()
    company_lower = (company_name or "").lower()

    for candidate in candidates:
        lower = candidate.lower()
        if lower in seen_lower:
            continue

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

def _is_blacklisted(text: str) -> bool:

    lower = text.lower().strip()
    if not lower:
        return True
    if lower in NOISE_TERMS:
        return True
    if any(word in lower for word in config.BLACKLIST_WORDS):
        return True

    if lower in config.EXACT_BADGE_TERMS:
        return True
    if re.fullmatch(r"#\s*\d+(\s*[-:.]?\s*(best\s?seller|new|featured|trending|hot|sale))?", lower):
        return True

    if re.search(r"\b(under|above|below)\b\s*[₹$€£]?\s*\d", lower):
        return True
    if re.search(r"[₹$€£]\s*\d[\d,]*\s*(?:-|–|to)\s*[₹$€£]?\s*\d", lower):
        return True

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