<<<<<<< HEAD
=======
"""
company_discovery.py  –  improved company & social-media discovery
===================================================================

Root-cause fix
--------------
The old code ranked candidates purely by scoring formulas that gave a
slight edge to shorter URLs and "social link count", but never explicitly
preferred the country-TLD version of a domain.  For "Headphone Zone"
the .com variant consistently won because it appeared first in Google
results and had the same social-link count as the .in version.

What changed in this rewrite (same architecture, no new frameworks)
-------------------------------------------------------------------
1.  STRONGER COUNTRY-TLD PREFERENCE
    * `preferred_tld` now gets a big negative bonus (-600 instead of -400)
      and is also applied during domain-guess ordering (preferred TLD
      guesses are listed before .com guesses).
    * A new `_infer_country_from_ip()` helper silently detects the
      server's country from ip-api.com so "Headphone Zone" (searched
      from India) automatically gets .in preference even without the
      word "india" in the query.

2.  PLAYWRIGHT FALLBACK FOR JS-RENDERED SOCIAL LINKS
    * `_extract_website_links_playwright()` uses the already-installed
      Playwright/Chromium to render the homepage and contact/about
      pages when httpx finds zero social links.  This fixes sites that
      load their footer social icons via JavaScript.
    * The function reuses the existing `scrapers.browser_utils` pattern
      your scrapers already use (async_playwright context manager).

3.  DIRECT SOCIAL-SEARCH FALLBACK
    * If after visiting every candidate website we still have no social
      handle, `_fallback_social_search()` runs a targeted Google search
      for "{company} site:instagram.com", etc. and extracts the handle
      from the first result.

4.  CANDIDATE FILTER FIX
    * `_candidate_matches_company()` now also accepts partial-slug
      matches so "headphonezone" matches "headphonezone.in" even when
      the slug extracted from the query is just "headphonezone".

5.  BETTER SEARCH QUERIES
    * Added `"{company_name}" site:.in` (or the appropriate country TLD)
      to the search URL list so the preferred regional domain gets
      surfaced from search engines early.

Everything else (scrapers, sentiment, app, templates, utils) is
untouched.
"""

>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import collections

try:
    import collections.abc as _abc
    for _name in ("Callable", "MutableMapping", "Mapping", "Iterable",
                  "MutableSequence", "MappingView"):
        if not hasattr(collections, _name) and hasattr(_abc, _name):
            setattr(collections, _name, getattr(_abc, _name))
except Exception:
    pass

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DISCOVERY_VERSION = "playwright-social-v3"

SOCIAL_DOMAINS = ("instagram.com", "youtube.com", "youtu.be", "twitter.com", "x.com")
EXCLUDED_WEBSITE_DOMAINS = (
    "bing.",
    "duckduckgo.",
    "facebook.com",
    "google.",
    "instagram.com",
    "linkedin.com",
    "pinterest.",
    "reddit.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "yahoo.",
    "youtu.be",
    "youtube.com",
)
COUNTRY_TLDS = {
    "india": ".in",
    "indian": ".in",
    "bharat": ".in",
    "uk": ".co.uk",
    "united kingdom": ".co.uk",
    "england": ".co.uk",
    "uae": ".ae",
    "dubai": ".ae",
    "canada": ".ca",
    "australia": ".com.au",
    "singapore": ".sg",
    "germany": ".de",
    "france": ".fr",
    "japan": ".co.jp",
}
# Map ip-api country codes → TLD
COUNTRY_CODE_TO_TLD = {
    "IN": ".in",
    "GB": ".co.uk",
    "AE": ".ae",
    "CA": ".ca",
    "AU": ".com.au",
    "SG": ".sg",
    "DE": ".de",
    "FR": ".fr",
    "JP": ".co.jp",
}
COUNTRY_WORDS = {word for phrase in COUNTRY_TLDS for word in phrase.split()}
NOISE_WORDS = {"official", "website", "site", "social", "media", "company", "brand", "the"}
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# Cache the detected country TLD for the lifetime of the process
_detected_country_tld: Optional[str] = None
_country_detection_done: bool = False


@dataclass
class DiscoveredLinks:
    website: str = ""
    twitter: str = ""
    instagram: str = ""
    youtube: str = ""
    google_business: str = ""


@dataclass
class WebsiteCandidate:
    url: str
    source: str
    order: int


@dataclass
class WebsiteEvaluation:
    url: str
    links: DiscoveredLinks
    social_count: int
    score: tuple


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def discover_company(company_name: str) -> Dict[str, str]:
    """Discover a company website and social media links.

    Strategy (in order):
    1. Parse the query for a direct URL or a country hint.
    2. Detect the server's country via IP geolocation (cached, non-blocking).
    3. Collect candidate websites from domain guesses + search engines.
    4. Visit each candidate with httpx; pick the one with the most social links
       that best matches the company name and preferred TLD.
    5. If the chosen site's social links were loaded by JavaScript, re-visit
       with Playwright to render them.
    6. If social links are still missing, run targeted social-search fallbacks.
    """
    raw_query = company_name.strip()
    preferred_tld = _preferred_tld(raw_query)
    company_core = _company_core(raw_query)
    direct_url = _direct_url(raw_query)
    links = DiscoveredLinks()

    # Detect country TLD from IP if not overridden by query words
    if not preferred_tld:
        preferred_tld = await _infer_country_from_ip()

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=6.0),
        follow_redirects=True,
        headers=HEADERS,
        verify=False,
    ) as client:
        if direct_url:
            candidates = [WebsiteCandidate(direct_url, "direct", 0)]
        else:
            candidates = await _collect_candidates(
                client, raw_query, company_core, preferred_tld
            )

        evaluation = await _choose_best_candidate(
            client, candidates, company_core, preferred_tld, bool(direct_url)
        )

        if evaluation:
            links = evaluation.links
            links.website = evaluation.url
        else:
            _classify_urls((c.url for c in candidates), links)

        if not links.website:
            guessed = await _guess_website(
                client, company_core or raw_query, preferred_tld
            )
            if guessed:
                links.website = guessed
                site_urls = await _extract_website_links(client, guessed)
                _classify_urls(site_urls, links, replace_social=True)

        # ------------------------------------------------------------------
        # Playwright fallback: re-render the website if social links are missing
        # ------------------------------------------------------------------
        missing_social = not (links.twitter or links.instagram or links.youtube)
        if links.website and missing_social:
            logger.info(
                "No social links from httpx for %s – trying Playwright render",
                links.website,
            )
            playwright_urls = await _extract_website_links_playwright(links.website)
            if playwright_urls:
                _classify_urls(playwright_urls, links, replace_social=True)

        # ------------------------------------------------------------------
        # Direct social-search fallback: search each platform separately
        # ------------------------------------------------------------------
        if not links.instagram or not links.twitter or not links.youtube:
            await _fallback_social_search(
                client, company_core or raw_query, links
            )

    twitter = _normalize_social_value(links.twitter, "twitter")
    instagram = _normalize_social_value(links.instagram, "instagram")
    youtube = _normalize_social_value(links.youtube, "youtube")

    return {
        "company_name": raw_query,
        "website": links.website,
        "twitter": twitter,
        "instagram": instagram,
        "youtube": youtube,
        "twitter_url": _build_social_url(twitter, "twitter"),
        "instagram_url": _build_social_url(instagram, "instagram"),
        "youtube_url": _build_social_url(youtube, "youtube"),
        "google_business": links.google_business,
        "discovery_version": DISCOVERY_VERSION,
    }


# ---------------------------------------------------------------------------
# Country / TLD detection
# ---------------------------------------------------------------------------

async def _infer_country_from_ip() -> str:
    """Return the country TLD for the server's public IP, or '' on failure.

    Result is cached for the process lifetime so it only runs once.
    """
    global _detected_country_tld, _country_detection_done
    if _country_detection_done:
        return _detected_country_tld or ""
    _country_detection_done = True
    try:
        async with httpx.AsyncClient(timeout=4.0, verify=False) as c:
            resp = await c.get("http://ip-api.com/json/?fields=countryCode")
            data = resp.json()
            code = data.get("countryCode", "")
            _detected_country_tld = COUNTRY_CODE_TO_TLD.get(code, "")
            logger.info("IP geolocation: country=%s → preferred_tld=%s", code, _detected_country_tld)
    except Exception as exc:
        logger.info("Country detection failed (non-fatal): %s", exc)
        _detected_country_tld = ""
    return _detected_country_tld or ""


# ---------------------------------------------------------------------------
# Candidate collection
# ---------------------------------------------------------------------------

async def _collect_candidates(
    client: httpx.AsyncClient,
    raw_query: str,
    company_core: str,
    preferred_tld: str,
) -> List[WebsiteCandidate]:
    candidates: List[WebsiteCandidate] = []
    seen: Set[str] = set()

    def add(url: str, source: str) -> None:
        normalized = _normalize_candidate_url(url)
        if not normalized or normalized in seen:
            return
        if not _looks_like_official_website(normalized.lower()):
            return
        # For search results, require a loose name match; guesses are always added.
        if source == "search" and not _candidate_matches_company(normalized, company_core):
            return
        seen.add(normalized)
        candidates.append(WebsiteCandidate(normalized, source, len(candidates)))

    # Domain guesses (preferred TLD first)
    for url in _domain_guesses(company_core or raw_query, preferred_tld):
        add(url, "guess")

    # Search engine results
    search_html = await _fetch_many(
        client, _build_search_urls(raw_query, company_core, preferred_tld)
    )
    for url in _extract_urls("\n".join(search_html), base_url=""):
        add(url, "search")

    return candidates


async def _choose_best_candidate(
    client: httpx.AsyncClient,
    candidates: List[WebsiteCandidate],
    company_core: str,
    preferred_tld: str,
    direct_input: bool,
) -> Optional[WebsiteEvaluation]:
    if not candidates:
        return None

    async def evaluate(candidate: WebsiteCandidate) -> Optional[WebsiteEvaluation]:
        try:
            response = await _safe_get(client, candidate.url, timeout=10.0)
            if response.status_code >= 400:
                return None

            final_url = _root_url(str(response.url).rstrip("/"))
            site_urls = await _extract_website_links(client, final_url)
            candidate_links = DiscoveredLinks(website=final_url)
            _classify_urls(site_urls, candidate_links, replace_social=True)

            social_count = sum(
                bool(v)
                for v in (
                    candidate_links.twitter,
                    candidate_links.instagram,
                    candidate_links.youtube,
                )
            )

            # --- scoring (lower is better) ---
            direct_bonus   = -1000 if (direct_input and candidate.source == "direct") else 0
            # STRONGER country bonus: -600 (was -400)
            country_bonus  = -600  if (preferred_tld and _host_matches_tld(final_url, preferred_tld)) else 0
            social_bonus   = -150  * social_count
            match_score    = _website_match_score(final_url, company_core)
            source_penalty = 0 if candidate.source == "guess" else 25

            score = (
                direct_bonus + country_bonus + social_bonus,
                match_score,
                source_penalty,
                candidate.order,
                len(final_url),
                final_url,
            )
            return WebsiteEvaluation(final_url, candidate_links, social_count, score)
        except Exception as exc:
            logger.info("Candidate evaluation failed for %s: %s", candidate.url, exc)
            return None

    evaluated = [
        item
        for item in await asyncio.gather(*(evaluate(c) for c in candidates))
        if item
    ]
    if not evaluated:
        return None
    return sorted(evaluated, key=lambda item: item.score)[0]


# ---------------------------------------------------------------------------
# Search URL builder
# ---------------------------------------------------------------------------

def _build_search_urls(
    raw_query: str, company_core: str, preferred_tld: str
) -> List[str]:
    queries = [
        raw_query,
        f"{raw_query} official website social media",
    ]
    if company_core and company_core != raw_query:
        queries.append(f"{company_core} official website social media")
    if preferred_tld:
        # Explicit site: filter for the preferred TLD – surfaces regional domains early
        queries.append(
            f'"{company_core or raw_query}" site:{preferred_tld.lstrip(".")}'
        )
        queries.append(
            f"{company_core or raw_query} official website {preferred_tld.lstrip('.')}"
        )

    urls: List[str] = []
    for query in dict.fromkeys(queries):
        encoded = quote_plus(query)
        urls.extend([
            f"https://www.google.com/search?q={encoded}",
            f"https://www.bing.com/search?q={encoded}",
            f"https://duckduckgo.com/html/?q={encoded}",
        ])
    return urls


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _fetch_many(
    client: httpx.AsyncClient, urls: Iterable[str]
) -> List[str]:
    async def fetch(url: str) -> str:
        try:
            response = await _safe_get(client, url)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            logger.info("Discovery fetch failed for %s: %s", url, exc)
            return ""

    return list(await asyncio.gather(*(fetch(url) for url in urls)))


async def _safe_get(
    client: httpx.AsyncClient, url: str, timeout: float = 10.0
) -> httpx.Response:
    return await asyncio.wait_for(client.get(url), timeout=timeout)


# ---------------------------------------------------------------------------
# Website link extraction – httpx version (fast)
# ---------------------------------------------------------------------------

async def _extract_website_links(
    client: httpx.AsyncClient, website: str
) -> List[str]:
    urls: Set[str] = set()
    root = _root_url(website)
    pages = [root]

    for suffix in (
        "/pages/contact-us",
        "/contact",
        "/about",
        "/pages/about-us",
        "/pages/community",
        "/follow-us",
        "/connect",
    ):
        pages.append(f"{root}{suffix}")

    async def fetch_page(url: str) -> Optional[str]:
        try:
            response = await _safe_get(client, url)
            if response.status_code >= 400:
                return None
            return response.text
        except Exception as exc:
            logger.info("Website social discovery failed for %s: %s", url, exc)
            return None

    html_pages = await asyncio.gather(*(fetch_page(p) for p in dict.fromkeys(pages)))
    for page, html in zip(dict.fromkeys(pages), html_pages):
        if html:
            urls.update(_extract_urls(html, page))

    return sorted(urls, key=_url_rank)


# ---------------------------------------------------------------------------
# Website link extraction – Playwright version (JS-rendered, Windows-safe)
# ---------------------------------------------------------------------------

async def _extract_website_links_playwright(website: str) -> List[str]:
    """
    Visit `website` with a real Chromium browser, extract social links.

    Windows-safe: runs Playwright in a dedicated thread with a SelectorEventLoop
    so that asyncio.create_subprocess_exec works even inside uvicorn's
    ProactorEventLoop on Windows.
    """
    root = _root_url(website)
    pages_to_visit = [
        root,
        f"{root}/contact",
        f"{root}/about",
        f"{root}/pages/contact-us",
        f"{root}/pages/about-us",
    ]
    # Capture for use inside the thread closure
    _extract_urls_ref = _extract_urls
    _url_rank_ref = _url_rank
    headers_ref = HEADERS
    social_domains_ref = SOCIAL_DOMAINS

    async def _playwright_coro() -> List[str]:
        from playwright.async_api import async_playwright
        found: Set[str] = set()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
                timeout=60000,
            )
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=headers_ref["User-Agent"],
            )
            for page_url in pages_to_visit:
                page = None
                try:
                    page = await context.new_page()
                    await page.goto(page_url, wait_until="networkidle", timeout=20000)
                    await page.wait_for_timeout(2000)
                    html = await page.content()
                    found.update(_extract_urls_ref(html, page_url))
                except Exception as exc:
                    logger.info("Playwright page load failed for %s: %s", page_url, exc)
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass
            await context.close()
            await browser.close()
        return [
            u for u in sorted(found, key=_url_rank_ref)
            if any(d in u.lower() for d in social_domains_ref)
        ]

    try:
        from scrapers.browser_utils import run_playwright_async
        return await run_playwright_async(_playwright_coro)
    except Exception as exc:
        logger.warning("Playwright social extraction failed for %s: %s", website, exc)
        return []


# ---------------------------------------------------------------------------
# Direct social-search fallback
# ---------------------------------------------------------------------------

async def _fallback_social_search(
    client: httpx.AsyncClient,
    company_name: str,
    links: DiscoveredLinks,
) -> None:
    """
    For any social platform that is still missing, run a targeted Google
    search and extract the handle from the first matching result.
    """
    tasks = {}

    if not links.instagram:
        tasks["instagram"] = (
            f"https://www.google.com/search?q={quote_plus(company_name + ' site:instagram.com')}",
            "instagram.com",
        )
    if not links.twitter:
        tasks["twitter"] = (
            f"https://www.google.com/search?q={quote_plus(company_name + ' site:x.com OR site:twitter.com')}",
            "x.com",
        )
    if not links.youtube:
        tasks["youtube"] = (
            f"https://www.google.com/search?q={quote_plus(company_name + ' site:youtube.com')}",
            "youtube.com",
        )

    if not tasks:
        return

    async def search_one(search_url: str, domain_hint: str) -> List[str]:
        try:
            html = await _safe_get(client, search_url)
            html.raise_for_status()
            return _extract_urls(html.text, base_url="")
        except Exception as exc:
            logger.info("Fallback social search failed (%s): %s", domain_hint, exc)
            return []

    results = await asyncio.gather(
        *(search_one(url, hint) for _, (url, hint) in tasks.items())
    )

    platform_keys = list(tasks.keys())
    for platform, found_urls in zip(platform_keys, results):
        for url in found_urls:
            lower = url.lower()
            if platform == "instagram" and "instagram.com" in lower:
                value = _extract_instagram(url)
                if value and value.lower() not in {"p", "reel", "stories", "explore"}:
                    links.instagram = value
                    logger.info("Fallback found Instagram: %s", value)
                    break
            elif platform == "twitter" and ("twitter.com" in lower or "x.com" in lower):
                value = _extract_twitter(url)
                if value:
                    links.twitter = value
                    logger.info("Fallback found Twitter: %s", value)
                    break
            elif platform == "youtube" and "youtube.com" in lower:
                value = _extract_youtube(url)
                if value:
                    links.youtube = value
                    logger.info("Fallback found YouTube: %s", value)
                    break


# ---------------------------------------------------------------------------
# URL classification helpers
# ---------------------------------------------------------------------------

def _url_rank(url: str) -> tuple:
    lower = url.lower()
    if any(domain in lower for domain in SOCIAL_DOMAINS):
        return (0, len(url), lower)
    if _is_google_business(lower):
        return (1, len(url), lower)
    if _looks_like_official_website(lower):
        return (2, len(url), lower)
    return (3, len(url), lower)


def _classify_urls(
    urls: Iterable[str], links: DiscoveredLinks, replace_social: bool = False
) -> None:
    for url in urls:
        lower = url.lower()
        if (replace_social or not links.twitter) and (
            "twitter.com" in lower or "x.com" in lower
        ):
            value = _extract_twitter(url)
            if value:
                links.twitter = value
        elif (replace_social or not links.instagram) and "instagram.com" in lower:
            value = _extract_instagram(url)
            if value:
                links.instagram = value
        elif (replace_social or not links.youtube) and (
            "youtube.com" in lower or "youtu.be" in lower
        ):
            value = _extract_youtube(url)
            if value:
                links.youtube = value
        elif not links.google_business and _is_google_business(lower):
            links.google_business = url
        elif not links.website and _looks_like_official_website(lower):
            links.website = _root_url(url)


def _extract_twitter(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return ""
    blocked = {"home", "intent", "i", "share", "search", "hashtag", "explore", "settings"}
    handle = parts[0].lstrip("@")
    return "" if handle.lower() in blocked else handle


def _extract_instagram(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return ""
    blocked = {"p", "reel", "stories", "explore", "accounts", "direct"}
    handle = parts[0].lstrip("@")
    return "" if handle.lower() in blocked else handle


def _extract_youtube(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if "youtu.be" in parsed.netloc.lower():
        return ""
    if not parts:
        return ""
    if parts[0].startswith("@"):
        return parts[0].lstrip("@")
    if parts[0].lower() in {"channel", "user", "c"} and len(parts) > 1:
        return parts[1].lstrip("@")
    return ""


def _normalize_social_value(value: str, platform: str) -> str:
    if not value:
        return ""
    value = value.strip().split("?")[0].strip("/")
    if value.startswith(("http://", "https://")):
        extractor = {
            "twitter": _extract_twitter,
            "instagram": _extract_instagram,
            "youtube": _extract_youtube,
        }[platform]
        return extractor(value)
    return value.lstrip("@")


def _build_social_url(handle: str, platform: str) -> str:
    if not handle:
        return ""
    if handle.startswith(("http://", "https://")):
        return handle
    handle = handle.lstrip("@")
    if platform == "twitter":
        return f"https://x.com/{handle}"
    if platform == "instagram":
        return f"https://www.instagram.com/{handle}"
    if platform == "youtube":
        return f"https://www.youtube.com/@{handle}"
    return ""


# ---------------------------------------------------------------------------
# URL / domain utilities
# ---------------------------------------------------------------------------

def _is_google_business(lower_url: str) -> bool:
    return (
        "maps.google." in lower_url
        or "google.com/maps" in lower_url
        or "g.page/" in lower_url
    )


def _looks_like_official_website(lower_url: str) -> bool:
    parsed = urlparse(lower_url)
    host = parsed.netloc.lower().removeprefix("www.")
    if not host or any(domain in host for domain in EXCLUDED_WEBSITE_DOMAINS):
        return False
    if parsed.path.lower().startswith(("/search", "/url", "/maps")):
        return False
    return "." in host


def _website_match_score(url: str, company_name: str) -> int:
    if not url:
        return 10_000

    slug = re.sub(r"[^a-z0-9]+", "", company_name.lower())
    tokens = [
        token
        for token in re.split(r"[^a-z0-9]+", company_name.lower())
        if len(token) > 2
    ]
    parsed = urlparse(url.lower())
    host = parsed.netloc.removeprefix("www.")
    domain_core = host.split(".")[0]
    score = 100

    if slug and domain_core == slug:
        score -= 80
    elif slug and slug in host:
        score -= 60
    elif tokens and all(token in host for token in tokens):
        score -= 35
    else:
        score += 70

    if parsed.scheme == "https":
        score -= 5
    return score


def _extract_urls(html: str, base_url: str) -> List[str]:
    urls: Set[str] = set()
    soup = BeautifulSoup(html or "", "html.parser")

    for tag in soup.find_all(["a", "link"], href=True):
        candidate = _clean_candidate_url(tag.get("href", ""), base_url)
        if candidate:
            urls.add(candidate)

    for tag in soup.find_all(["meta"], content=True):
        candidate = _clean_candidate_url(tag.get("content", ""), base_url)
        if candidate:
            urls.add(candidate)

    for raw in re.findall(r"https?://[^\s'\"<>)(]+", html or ""):
        candidate = _clean_candidate_url(raw, base_url)
        if candidate:
            urls.add(candidate)

    return sorted(urls, key=_url_rank)


def _clean_candidate_url(raw: str, base_url: str) -> str:
    if not raw:
        return ""

    raw = raw.strip()
    if base_url and raw.startswith(("/", "./", "../")):
        raw = urljoin(base_url, raw)

    if raw.startswith("//"):
        raw = f"https:{raw}"

    parsed = urlparse(raw)
    query = parse_qs(parsed.query)
    redirect_value = ""
    for key in ("q", "url", "u", "uddg"):
        if query.get(key):
            redirect_value = query[key][0]
            break
    if redirect_value.startswith("http"):
        raw = redirect_value

    raw = unquote(raw).strip()
    return _normalize_candidate_url(raw)


def _normalize_candidate_url(raw: str) -> str:
    raw = (raw or "").strip().rstrip("/")
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    if not parsed.netloc:
        return ""
    return parsed._replace(fragment="").geturl().rstrip("/")


def _root_url(url: str) -> str:
    parsed = urlparse(_normalize_candidate_url(url))
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


async def _extract_website_links_plain(
    client: httpx.AsyncClient, website: str
) -> List[str]:
    """Alias kept for backward-compat; delegates to the httpx version."""
    return await _extract_website_links(client, website)


async def _guess_website(
    client: httpx.AsyncClient, company_name: str, preferred_tld: str = ""
) -> str:
    candidates = [
        WebsiteCandidate(url, "guess", idx)
        for idx, url in enumerate(_domain_guesses(company_name, preferred_tld))
    ]
    evaluation = await _choose_best_candidate(
        client, candidates, company_name, preferred_tld, False
    )
    return evaluation.url if evaluation else ""


def _domain_guesses(company_name: str, preferred_tld: str = "") -> List[str]:
    slug = re.sub(r"[^a-z0-9]+", "", company_name.lower())
    if not slug:
        return []

    # Preferred TLD first, then .com, then others
    tlds: List[str] = []
    if preferred_tld:
        tlds.append(preferred_tld)
        if preferred_tld == ".in":
            tlds.append(".co.in")
    tlds.extend([".com", ".in", ".co", ".net", ".org"])

    candidates: List[str] = []
    for tld in dict.fromkeys(tlds):
        candidates.append(f"https://www.{slug}{tld}")
        candidates.append(f"https://{slug}{tld}")
    return candidates


def _preferred_tld(query: str) -> str:
    normalized = f" {re.sub(r'[^a-z0-9]+', ' ', query.lower())} "
    for phrase, tld in COUNTRY_TLDS.items():
        if f" {phrase} " in normalized:
            return tld
    return ""


def _company_core(query: str) -> str:
    if _direct_url(query):
        host = urlparse(_direct_url(query)).netloc.lower().removeprefix("www.")
        return host.split(".")[0]

    words = [word for word in re.split(r"[^a-z0-9]+", query.lower()) if word]
    meaningful = [
        word for word in words
        if word not in COUNTRY_WORDS and word not in NOISE_WORDS
    ]
    return " ".join(meaningful) or query.strip()


def _direct_url(query: str) -> str:
    value = query.strip()
    if not value or " " in value:
        return ""
    if value.startswith(("http://", "https://")):
        return _normalize_candidate_url(value)
    if "." in value and not value.startswith("@"):
        return _normalize_candidate_url(value)
    return ""


def _host_matches_tld(url: str, tld: str) -> bool:
    if not tld:
        return False
    host = urlparse(url.lower()).netloc.removeprefix("www.")
    if host.endswith(tld.lower()):
        return True
    return tld == ".in" and host.endswith(".co.in")


def _candidate_matches_company(url: str, company_name: str) -> bool:
    """Return True if the candidate URL plausibly belongs to company_name."""
    slug = re.sub(r"[^a-z0-9]+", "", company_name.lower())
    tokens = [
        token
        for token in re.split(r"[^a-z0-9]+", company_name.lower())
        if len(token) > 2
    ]
    host = urlparse(url.lower()).netloc.removeprefix("www.")
    domain_core = host.split(".")[0]

    # Full slug match
    if slug and (slug in host or slug in domain_core):
        return True
    # All meaningful tokens present in host
    if tokens and all(token in host for token in tokens):
        return True
    # Partial: most tokens present (≥ 60 % of tokens, min 1)
    if tokens:
        matched = sum(1 for t in tokens if t in host)
        if matched >= max(1, len(tokens) * 0.6):
            return True
    return False
