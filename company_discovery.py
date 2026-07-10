"""
company_discovery.py — Manobhava-AI Website Product Analyzer
==============================================================

PHASE 1 REFACTOR: Website Metadata Extractor
----------------------------------------------
This module no longer "discovers" a company's website. The application now
receives a website URL directly (see url_utils.is_url / normalize_url) and
uses THAT as the single source of truth.

The only remaining job of this module is:

    await extract_website_metadata(url: str) -> Dict[str, object]

    Fetch the supplied homepage and pull lightweight metadata out of the
    HTML that's already there:
        - Company Name
        - Logo / favicon
        - Website (the resolved/final URL)
        - Instagram / Twitter (X) / YouTube / Facebook / LinkedIn URLs

This module deliberately does NOT:
    - perform a Google/search-engine query
    - guess or brute-force candidate domains
    - call any AI/Gemini lookup
    - validate/score whether a domain is "the real brand site"

Everything returned comes directly from the one homepage HTML response.
Social links are located by scanning every <a> tag (which inherently
covers header, footer, and nav markup, since they're all part of the same
DOM) and relevant <meta> tags, then matched against known social domains.

--------------------------------------------------------------------------
PLAYWRIGHT FALLBACK (added)
--------------------------------------------------------------------------
Some homepages (e.g. ConceptKart) return a blocked status code -
403 / 429 / 500 / 502 / 503 / 504 - to a plain httpx request even though a
real browser loads them fine. When that happens (or the plain request
fails outright - timeout, connection reset, TLS handshake failure), this
module makes exactly ONE follow-up attempt via a headless Playwright
browser instead of retrying the same blocked HTTP request. No HTTP retries
are attempted first - a blocked status code skips straight to Playwright.
If Playwright is unavailable or also fails, the function still returns its
normal empty-field result rather than raising.

--------------------------------------------------------------------------
BACKWARD COMPATIBILITY
--------------------------------------------------------------------------
The returned dict keeps the same keys the old discover_company() produced
(company_name, website, twitter, instagram, youtube, twitter_url,
instagram_url, youtube_url, google_business, discovery_version, facebook,
linkedin, facebook_url, linkedin_url, website_type, website_verified,
website_confidence, discovery_notes) so the rest of the pipeline
(Product Discovery, scrapers, dashboard) needs zero changes. One new
field, `logo`, has been added additively.

This function never raises. A missing/unreachable homepage or a missing
social link simply results in empty strings — never an exception.
"""

import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from url_utils import derive_company_name

logger = logging.getLogger(__name__)

DISCOVERY_VERSION = "website-metadata-extractor-v1"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Homepage fetch is bounded so a slow/unresponsive site can never block
# the pipeline for long — same spirit as the hard per-stage timeouts
# already used elsewhere in the app (app.py's _timed()/wait_for calls).
_FETCH_TIMEOUT = httpx.Timeout(12.0, connect=6.0)

_SOCIAL_DOMAIN_MAP = {
    "twitter.com": "twitter", "x.com": "twitter",
    "instagram.com": "instagram",
    "youtube.com": "youtube", "youtu.be": "youtube",
    "facebook.com": "facebook", "fb.com": "facebook",
    "linkedin.com": "linkedin",
}

_BLOCKED_HANDLES = {
    "twitter":   {"home", "intent", "i", "share", "search", "hashtag", "explore", "settings"},
    "instagram": {"p", "reel", "stories", "explore", "accounts", "direct"},
}

_ICON_RELS = (
    "icon", "shortcut icon", "apple-touch-icon",
    "apple-touch-icon-precomposed", "mask-icon",
)

# HTTP status codes that mean "this homepage is bot-protected / erroring at
# the edge, not actually missing" - a plain httpx request never has a
# realistic shot at these, so we skip straight to Playwright instead of
# retrying the same blocked request over plain HTTP.
_BLOCKED_STATUS_CODES = {403, 429, 500, 502, 503, 504}

_PW_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
]

# Website metadata extraction is a one-shot call per analysis (not a
# high-frequency scraper), so the Playwright fallback launches and tears
# down its own browser rather than keeping a worker pool alive - contrast
# with google/twitter/instagram/youtube scrapers, which reuse a pool of
# browser contexts across many calls.
_PW_NAV_TIMEOUT_MS = 15000


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------

async def extract_website_metadata(url: str) -> Dict[str, object]:
    """
    Fetch `url`'s homepage and extract lightweight brand metadata directly
    from the HTML. Never raises — worst case, returns mostly-empty fields.
    """
    result = _empty_result(url)
    if not url:
        return result

    html, final_url = await _fetch_homepage(url)
    if not html:
        return result

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:
        logger.warning("Website metadata: failed to parse HTML for %s: %s", url, exc)
        return result

    base_url = final_url or url
    result["website"] = base_url
    result["company_name"] = _extract_company_name(soup, base_url)
    result["logo"] = _extract_logo(soup, base_url)

    socials = _extract_social_handles(soup, base_url)
    for platform, handle in socials.items():
        result[platform] = handle
        result[f"{platform}_url"] = _build_social_url(handle, platform)

    logger.info(
        "Website metadata extracted for %s: name=%r logo=%r twitter=%r "
        "instagram=%r youtube=%r facebook=%r linkedin=%r",
        url, result["company_name"], result["logo"], result["twitter"],
        result["instagram"], result["youtube"], result["facebook"], result["linkedin"],
    )
    return result


# --------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------

async def _fetch_homepage(url: str) -> "tuple[str, str]":
    try:
        async with httpx.AsyncClient(
            timeout=_FETCH_TIMEOUT,
            follow_redirects=True,
            verify=False,
            headers=_HEADERS,
        ) as client:
            response = await client.get(url)
            if response.status_code >= 400:
                final_url = str(response.url) or url
                if response.status_code in _BLOCKED_STATUS_CODES:
                    # Bot-protected / erroring at the edge - a plain HTTP
                    # retry has no realistic shot at succeeding here, so we
                    # skip HTTP entirely (zero retries) and go straight to
                    # a real browser instead.
                    logger.info(
                        "Website metadata: %s returned HTTP %s (bot-protected) "
                        "- skipping HTTP retries. Using Playwright fallback.",
                        url, response.status_code,
                    )
                    pw_html, pw_url = await _fetch_homepage_playwright(url)
                    if pw_html:
                        return pw_html, pw_url
                else:
                    logger.info(
                        "Website metadata: %s returned HTTP %s", url, response.status_code
                    )
                return "", final_url
            return response.text, str(response.url) or url
    except Exception as exc:
        logger.warning("Website metadata: fetch failed for %s: %s", url, exc)
        # A network-level failure (timeout, connection reset, TLS handshake
        # failure, etc.) is also worth one Playwright attempt - a real
        # browser handles TLS/anti-bot fingerprinting differently than a
        # plain httpx client and can succeed where the plain request could
        # not, without ever retrying the same plain HTTP request itself.
        logger.info("Website metadata: %s - using Playwright fallback.", url)
        pw_html, pw_url = await _fetch_homepage_playwright(url)
        if pw_html:
            return pw_html, pw_url
        return "", url


async def _fetch_homepage_playwright(url: str) -> "tuple[str, str]":
    """Last-resort homepage fetch via a real headless browser.

    Only called when a plain HTTP request came back blocked (403 / 429 /
    500 / 502 / 503 / 504) or failed outright. Never raises - any failure
    here just means the caller falls back to the normal empty-field
    result, exactly as if this function didn't exist.
    """
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        logger.warning(
            "Website metadata: Playwright unavailable (%s); cannot use "
            "the browser fallback for %s.", exc, url,
        )
        return "", ""

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=_PW_LAUNCH_ARGS)
            try:
                context = await browser.new_context(
                    user_agent=_HEADERS["User-Agent"],
                    locale="en-US",
                    viewport={"width": 1280, "height": 900},
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=_PW_NAV_TIMEOUT_MS)
                    await page.wait_for_timeout(1000)
                    html = await page.content()
                    final_url = page.url or url
                    logger.info(
                        "Website metadata: Playwright fallback succeeded for %s.", url
                    )
                    return html, final_url
                finally:
                    await page.close()
                    await context.close()
            finally:
                await browser.close()
    except Exception as exc:
        logger.warning("Website metadata: Playwright fallback failed for %s: %s", url, exc)
        return "", ""


# --------------------------------------------------------------------------
# Extraction helpers
# --------------------------------------------------------------------------

def _extract_company_name(soup: BeautifulSoup, base_url: str) -> str:
    og_site = soup.find("meta", attrs={"property": "og:site_name"})
    if og_site and og_site.get("content", "").strip():
        return og_site["content"].strip()

    app_name = soup.find("meta", attrs={"name": "application-name"})
    if app_name and app_name.get("content", "").strip():
        return app_name["content"].strip()

    if soup.title and soup.title.string and soup.title.string.strip():
        title = soup.title.string.strip()
        for sep in (" | ", " – ", " — ", " - ", " :: "):
            if sep in title:
                candidate = title.split(sep)[0].strip()
                if candidate:
                    return candidate
        return title

    return derive_company_name(base_url)


def _extract_logo(soup: BeautifulSoup, base_url: str) -> str:
    for link in soup.find_all("link", href=True):
        rel_attr = link.get("rel", "")
        rel = " ".join(rel_attr).lower() if isinstance(rel_attr, list) else str(rel_attr).lower()
        if any(icon_rel in rel for icon_rel in _ICON_RELS):
            resolved = _resolve_url(link["href"], base_url)
            if resolved:
                return resolved

    og_image = soup.find("meta", attrs={"property": "og:image"})
    if og_image and og_image.get("content", "").strip():
        resolved = _resolve_url(og_image["content"].strip(), base_url)
        if resolved:
            return resolved

    # Last-resort convention: most sites serve a favicon here even with no
    # explicit <link>.
    return _resolve_url("/favicon.ico", base_url)


def _extract_social_handles(soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
    """Scan anchor + meta tags (covers header/footer/nav/body alike) for
    official social links. Returns one handle per platform at most; missing
    platforms are simply absent (caller defaults them to "")."""
    found: Dict[str, str] = {}

    raw_candidates: List[str] = [tag["href"] for tag in soup.find_all("a", href=True)]
    for tag in soup.find_all("meta", content=True):
        prop = (tag.get("property") or tag.get("name") or "").lower()
        if prop.startswith("og:") or "social" in prop or "same_as" in prop:
            raw_candidates.append(tag["content"])

    for raw in raw_candidates:
        resolved = _resolve_url(raw, base_url)
        if not resolved:
            continue
        host = urlparse(resolved.lower()).netloc
        if host.startswith("www."):
            host = host[4:]
        platform = next(
            (p for domain, p in _SOCIAL_DOMAIN_MAP.items() if domain in host), None
        )
        if not platform or platform in found:
            continue
        handle = _extract_handle(resolved, platform)
        if handle:
            found[platform] = handle

    return found


def _extract_handle(url: str, platform: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return ""

    if platform == "youtube":
        if "youtu.be" in parsed.netloc.lower():
            return ""
        if parts[0].startswith("@"):
            return parts[0].lstrip("@")
        if parts[0].lower() in {"channel", "user", "c"} and len(parts) > 1:
            return parts[1].lstrip("@")
        return ""

    if platform == "linkedin":
        if parts[0].lower() == "company" and len(parts) > 1:
            return f"company/{parts[1]}"
        if parts[0].lower() == "school" and len(parts) > 1:
            return f"school/{parts[1]}"
        return ""

    blocked = _BLOCKED_HANDLES.get(platform, set())
    handle = parts[0].lstrip("@")
    return "" if handle.lower() in blocked else handle


def _build_social_url(handle: str, platform: str) -> str:
    if not handle:
        return ""
    if platform == "linkedin":
        return f"https://www.linkedin.com/{handle}"
    if platform == "twitter":
        return f"https://x.com/{handle}"
    if platform == "instagram":
        return f"https://www.instagram.com/{handle}"
    if platform == "youtube":
        return f"https://www.youtube.com/@{handle}"
    if platform == "facebook":
        return f"https://www.facebook.com/{handle}"
    return ""


def _resolve_url(raw: str, base_url: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith(("/", "./", "../")):
        raw = urljoin(base_url, raw)
    if not raw.startswith(("http://", "https://")):
        return ""
    return raw


def _empty_result(url: str) -> Dict[str, object]:
    return {
        "company_name": "",
        "website": url or "",
        "logo": "",
        "twitter": "",
        "instagram": "",
        "youtube": "",
        "facebook": "",
        "linkedin": "",
        "twitter_url": "",
        "instagram_url": "",
        "youtube_url": "",
        "facebook_url": "",
        "linkedin_url": "",
        "google_business": "",
        "discovery_version": DISCOVERY_VERSION,
        # Kept for backward compatibility with older callers/templates.
        # This module no longer classifies/validates websites, so these
        # are fixed, honest defaults rather than discovery-pipeline output.
        "website_type": "unknown",
        "website_verified": False,
        "website_confidence": "low",
        "discovery_notes": [],
    }
