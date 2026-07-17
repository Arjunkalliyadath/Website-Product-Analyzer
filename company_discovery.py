"""
Module Name
    company_discovery.py

Purpose
    Extracts lightweight brand/company metadata directly from a website's
    homepage HTML. The website URL is supplied by the caller as the single
    source of truth; this module does not search for, guess, or validate
    candidate domains.

Responsibilities
    - Fetch a given URL's homepage over plain HTTP, with a single
      Playwright browser fallback for blocked or otherwise unreachable
      responses.
    - Parse the resulting HTML to extract the company name, logo/favicon,
      resolved website URL, and social profile links (Twitter/X,
      Instagram, YouTube, Facebook, LinkedIn).
    - Return a fixed-shape dict compatible with the legacy
      discover_company() output, so downstream consumers (product
      discovery, scrapers, dashboard) require no changes.

Architecture
    A single async entry point, extract_website_metadata(), orchestrates
    a fetch stage followed by independent, stateless extraction helpers
    that each operate on the same parsed BeautifulSoup document. The
    module never raises: any fetch or parse failure yields the same
    empty-field result shape produced by _empty_result().

Discovery Flow
    1. Fetch the homepage via plain HTTP (httpx). A blocked status code
       (403/429/500/502/503/504) or a network-level failure skips any
       HTTP retry and makes exactly one follow-up attempt via a headless
       Playwright browser instead.
    2. Parse the returned HTML with BeautifulSoup.
    3. Extract the company name from og:site_name, then
       application-name, then the page <title>, falling back to a name
       derived from the URL itself.
    4. Extract a logo/favicon URL from <link rel="icon"...> tags, then
       og:image, then the conventional /favicon.ico path.
    5. Scan every <a href> and relevant <meta> tag for known social
       domains, extract one handle per platform, and rebuild a canonical
       profile URL for each.

Inputs
    url: str — a website URL, typically already validated/normalized by
    the caller (see url_utils.is_url / normalize_url).

Outputs
    Dict[str, object] with keys: company_name, website, logo, twitter,
    instagram, youtube, facebook, linkedin, twitter_url, instagram_url,
    youtube_url, facebook_url, linkedin_url, google_business,
    discovery_version, website_type, website_verified,
    website_confidence, discovery_notes.

Dependencies
    httpx and BeautifulSoup for the HTTP/HTML pipeline; Playwright
    (imported lazily) for the blocked-homepage fallback; url_utils for
    company-name derivation from a bare URL.
"""

import logging
from typing import Dict, List
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

_BLOCKED_STATUS_CODES = {403, 429, 500, 502, 503, 504}

_PW_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
]

_PW_NAV_TIMEOUT_MS = 15000

_SOCIAL_URL_TEMPLATES = {
    "linkedin": "https://www.linkedin.com/{}",
    "twitter": "https://x.com/{}",
    "instagram": "https://www.instagram.com/{}",
    "youtube": "https://www.youtube.com/@{}",
    "facebook": "https://www.facebook.com/{}",
}


async def extract_website_metadata(url: str) -> Dict[str, object]:
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
        logger.info("Website metadata: %s - using Playwright fallback.", url)
        pw_html, pw_url = await _fetch_homepage_playwright(url)
        if pw_html:
            return pw_html, pw_url
        return "", url


async def _fetch_homepage_playwright(url: str) -> "tuple[str, str]":
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

    return _resolve_url("/favicon.ico", base_url)


def _extract_social_handles(soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
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
    template = _SOCIAL_URL_TEMPLATES.get(platform)
    return template.format(handle) if template else ""


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
        "website_type": "unknown",
        "website_verified": False,
        "website_confidence": "low",
        "discovery_notes": [],
    }
