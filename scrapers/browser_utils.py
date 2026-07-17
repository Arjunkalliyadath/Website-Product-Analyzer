import asyncio
import concurrent.futures
import re
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Awaitable, Callable, Iterator, List, TypeVar
from urllib.parse import quote_plus, unquote

import httpx

# Generic type variable used for preserving return types of async functions
_T = TypeVar("_T")


# ===========================================================================
# Browser Launch Throttling
# ===========================================================================

# Maximum number of Chromium browsers that can launch simultaneously.
# This prevents multiple browser launches from consuming excessive CPU
# during application startup.
MAX_CONCURRENT_BROWSER_LAUNCHES = 2

# Shared semaphore used to control concurrent browser launches.
_browser_launch_semaphore = threading.Semaphore(MAX_CONCURRENT_BROWSER_LAUNCHES)


@contextmanager
def browser_launch_slot() -> Iterator[None]:
    """
    Context manager that reserves a browser launch slot.

    Only limits the launch process. Once the browser is launched,
    the slot is released immediately.
    """
    _browser_launch_semaphore.acquire()
    try:
        yield
    finally:
        _browser_launch_semaphore.release()


# ===========================================================================
# Storage Directory
# ===========================================================================

def get_storage_dir() -> Path:
    """
    Creates (if necessary) and returns the Playwright storage directory.
    """
    path = Path("downloads") / ".playwright"
    path.mkdir(parents=True, exist_ok=True)
    return path


# ===========================================================================
# Comment Cleaning
# ===========================================================================

def normalize_comments(items: List[str]) -> List[str]:
    """
    Cleans a list of comments by:
    - Removing empty comments
    - Ignoring comments shorter than 3 characters
    - Removing comments containing URLs
    - Removing duplicate comments (case-insensitive)
    """
    cleaned = []
    seen = set()

    for item in items:
        text = (item or "").strip()

        # Skip empty or very short comments
        if not text or len(text) < 3:
            continue

        # Skip comments containing links
        if "http" in text.lower():
            continue

        lowered = text.lower()

        # Skip duplicate comments
        if lowered in seen:
            continue

        seen.add(lowered)
        cleaned.append(text)

    return cleaned


# ===========================================================================
# Social Profile Discovery Fallback
# ===========================================================================
# company_discovery.py finds real handles by scanning the company's own
# homepage for links to twitter.com/instagram.com/youtube.com. When that
# scan comes up empty, twitter_scraper.py / instagram_scraper.py /
# youtube_scraper.py previously fell back to slugifying the company name
# into a guessed handle (e.g. "Headphone Zone" -> "@headphone_zone"). That
# guess is frequently wrong and fails *silently* - the scraper just
# returns zero results with no indication the handle itself was bad. This
# helper replaces that guess with an actual (lightweight) search, so a
# missing homepage link no longer means "give up and guess" - it means
# "go find the real one first, and only return [] if that also comes up
# empty."
#
# Uses DuckDuckGo's plain-HTML search endpoint rather than Google: Google
# blocks bare httpx requests without JS, while DuckDuckGo's /html/ endpoint
# is designed to be scraped without a browser. This keeps the fallback
# cheap (one plain GET) instead of needing a full Playwright page just to
# resolve a handle.

_SEARCH_TIMEOUT_SECONDS = 4.0

_PLATFORM_SEARCH_DOMAIN = {
    "twitter": "x.com",
    "instagram": "instagram.com",
    "youtube": "youtube.com",
}

_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}


def find_social_profile_url(company_name: str, platform: str) -> str:
    """Best-effort discovery of a company's real profile URL on
    ``platform`` via a plain-HTML web search. Intended as a fallback ONLY
    when the company's own homepage didn't link to that platform.

    Returns "" on any failure (network error, no match, unsupported
    platform) - never raises. Callers should treat "" exactly like "not
    found", not like an error worth logging loudly.
    """
    company_name = (company_name or "").strip()
    domain = _PLATFORM_SEARCH_DOMAIN.get(platform)
    if not company_name or not domain:
        return ""

    query = f'site:{domain} "{company_name}"'
    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"

    try:
        with httpx.Client(follow_redirects=True, headers=_SEARCH_HEADERS) as client:
            resp = client.get(search_url, timeout=_SEARCH_TIMEOUT_SECONDS)
        if resp.status_code >= 400:
            return ""

        # DuckDuckGo's HTML results wrap outbound links in a redirect
        # (//duckduckgo.com/l/?uddg=<url-encoded target>&...); pull the
        # first one that actually points at the target platform.
        for raw in re.findall(
            r'href="(https?://duckduckgo\.com/l/\?uddg=[^"]+)"', resp.text
        ):
            target = unquote(raw.split("uddg=", 1)[1].split("&", 1)[0])
            if domain in target.lower():
                return target

        # Fallback: some result rows link straight to the platform
        # without going through the redirect wrapper.
        for raw in re.findall(
            r'href="(https?://[^"]*' + re.escape(domain) + r'[^"]*)"', resp.text
        ):
            return unquote(raw)
    except Exception:
        return ""

    return ""


# ===========================================================================
# Playwright Execution
# ===========================================================================

def _run_playwright_in_fresh_loop(coro_factory: Callable[[], Awaitable[_T]]) -> _T:
    """
    Runs a Playwright coroutine inside a brand-new event loop.

    This avoids conflicts when running Playwright from worker threads,
    especially on Windows.
    """

    # Use Windows-compatible event loop policy if required
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Create and set a fresh event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(coro_factory())
    finally:
        loop.close()


async def run_playwright_async(coro_factory: Callable[[], Awaitable[_T]]) -> _T:
    """
    Executes a Playwright coroutine inside a separate worker thread.

    This keeps the main asyncio event loop responsive while Playwright
    performs browser automation.
    """

    # Get the current asyncio event loop
    loop = asyncio.get_event_loop()

    # Run Playwright in a dedicated thread
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(
            pool,
            _run_playwright_in_fresh_loop,
            coro_factory
        )