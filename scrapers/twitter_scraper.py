import asyncio
import atexit
import concurrent.futures
import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List

import httpx

from scrapers.browser_utils import browser_launch_slot, normalize_comments, find_social_profile_url
from config import (
    MAX_TWITTER_POSTS,
    MAX_SCROLL_ITERATIONS,
    SCROLL_IDLE_LIMIT,
    NAVIGATION_RETRIES,
    NAV_TIMEOUT_MS,
    NAV_TIMEOUT_MS_MAX,
    NAV_TIMEOUT_MS_MIN,
)

logger = logging.getLogger(__name__)

# --- Hard internal time budget -----------------------------------------
# Kept a few seconds under the outer asyncio.wait_for() cap applied in
# app.py (26s, widened from 20s - the old value was below observed
# setup_time (~7.4s) + this budget (12s), leaving only ~0.6s of margin
# before debug-artifact/screenshot capture on a failure path could push
# the outer timeout past its own cap) so this scraper almost always
# returns on its own, with whatever it has collected so far, instead of
# being cut off cold by the outer timeout and losing partial results.
# Lowered from 16 -> 12 now that
# navigation itself fails fast (NAV_TIMEOUT_MS, NAVIGATION_RETRIES in
# config.py), so a blocked/login-walled profile is recognized in a few
# seconds instead of eating the whole budget on retries.
TIME_BUDGET_SECONDS = 16

# Overall cap on how many tweets/replies this scraper will try to collect
# for one company/handle, sourced from config so it can be tuned without
# touching this file.
MAX_TOTAL_POSTS = MAX_TWITTER_POSTS

MAX_REPLIES_PER_TWEET = 20

_LOGIN_MARKERS = (
    "/login", "/i/flow/login", "/account/access", "/i/flow/", "/logout",
    "/error",
)

# Text-based signals that the page is a login wall even when the URL itself
# didn't change (X sometimes renders an in-page "Sign in" prompt on top of
# a timeline that never redirected).
_LOGIN_TEXT_MARKERS = (
    "sign in to x", "log in to x", "don't miss what's happening",
)

# Additional text-based signals used only for diagnostics (never to change
# control flow beyond "give up and return gracefully"). These let logs say
# *why* nothing was collected instead of a generic empty result, which is
# the difference between "the scraper is broken" and "this profile is
# suspended" when someone is debugging a run later.
_SUSPENDED_MARKERS = ("account suspended",)
_NOT_FOUND_MARKERS = (
    "this account doesn't exist", "this account doesn’t exist",
    "page doesn't exist", "page doesn’t exist", "hmm...this page doesn",
)
_PROTECTED_MARKERS = ("these posts are protected", "these tweets are protected")
_RATE_LIMIT_MARKERS = ("rate limit exceeded", "try again later", "something went wrong")

# Text of publicly-visible "expand" controls X shows on tweet detail pages.
# Clicking these (when present) renders replies that are collapsed by
# default; anything gated behind an actual login wall is left alone.
_EXPAND_BUTTON_TEXTS = ("Show more replies", "Show replies", "Continue thread", "Show additional replies")

# Markers used to recognize and skip pinned/promoted posts so they don't
# pollute the sentiment sample with non-organic or repeated content.
_NOISE_MARKERS = ("promoted", "pinned")

NAV_RETRY_BACKOFF_SECONDS = 0.75

# Text scraping never needs images or fonts; skipping them cuts page load
# time without affecting anything we read out of the DOM.
_BLOCKED_RESOURCE_TYPES = {"image", "font"}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_LAUNCH_ARGS = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]

# --- Browser pool -----------------------------------------------------------
# One Playwright browser + context per worker thread, kept alive across
# calls instead of being relaunched every time. This avoids paying
# Chromium process-startup cost on every single request and lets cookies
# persist between calls on the same worker thread.
MAX_BROWSER_WORKERS = 3
MAX_USES_BEFORE_RECYCLE = 50

_thread_local = threading.local()
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_BROWSER_WORKERS, thread_name_prefix="twitter_scraper"
)


class _BrowserHandle:
    """Holds one worker thread's Playwright/browser/context.

    Registered both in that thread's ``_thread_local`` (for reuse by the
    owning thread) and in the module-level ``_worker_handles`` list (so
    process-exit cleanup can reach it without running code back on the
    executor's worker threads).
    """

    __slots__ = ("playwright", "browser", "context", "uses")

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.uses = 0


_worker_handles: List["_BrowserHandle"] = []
_worker_handles_lock = threading.Lock()


def _get_handle() -> "_BrowserHandle":
    handle = getattr(_thread_local, "handle", None)
    if handle is None:
        handle = _BrowserHandle()
        _thread_local.handle = handle
        with _worker_handles_lock:
            _worker_handles.append(handle)
    return handle


def _install_resource_blocking(context) -> None:
    def _route_handler(route):
        try:
            if route.request.resource_type in _BLOCKED_RESOURCE_TYPES:
                route.abort()
                return
        except Exception:
            pass
        try:
            route.continue_()
        except Exception:
            pass

    try:
        context.route("**/*", _route_handler)
    except Exception:
        logger.exception("Failed to install resource blocking; continuing without it.")


def _teardown_thread_browser() -> None:
    handle = _get_handle()
    try:
        if handle.context is not None:
            handle.context.close()
    except Exception:
        pass
    try:
        if handle.browser is not None:
            handle.browser.close()
    except Exception:
        pass
    handle.context = None
    handle.browser = None
    handle.uses = 0


def _close_handle_with_timeout(handle: "_BrowserHandle", timeout: float = 5.0) -> None:
    """Close one worker's Playwright objects, bounded by a short timeout.

    Runs on a fresh, throwaway ``threading.Thread`` rather than by
    submitting a job to ``_EXECUTOR``. Submitting to the same executor
    we're trying to shut down is what previously caused ``RuntimeError:
    cannot schedule new futures after shutdown`` — Python's interpreter
    shutdown sequence can mark the executor (or the process-wide threading
    state) as shut down before our atexit callback runs, and any
    ``executor.submit()`` after that point raises. A plain, unpooled thread
    has no such shutdown flag to race against.
    """

    def _job():
        try:
            if handle.context is not None:
                handle.context.close()
        except Exception:
            pass
        try:
            if handle.browser is not None:
                handle.browser.close()
        except Exception:
            pass
        try:
            if handle.playwright is not None:
                handle.playwright.stop()
        except Exception:
            pass
        handle.context = None
        handle.browser = None
        handle.playwright = None

    t = threading.Thread(target=_job, daemon=True)
    t.start()
    t.join(timeout=timeout)


def _shutdown_all_workers() -> None:
    """Best-effort cleanup of every worker's browser at process exit.

    Iterates the ``_worker_handles`` registry directly instead of
    submitting cleanup jobs back onto ``_EXECUTOR`` — see
    ``_close_handle_with_timeout`` for why that was unsafe.
    """
    with _worker_handles_lock:
        handles = list(_worker_handles)
    for handle in handles:
        _close_handle_with_timeout(handle)


atexit.register(_shutdown_all_workers)


def _ensure_context():
    """Get (creating or recycling as needed) this thread's browser context."""
    handle = _get_handle()
    ctx = handle.context

    if ctx is not None:
        if handle.uses >= MAX_USES_BEFORE_RECYCLE:
            logger.info(
                "Recycling browser context on %s after %d uses.",
                threading.current_thread().name, handle.uses,
            )
            _teardown_thread_browser()
            ctx = None
        else:
            try:
                _ = ctx.pages  # cheap liveness check
                handle.uses += 1
                return ctx
            except Exception:
                logger.warning(
                    "Browser context on %s appears dead; recreating.",
                    threading.current_thread().name,
                )
                _teardown_thread_browser()
                ctx = None

    from playwright.sync_api import sync_playwright

    pw = handle.playwright
    if pw is None:
        pw = sync_playwright().start()
        handle.playwright = pw

    with browser_launch_slot():
        browser = pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
    context = browser.new_context(
        user_agent=_USER_AGENT,
        viewport={"width": 1280, "height": 1800},
        locale="en-US",
    )
    context.set_default_timeout(8000)
    context.set_default_navigation_timeout(10000)
    _install_resource_blocking(context)

    handle.browser = browser
    handle.context = context
    handle.uses = 1
    logger.info("Launched a new browser context on %s.", threading.current_thread().name)
    return context


def _profile_url(target: str) -> str:
    """Resolve ``target`` (already a full URL, an "@handle", or a bare
    company name) into a usable x.com profile URL.

    Previously this only ever did ``target.lstrip('@')`` and slapped the
    result onto ``https://x.com/``, so anything that wasn't already a
    clean handle - a company name with spaces, punctuation, a
    twitter.com link, a URL with tracking query params - produced an
    invalid or 404-prone URL before navigation ever got a chance to run.
    This is still a heuristic (there's no public search API to confirm
    the real handle without authentication), but it resolves the common
    shapes correctly instead of only the already-correct one.
    """
    target = (target or "").strip()
    if not target:
        return ""

    if target.startswith(("http://", "https://")):
        # Normalize the legacy twitter.com domain to x.com and strip
        # anything (query string, fragment, trailing slash) that isn't
        # part of the actual profile path.
        normalized = target.replace("twitter.com", "x.com")
        normalized = normalized.split("?")[0].split("#")[0]
        return normalized.rstrip("/")

    handle = target.lstrip("@").strip()
    # A valid X handle is letters/digits/underscore only. A bare company
    # name ("Acme Corp", "Acme, Inc.") fails that check, so collapse it to
    # the closest handle-safe candidate instead of building a URL that's
    # guaranteed to 404. This won't always land on the *correct* handle,
    # but it stops obviously-broken URLs (with spaces/punctuation) from
    # ever being requested.
    if not re.fullmatch(r"[A-Za-z0-9_]+", handle):
        handle = re.sub(r"[^A-Za-z0-9_]", "", handle.replace(" ", ""))
    return f"https://x.com/{handle}" if handle else ""


# --- Browser-free preflight ------------------------------------------------
# X/Twitter is optional per the pipeline's own priority order, and the log
# shows every single attempt in this run either failing to navigate at all
# (browser-launch contention ate the time budget before Chromium could even
# open the page) or landing on a login wall once it did load. Since
# _looks_blocked's own URL/text markers work just as well against a plain
# HTTP response as a rendered page, checking them BEFORE ever calling
# _ensure_context() means an obviously-blocked target never consumes one of
# the shared browser-launch slots (browser_launch_slot()) at all - freeing
# that capacity for Google/YouTube instead, without touching
# MAX_CONCURRENT_BROWSER_LAUNCHES or any timeout constant.
_PREFLIGHT_TIMEOUT_SECONDS = 4.0


def _httpx_preflight_blocked(url: str) -> bool:
    """One plain GET, no retry: True if the response is already an
    unambiguous login-wall/redirect, False if it looks navigable (or if
    the check itself is inconclusive - in which case the existing
    Playwright path decides, unchanged)."""
    try:
        with httpx.Client(follow_redirects=True) as client:
            resp = client.get(
                url,
                timeout=_PREFLIGHT_TIMEOUT_SECONDS,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                    ),
                },
            )
    except Exception:
        return False  # inconclusive - let the browser path try properly

    final_url = str(resp.url)
    if any(marker in final_url for marker in _LOGIN_MARKERS):
        return True
    body_sample = resp.text[:20000].lower()
    return any(marker in body_sample for marker in _LOGIN_TEXT_MARKERS)


def _looks_blocked(page) -> bool:
    """Return True only for a real login redirect."""
    try:
        url = page.url or ""
    except Exception:
        return False

    return any(marker in url for marker in _LOGIN_MARKERS)


def _diagnose_empty_page(page) -> str:
    """Classify why no tweets were found, for logging only - never used
    to change control flow beyond "give up gracefully", which happens
    regardless of the reason.

    Returns one of: "login_wall", "suspended", "not_found", "protected",
    "rate_limited", "rendering_failure", "no_tweets". Bounded to a single
    short body-text read so this never adds meaningful latency on top of
    the empty-result path it runs on.
    """
    try:
        url = page.url or ""
    except Exception:
        url = ""
    if any(marker in url for marker in _LOGIN_MARKERS):
        return "login_wall"

    try:
        body_text = page.locator("body").inner_text(timeout=800).lower()
    except Exception:
        return "rendering_failure"

    if any(m in body_text for m in _LOGIN_TEXT_MARKERS):
        return "login_wall"
    if any(m in body_text for m in _SUSPENDED_MARKERS):
        return "suspended"
    if any(m in body_text for m in _NOT_FOUND_MARKERS):
        return "not_found"
    if any(m in body_text for m in _PROTECTED_MARKERS):
        return "protected"
    if any(m in body_text for m in _RATE_LIMIT_MARKERS):
        return "rate_limited"
    if not body_text.strip():
        return "rendering_failure"
    return "no_tweets"


# Evidence directory for zero-result debug captures. Lives at
# <project_root>/debug (sibling of app.py's DOWNLOADS_DIR), since this file
# is itself at <project_root>/scrapers/twitter_scraper.py.
_DEBUG_DIR = Path(__file__).resolve().parent.parent / "debug"


def _save_debug_artifacts(page, url: str, reason: str, extra: Dict[str, str] = None) -> None:
    """Save HTML/screenshot/context ONLY on failure/zero-results for
    debugging. Never on the success path.

    Writes three files under ``_DEBUG_DIR``, all sharing one base name
    stamped with date-time + milliseconds + thread id so concurrent or
    rapid-fire failures never overwrite each other:
      * <base>.png - screenshot
      * <base>.html - full page HTML
      * <base>.txt  - final URL, page title, reason, and whatever the
        caller passes in ``extra`` (e.g. login-wall / article-existence
        checks).
    """
    try:
        _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_url = re.sub(r'[^a-zA-Z0-9]', '_', url)[:50].strip('_') or "unknown"
        stamp = f"{time.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 1000:03d}_{threading.get_ident()}"
        base = _DEBUG_DIR / f"twitter_{safe_url}_{stamp}"

        try:
            final_url = page.url or ""
        except Exception:
            final_url = "<unavailable>"
        try:
            title = page.title()
        except Exception:
            title = "<unavailable>"

        try:
            page.screenshot(path=f"{base}.png", timeout=3000, full_page=True)
        except Exception:
            logger.warning("[TWITTER_DEBUG] could not capture screenshot for %s", url)

        try:
            with open(f"{base}.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            logger.warning("[TWITTER_DEBUG] could not capture HTML for %s", url)

        info = {
            "reason": reason,
            "target_url": url,
            "final_url": final_url,
            "page_title": title,
        }
        if extra:
            info.update(extra)
        try:
            with open(f"{base}.txt", "w", encoding="utf-8") as f:
                for k, v in info.items():
                    f.write(f"{k}: {v}\n")
        except Exception:
            pass

        logger.warning(
            "[TWITTER_DEBUG] saved zero-result evidence for %s reason=%s: %s.{png,html,txt}",
            url, reason, base,
        )
    except Exception:
        logger.exception("[TWITTER_DEBUG] failed to save debug artifacts for %s", url)


def _adaptive_nav_timeout(time_left) -> int:
    """Scale the navigation timeout to how much of the internal time
    budget is actually left, instead of a fixed 5000ms for every attempt.

    Navigation always receives at least NAV_TIMEOUT_MS_MIN (8s) as long as
    that much time is actually left in the budget - the budget clock only
    starts once browser/context/page setup has finished (see _run below),
    so this floor is real, not eaten by Chromium startup. When plenty of
    budget remains the timeout can scale up to NAV_TIMEOUT_MS_MAX (12s) for
    a genuinely slow page; when the budget itself is under the floor, we
    hand over whatever is left rather than blocking past the scraper's own
    deadline.
    """
    remaining_ms = max(0.0, time_left()) * 1000
    if remaining_ms <= NAV_TIMEOUT_MS_MIN:
        return max(3000, int(remaining_ms))
    return int(min(NAV_TIMEOUT_MS_MAX, max(NAV_TIMEOUT_MS_MIN, remaining_ms * 0.5)))


def _goto_with_retry(page, url: str, *, timeout: int, time_left, retries: int = NAVIGATION_RETRIES) -> bool:
    """Navigate with retries, logging, and a short backoff between attempts.

    The backoff gives transient network blips or momentary rate-limiting a
    moment to clear instead of hammering X with back-to-back retries.

    ``timeout`` is accepted for call-site compatibility but is intentionally
    ignored: we recompute an adaptive timeout before every attempt so that
    retries never ask for more time than the scraper actually has left.
    """
    last_exc = None
    total_attempts = retries + 1
    for attempt in range(total_attempts):
        remaining = time_left()
        if remaining <= 3:
            logger.warning(
                "Skipping navigation to %s: out of time budget "
                "(attempt=%d/%d remaining=%.1fs).",
                url, attempt + 1, total_attempts, remaining,
            )
            return False
        # Recompute adaptive timeout against the *current* remaining budget,
        # not the value frozen at call time.  This is the core fix: a failed
        # attempt burns real wall-clock time, so each retry must recalculate
        # rather than reuse a stale number that can now exceed what is left.
        attempt_timeout = _adaptive_nav_timeout(time_left)
        if attempt > 0:
            logger.info(
                "Twitter/X nav retry attempt=%d/%d url=%s remaining=%.1fs chosen_timeout=%dms",
                attempt + 1, total_attempts, url, remaining, attempt_timeout,
            )
        try:
            page.goto(url, wait_until="commit", timeout=attempt_timeout,)
            return True
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Twitter/X nav attempt %d/%d to %s failed: %s",
                attempt + 1, total_attempts, url, exc,
            )
            if attempt < retries:
                # Cap the backoff sleep to what is actually left minus the
                # minimum guard (3 s) so we never sleep past the deadline.
                backoff = min(
                    NAV_RETRY_BACKOFF_SECONDS * (attempt + 1),
                    max(0.0, time_left() - 3),
                )
                if backoff <= 0:
                    logger.warning(
                        "No time left for backoff before retry %d/%d to %s; aborting.",
                        attempt + 2, total_attempts, url,
                    )
                    break
                time.sleep(backoff)
    logger.error("Giving up on %s after %d attempts: %s", url, total_attempts, last_exc)
    return False


def _wait_for_more(page, selector: str, prev_count: int, max_ms: int = 500) -> int:
    """Poll ``selector``'s match count in short steps until it exceeds
    ``prev_count`` or ``max_ms`` elapses, whichever comes first.

    Replaces an unconditional ``page.wait_for_timeout(max_ms)`` after a
    scroll step: the worst-case wait is identical (this never runs longer
    than ``max_ms``), but as soon as new article nodes attach - which is
    most of the time on a live timeline - this returns immediately instead
    of always sitting out the full window, leaving more of the scraper's
    time budget for additional scroll iterations. Mirrors the same helper
    in youtube_scraper.py / google_scraper.py.
    """
    deadline = time.monotonic() + (max_ms / 1000)
    count = prev_count
    while time.monotonic() < deadline:
        try:
            count = page.locator(selector).count()
        except Exception:
            return count
        if count > prev_count:
            return count
        page.wait_for_timeout(100)
    return count


def _get_articles(page):
    """Fetch tweet/reply elements using a structural selector first.

    X renders each tweet as ``<article data-testid="tweet">``, which is
    far less brittle than a bare ``article`` tag (that attribute is tied
    to the component's role in the UI, not to styling that changes with
    every redesign). Falls back to the bare tag only if the structural
    selector finds nothing, so a markup change doesn't silently return
    zero results.
    """
    try:
        structural = page.locator('article[data-testid="tweet"]')
        if structural.count() > 0:
            return structural.all()
    except Exception:
        pass
    try:
        return page.locator("article").all()
    except Exception:
        return []


def _is_noise_article(article) -> bool:
    """Best-effort skip of pinned/promoted posts.

    Both are non-organic (an ad) or non-representative of current
    sentiment (a pinned post can be old/evergreen), so letting them into
    the sentiment sample skews results. Checked via the small
    "social context" line X renders above a tweet (e.g. "Pinned Tweet",
    "Promoted") rather than scanning the whole article for performance.
    """
    try:
        marker = article.locator('[data-testid="socialContext"]').first.inner_text(timeout=300)
    except Exception:
        return False
    marker_lower = marker.lower()
    return any(m in marker_lower for m in _NOISE_MARKERS)


def _expand_reply_threads(page, time_left, max_clicks: int = 4) -> None:
    """Click publicly-visible "show more" controls on a tweet detail page
    so replies that are collapsed by default get rendered before
    extraction.

    Only expands controls that are already visible on the unauthenticated
    page (nothing here logs in or bypasses a wall). Best-effort: missing
    buttons or click failures are silently skipped, and it stops early if
    the scraper's overall time budget is running low.
    """
    clicks = 0
    for text in _EXPAND_BUTTON_TEXTS:
        if clicks >= max_clicks or time_left() <= 3:
            break
        try:
            buttons = page.get_by_text(text, exact=False)
            count = min(buttons.count(), max_clicks - clicks)
        except Exception:
            continue
        for i in range(count):
            if time_left() <= 3:
                break
            try:
                buttons.nth(i).click(timeout=1500)
                clicks += 1
                page.wait_for_timeout(300)
            except Exception:
                continue


def _scroll_timeline_until_idle(page, time_left, max_items: int):
    """Scroll the timeline collecting unique tweet/reply text as it loads.

    Keeps scrolling until ``max_items`` tweets have been located,
    ``MAX_SCROLL_ITERATIONS`` scroll steps have happened,
    ``SCROLL_IDLE_LIMIT`` consecutive scrolls produced no new articles, or
    the overall scraper time budget runs low.

    Returns a ``(texts, tweet_links)`` tuple.
    """
    seen_text: List[str] = []
    seen_set = set()
    links: List[str] = []
    seen_links = set()

    iteration = 0
    idle = 0
    prev_count = -1
    start = time.monotonic()
    while True:
        iteration += 1
        if iteration > MAX_SCROLL_ITERATIONS:
            logger.info("Timeline scroll: hit MAX_SCROLL_ITERATIONS (%d).", MAX_SCROLL_ITERATIONS)
            break
        if time_left() <= 2 or len(seen_text) >= max_items:
            break

        try:
            for article in _get_articles(page):
                if _is_noise_article(article):
                    continue
                try:
                    text = article.inner_text()
                except Exception:
                    continue
                if text and len(text.split()) > 4:
                    key = " ".join(text.split()).lower()
                    if key not in seen_set:
                        seen_set.add(key)
                        seen_text.append(text)
                try:
                    href = article.locator("a[href*='/status/']").first.get_attribute("href")
                    if href:
                        link = "https://x.com" + href if href.startswith("/") else href
                        if link not in seen_links:
                            seen_links.add(link)
                            links.append(link)
                except Exception:
                    pass
        except Exception:
            pass

        count = len(seen_text)
        logger.info(
            "Timeline scroll: iteration=%d current_count=%d idle_streak=%d elapsed=%.1fs",
            iteration, count, idle, time.monotonic() - start,
        )

        if count >= max_items:
            logger.info("Timeline scroll: reached target of %d tweets.", max_items)
            break
        if count <= prev_count:
            idle += 1
            if idle >= SCROLL_IDLE_LIMIT:
                logger.info(
                    "Timeline scroll: no new tweets after %d consecutive scrolls; stopping.",
                    SCROLL_IDLE_LIMIT,
                )
                break
        else:
            idle = 0
        prev_count = max(prev_count, count)

        try:
            dom_count_before = page.locator('article[data-testid="tweet"], article').count()
        except Exception:
            dom_count_before = -1
        try:
            page.mouse.wheel(0, 1800)
        except Exception:
            break
        # Readiness-based wait: return as soon as new article nodes attach
        # instead of always sitting out the full 500ms window. Same
        # worst-case bound as the previous fixed sleep.
        _wait_for_more(page, 'article[data-testid="tweet"], article', dom_count_before, max_ms=500)

    return seen_text, links


async def scrape_twitter_comments(company_data: Dict[str, str]) -> List[str]:
    target = (
        company_data.get("twitter_url")
        or company_data.get("twitter")
    )

    loop = asyncio.get_event_loop()

    if not target:
        company_name = (company_data.get("company_name") or "").strip()
        if company_name:
            try:
                target = await loop.run_in_executor(
                    _EXECUTOR, find_social_profile_url, company_name, "twitter",
                )
            except Exception:
                target = ""
            if target:
                logger.info(
                    "Twitter/X scrape: no handle from the site scan; "
                    "search fallback found %r for %r.", target, company_name,
                )
        if not target:
            logger.info(
                "Twitter/X scrape: no handle from the site scan and the "
                "search fallback found nothing for %r; skipping rather "
                "than guessing a handle from the name.",
                company_data.get("company_name"),
            )
            return []

    url = _profile_url(target)

    if not url:
        logger.warning(
            "Twitter/X scrape: could not resolve a usable profile URL from %r.",
            target,
        )
        return []

    try:
        preflight_blocked = await loop.run_in_executor(
            _EXECUTOR,
            _httpx_preflight_blocked,
            url,
        )
    except Exception:
        preflight_blocked = False

    if preflight_blocked:
        logger.info(
            "Twitter/X scrape: preflight detected a login wall for %s before "
            "touching the browser pool.",
            url,
        )
        return []

    def _run() -> List[str]:

        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(
                asyncio.WindowsProactorEventLoopPolicy()
            )

        setup_start = time.monotonic()

        try:
            context = _ensure_context()
        except Exception:
            logger.exception(
                "Could not start/obtain a browser context for %r.",
                url,
            )
            return []

        results: List[str] = []
        duplicates_removed = 0
        seen = set()

        def _add(text: str):
            nonlocal duplicates_removed

            key = " ".join(text.split()).lower()

            if key in seen:
                duplicates_removed += 1
                return

            seen.add(key)
            results.append(text)

        page = None

        try:
            page = context.new_page()
            page.set_default_timeout(8000)

            setup_elapsed = time.monotonic() - setup_start

            start = time.monotonic()
            deadline = start + TIME_BUDGET_SECONDS

            logger.info(
                "Twitter/X scrape: browser/context/page setup took %.1fs; "
                "starting %ds navigation+scrape budget now.",
                setup_elapsed,
                TIME_BUDGET_SECONDS,
            )

            def time_left():
                return deadline - time.monotonic()

            _nav_start = time.monotonic()
            _nav_ok = _goto_with_retry(
                page,
                url,
                timeout=_adaptive_nav_timeout(time_left),
                time_left=time_left,
            )
            logger.info(
                "Twitter/X scrape: navigation took %.2fs (ok=%s).",
                time.monotonic() - _nav_start, _nav_ok,
            )
            if not _nav_ok:
                _save_debug_artifacts(page, url, "nav_failure")
                return normalize_comments(results)

            if _looks_blocked(page):
                try:
                    page.wait_for_selector("article", timeout=5000)
                    logger.info(
                        "Login prompt detected, but tweets are visible. Continuing."
                    )
                except Exception:
                    logger.info("Actual login wall detected.")
                    _save_debug_artifacts(page, url, "login_wall")
                    return normalize_comments(results)

            got_articles = True

            _select_start = time.monotonic()

            try:
                page.wait_for_selector("article", timeout=7000)
                page.wait_for_timeout(300)

            except Exception:

                got_articles = False

                reason = _diagnose_empty_page(page)

                try:
                    title = page.title()
                except Exception:
                    title = "<unknown>"

                logger.info(
                    "Twitter/X scrape: no tweets rendered for %s "
                    "(title=%r diagnosis=%s)",
                    url,
                    title,
                    reason,
                )

            logger.info(
                "Twitter/X scrape: wait_for_selector(article) took %.2fs (found=%s).",
                time.monotonic() - _select_start, got_articles,
            )

            tweet_links = []

            if got_articles and time_left() > 2:

                _scroll_start = time.monotonic()

                timeline_texts, tweet_links = _scroll_timeline_until_idle(
                    page,
                    time_left,
                    MAX_TOTAL_POSTS,
                )

                logger.info(
                    "Twitter/X scrape: timeline scrolling took %.2fs (collected=%d).",
                    time.monotonic() - _scroll_start, len(timeline_texts),
                )

                for t in timeline_texts:
                    _add(t)

            _reply_expand_total = 0.0

            for link in tweet_links:

                if len(results) >= MAX_TOTAL_POSTS or time_left() <= 3:
                    break

                try:

                    if not _goto_with_retry(
                        page,
                        link,
                        timeout=_adaptive_nav_timeout(time_left),
                        time_left=time_left,
                    ):
                        continue

                    try:
                        page.wait_for_selector(
                            'article[data-testid="tweet"], article',
                            timeout=1000,
                        )
                    except Exception:
                        pass

                    page.mouse.wheel(0, 1200)
                    page.wait_for_timeout(600)

                    _expand_start = time.monotonic()
                    _expand_reply_threads(page, time_left)
                    _reply_expand_total += time.monotonic() - _expand_start

                    remaining = MAX_TOTAL_POSTS - len(results)

                    replies_added = 0

                    articles = _get_articles(page)

                    for reply in articles[
                        1 : 1 + max(remaining, MAX_REPLIES_PER_TWEET)
                    ]:

                        if len(results) >= MAX_TOTAL_POSTS:
                            break

                        if _is_noise_article(reply):
                            continue

                        try:

                            text = reply.inner_text()

                            if text and len(text.split()) > 4:

                                before = len(results)

                                _add(text)

                                if len(results) > before:
                                    replies_added += 1

                        except Exception:
                            continue

                    logger.info(
                        "Twitter/X scrape: tweet=%s contributed %d replies.",
                        link,
                        replies_added,
                    )

                except Exception:
                    continue

            logger.info(
                "Twitter/X scrape: reply expansion took %.2fs total across %d link(s).",
                _reply_expand_total, len(tweet_links),
            )

            if not results:
                _save_debug_artifacts(
                    page,
                    url,
                    "zero_tweets",
                    extra={
                        "login_wall_present": str(_looks_blocked(page)),
                        "articles_exist": str(len(_get_articles(page)) > 0),
                        "diagnosis": _diagnose_empty_page(page),
                    },
                )

        finally:

            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass

        final = normalize_comments(results)[:MAX_TOTAL_POSTS]

        elapsed = time.monotonic() - start

        logger.info(
            "Twitter/X posts for %s: raw=%d duplicates_removed=%d final=%d "
            "elapsed=%.1fs",
            url,
            len(results),
            duplicates_removed,
            len(final),
            elapsed,
        )

        logger.info(
            "Twitter/X scrape: total scraper runtime %.2fs (setup+navigation+scrape).",
            time.monotonic() - setup_start,
        )

        logger.info("Twitter returned %d posts", len(final))

        return final

    logger.info("Twitter: submitting _run() to executor")

    try:
        result = await loop.run_in_executor(_EXECUTOR, _run)
        logger.info("Twitter: executor returned %d posts", len(result))
        return result

    except Exception:
        logger.exception(
            "Unhandled error scraping Twitter/X comments for %r.",
            url,
        )
        return []