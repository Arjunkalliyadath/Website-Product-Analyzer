import asyncio
import atexit
import concurrent.futures
import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Tuple

import httpx

from scrapers.browser_utils import browser_launch_slot, normalize_comments
from config import (
    MAX_INSTAGRAM_COMMENTS,
    MAX_SCROLL_ITERATIONS,
    SCROLL_IDLE_LIMIT,
    NAVIGATION_RETRIES,
    NAV_TIMEOUT_MS,
    NAV_TIMEOUT_MS_MAX,
    NAV_TIMEOUT_MS_MIN,
)

logger = logging.getLogger(__name__)

# Kept as an alias for backwards compatibility with any code importing the
# old name directly; the real cap now lives in config.py.
MAX_RESULTS = MAX_INSTAGRAM_COMMENTS

MIN_RESULTS_BEFORE_FALLBACK = 5

# How many individual post permalinks to try pulling captions/comments
# from once we're past the profile grid itself.
MAX_POSTS_TO_VISIT = 20

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
# config.py).
TIME_BUDGET_SECONDS = 12

_CHROME_MARKERS = (
    "followers", " posts", "view full profile", "following",
    "view profile", "log in", "sign up", "profile picture",
    "this account is private",
)

_NON_REVIEW_MARKERS = (
    "job description", "years of experience", "apply now", "we're hiring",
    "we are hiring", "job opening", "career opportunit", "job title",
    "questions about benefits", "employee review", "employer review",
    "work-life balance", "interview questions", "glassdoor",
    "salary range", "job posting", "now hiring", "currently hiring",
)

_MORE_COMMENTS_SELECTORS = [
    # Structural-first: real <button> elements matched by their
    # accessible/visible text. Instagram's embed markup renames its
    # generated CSS classes across deploys, but these render as semantic
    # buttons with stable, human-readable labels.
    "button:has-text('View more comments')",
    "button:has-text('Load more comments')",
    "button:has-text('View replies')",
    "button:has-text('Continue this thread')",
    # Instagram sometimes uses a styled <div role="button"> instead of a
    # native <button> for the same controls.
    "[role='button']:has-text('View more comments')",
    "[role='button']:has-text('Load more comments')",
    "[role='button']:has-text('View replies')",
    "[role='button']:has-text('Continue this thread')",
    # Class-based fallback, kept last, only for the one control that has
    # historically had no reliable text/role signature on some embed
    # variants.
    "span:has-text('View all')",
]

# Structural-first selector chain for captions/comment text. Each entry is
# tried in order via _first_matching_locator(); we stop at the first one
# that actually matches something on the page. Instagram rotates its
# generated CSS class names frequently, so leading with layout/structure
# (rows inside the comments list, elements near a <time> element) survives
# class churn far better than a single class-based selector. The
# class-based entry is kept last purely as a last-resort fallback for
# older embed markup that still uses stable "Caption"/"caption" class
# fragments.
_CAPTION_SELECTOR_CHAIN = [
    "article time ~ div",
    "article ul > li div[dir='auto']",
    "[class*='Caption'], [class*='caption']",
]

# Text scraping never needs images or fonts; skipping them cuts page load
# time without affecting anything we read out of the DOM.
_BLOCKED_RESOURCE_TYPES = {"image", "font"}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_LAUNCH_ARGS = [
    "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
]

# --- Browser pool -----------------------------------------------------------
# One Playwright browser + context per worker thread, kept alive across
# calls instead of being relaunched every time. This avoids paying
# Chromium process-startup cost on every single request and lets cookies
# persist between calls on the same worker thread.
MAX_BROWSER_WORKERS = 3
MAX_USES_BEFORE_RECYCLE = 50

_thread_local = threading.local()
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_BROWSER_WORKERS, thread_name_prefix="instagram_scraper"
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
        locale="en-US",
        viewport={"width": 1280, "height": 900},
    )
    context.set_default_timeout(8000)
    context.set_default_navigation_timeout(10000)
    _install_resource_blocking(context)

    handle.browser = browser
    handle.context = context
    handle.uses = 1
    logger.info("Launched a new browser context on %s.", threading.current_thread().name)
    return context


def _is_non_review(text: str) -> bool:
    low = text.lower()
    return _is_profile_chrome(text) or any(marker in low for marker in _NON_REVIEW_MARKERS)


def _is_profile_chrome(text: str) -> bool:
    low = f" {text.lower()} "
    hits = sum(1 for marker in _CHROME_MARKERS if marker in low)
    return hits >= 2


def _profile_url(target: str) -> str:
    target = (target or "").strip()
    if target.startswith(("http://", "https://")):
        return target
    return f"https://www.instagram.com/{target.lstrip('@')}/"


_BLOCK_TEXT_MARKERS = (
    "log in to see", "log in to continue", "this content isn't available",
    "sorry, this page isn't available",
)


def _first_matching_locator(page, selector_chain):
    """Try each selector in ``selector_chain`` in order; return the first
    one that matches at least one element on the page.

    Falls back to the *last* entry in the chain (the broadest, typically
    class-based selector) if none of the earlier, more structural
    selectors match anything, so behavior never silently regresses to
    "found nothing" just because a structural selector didn't apply to a
    particular page variant. Returns (locator_or_None, selector_used).
    """
    for sel in selector_chain[:-1]:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                return loc, sel
        except Exception:
            continue
    fallback_sel = selector_chain[-1]
    try:
        return page.locator(fallback_sel), fallback_sel
    except Exception:
        return None, fallback_sel


def _diagnose_page_state(page) -> str:
    """Classify why a page isn't yielding content, instead of logging a
    generic empty result.

    Returns one of: "ok", "login_wall", "checkpoint", "private_account",
    "no_public_comments", "rendering_failure", "selector_failure".

    URL-based checks run first since they're cheap and reliable; the
    bounded body-text check is the fallback for cases (common on the
    /embed/ pages) where Instagram overlays a login prompt or checkpoint
    without changing the URL at all.
    """
    try:
        url = page.url or ""
    except Exception:
        url = ""
    if "challenge" in url:
        return "checkpoint"
    if "accounts/login" in url or "/login" in url:
        return "login_wall"

    try:
        body_text = page.locator("body").inner_text(timeout=500).lower()
    except Exception:
        return "rendering_failure"

    if "this account is private" in body_text:
        return "private_account"
    if "checkpoint" in body_text:
        return "checkpoint"
    if any(marker in body_text for marker in _BLOCK_TEXT_MARKERS):
        return "login_wall"

    try:
        has_content = page.locator(
            "article, [class*='Caption'], [class*='caption']"
        ).count() > 0
    except Exception:
        return "selector_failure"
    if not has_content:
        return "no_public_comments"

    return "ok"


# --- Browser-free preflight ------------------------------------------------
# The log shows Instagram's embed page (the existing code's own
# login-wall-avoidance attempt) still landing on login_wall_present=True
# for every profile in this run - Instagram has tightened the embed
# endpoint enough that it's no longer a reliable dodge. Instagram is
# optional per the pipeline's priority order, so checking for that same
# login wall with one plain HTTP GET, BEFORE ever calling _ensure_context(),
# means a blocked profile never consumes one of the shared browser-launch
# slots (browser_launch_slot()) - freeing that capacity for Google/YouTube
# instead, without touching MAX_CONCURRENT_BROWSER_LAUNCHES or any timeout
# constant.
_PREFLIGHT_TIMEOUT_SECONDS = 4.0
_IG_LOGIN_MARKERS = ("/accounts/login", "/challenge")
_IG_LOGIN_TEXT_MARKERS = (
    "log in • instagram", "login • instagram", "loginform",
    "log in to see photos and videos",
)


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
    if any(marker in final_url for marker in _IG_LOGIN_MARKERS):
        return True
    body_sample = resp.text[:20000].lower()
    return any(marker in body_sample for marker in _IG_LOGIN_TEXT_MARKERS)


def _looks_blocked(page) -> bool:
    """Kept for internal call-site compatibility; now backed by the fuller
    ``_diagnose_page_state`` classification above."""
    return _diagnose_page_state(page) != "ok"


# Evidence directory for zero-result debug captures. Lives at
# <project_root>/debug (sibling of app.py's DOWNLOADS_DIR), since this file
# is itself at <project_root>/scrapers/instagram_scraper.py.
_DEBUG_DIR = Path(__file__).resolve().parent.parent / "debug"


def _save_debug_artifacts(page, identifier: str, reason: str, extra: Dict[str, str] = None) -> None:
    """Save HTML/screenshot/context ONLY on failure/zero-results for
    debugging. Never on the success path.

    Writes three files under ``_DEBUG_DIR``, all sharing one base name
    stamped with date-time + milliseconds + thread id so concurrent or
    rapid-fire failures never overwrite each other:
      * <base>.png - screenshot
      * <base>.html - full page HTML
      * <base>.txt  - final URL, page title, reason, and whatever the
        caller passes in ``extra`` (e.g. embed/profile page load status,
        login-wall presence).
    """
    try:
        _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_id = re.sub(r'[^a-zA-Z0-9]', '_', identifier)[:50].strip('_') or "unknown"
        stamp = f"{time.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 1000:03d}_{threading.get_ident()}"
        base = _DEBUG_DIR / f"instagram_{safe_id}_{stamp}"

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
            logger.warning("[INSTAGRAM_DEBUG] could not capture screenshot for %s", identifier)

        try:
            with open(f"{base}.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            logger.warning("[INSTAGRAM_DEBUG] could not capture HTML for %s", identifier)

        info = {
            "reason": reason,
            "identifier": identifier,
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
            "[INSTAGRAM_DEBUG] saved zero-result evidence for %s reason=%s: %s.{png,html,txt}",
            identifier, reason, base,
        )
    except Exception:
        logger.exception("[INSTAGRAM_DEBUG] failed to save debug artifacts for %s", identifier)


def _find_scroll_container(page):
    """Find the element that actually holds the scrollable comments feed,
    instead of assuming either "the whole page" or a blindly-supplied
    locator is correct.

    Scrolling the wrong (non-scrollable) container is indistinguishable
    from "nothing new loaded" and makes the idle-detection in
    ``_scroll_until_idle`` trigger for the wrong reason. Returns ``None``
    (meaning "fall back to page-level scroll") if nothing on the page is
    actually scrollable.
    """
    for sel in ("article", "[role='dialog']", "main", "body"):
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            scrollable = loc.evaluate("el => el.scrollHeight > el.clientHeight + 20")
            if scrollable:
                return loc
        except Exception:
            continue
    return None


NAV_RETRY_BACKOFF_SECONDS = 0.75


def _adaptive_nav_timeout(time_left) -> int:
    """Scale the navigation timeout to how much of the internal time
    budget is actually left, instead of a fixed 5000ms for every attempt.

    Navigation always receives at least NAV_TIMEOUT_MS_MIN (8s) as long as
    that much time is actually left in the budget - the budget clock only
    starts once browser/context/page setup has finished (see
    scrape_instagram_comments below), so this floor is real, not eaten by
    Chromium startup. When plenty of budget remains the timeout can scale
    up to NAV_TIMEOUT_MS_MAX (12s) for a genuinely slow page; when the
    budget itself is under the floor, we hand over whatever is left rather
    than blocking past the scraper's own deadline.
    """
    remaining_ms = max(0.0, time_left()) * 1000
    if remaining_ms <= NAV_TIMEOUT_MS_MIN:
        return max(3000, int(remaining_ms))
    return int(min(NAV_TIMEOUT_MS_MAX, max(NAV_TIMEOUT_MS_MIN, remaining_ms * 0.5)))


def _goto_with_retry(page, url: str, *, timeout: int, time_left, retries: int = NAVIGATION_RETRIES) -> bool:
    """Navigate with retries, logging, and a short backoff between attempts.

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
                "Instagram nav retry attempt=%d/%d url=%s remaining=%.1fs chosen_timeout=%dms",
                attempt + 1, total_attempts, url, remaining, attempt_timeout,
            )
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=attempt_timeout)
            return True
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Instagram nav attempt %d/%d to %s failed: %s",
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


def _expand_more_comments(page, time_left, limit: int = 20) -> int:
    """Click "View more comments" / "Load more comments" style controls."""
    clicked = 0
    for sel in _MORE_COMMENTS_SELECTORS:
        if clicked >= limit or time_left() <= 2:
            break
        try:
            buttons = page.locator(sel).all()
        except Exception:
            continue
        for btn in buttons[: max(0, limit - clicked)]:
            if time_left() <= 2:
                break
            try:
                if btn.is_visible():
                    btn.click(timeout=600)
                    clicked += 1
                    page.wait_for_timeout(200)
            except Exception:
                continue
    return clicked


def _wait_for_more(page, count_fn, prev_count: int, max_ms: int = 500) -> int:
    """Poll ``count_fn()`` in short steps until it exceeds ``prev_count`` or
    ``max_ms`` elapses, whichever comes first.

    Replaces an unconditional ``page.wait_for_timeout(max_ms)`` after a
    scroll step: the worst-case wait is identical (this never runs longer
    than ``max_ms``), but as soon as new items attach - which is most of
    the time when content is actually available - this returns immediately
    instead of always sitting out the full window, leaving more of the
    scraper's time budget for additional scroll iterations. Mirrors the
    same helper in youtube_scraper.py / google_scraper.py / twitter_scraper.py,
    adapted here to reuse the ``count_fn`` already threaded through this
    function's caller instead of a fixed selector.
    """
    deadline = time.monotonic() + (max_ms / 1000)
    count = prev_count
    while time.monotonic() < deadline:
        try:
            count = count_fn()
        except Exception:
            return count
        if count > prev_count:
            return count
        page.wait_for_timeout(80)
    return count


def _scroll_until_idle(page, time_left, count_fn, max_items: int, scroll_target=None) -> int:
    """Generic idle-bounded scroll loop shared by the profile/post passes.

    Keeps scrolling (page-level, or a specific ``scroll_target`` locator if
    given) until ``max_items`` have been located, ``MAX_SCROLL_ITERATIONS``
    scroll steps have happened, ``SCROLL_IDLE_LIMIT`` consecutive scrolls
    produced no new items, or the overall scraper time budget runs low.

    Returns the last observed count.

    Note: Instagram does not serve most content (full comment threads,
    additional posts beyond the first screenful, etc.) to unauthenticated
    sessions — it redirects to a login wall instead. In practice this loop
    will often exit quickly via the idle-streak condition simply because
    nothing new is available to load without logging in, not because of a
    bug here.
    """
    iteration = 0
    idle = 0
    prev_count = -1
    start = time.monotonic()
    while True:
        iteration += 1
        if iteration > MAX_SCROLL_ITERATIONS:
            logger.info("Instagram scroll: hit MAX_SCROLL_ITERATIONS (%d).", MAX_SCROLL_ITERATIONS)
            break
        if time_left() <= 2:
            logger.info("Instagram scroll: stopping, low on overall time budget.")
            break

        count = count_fn()
        logger.info(
            "Instagram scroll: iteration=%d current_count=%d idle_streak=%d elapsed=%.1fs",
            iteration, count, idle, time.monotonic() - start,
        )
        if count >= max_items:
            logger.info("Instagram scroll: reached target of %d item(s).", max_items)
            break
        if count <= prev_count:
            idle += 1
            if idle >= SCROLL_IDLE_LIMIT:
                logger.info(
                    "Instagram scroll: no new items after %d consecutive scrolls; stopping.",
                    SCROLL_IDLE_LIMIT,
                )
                break
        else:
            idle = 0
        prev_count = max(prev_count, count)

        _expand_more_comments(page, time_left)
        try:
            if scroll_target is not None:
                scroll_target.evaluate("el => el.scrollTo(0, el.scrollHeight)")
            else:
                page.mouse.wheel(0, 1200)
        except Exception:
            break
        # Readiness-based wait: return as soon as new items attach instead
        # of always sitting out the full 450ms window. Same worst-case
        # bound as the previous fixed sleep.
        _wait_for_more(page, count_fn, prev_count, max_ms=450)

    return max(prev_count, 0)


async def scrape_instagram_comments(company_data: Dict[str, str]) -> List[str]:
    target = (
        company_data.get("instagram_url")
        or company_data.get("instagram")
        or company_data.get("company_name", "")
    )
    if not target:
        return []

    handle = target
    for prefix in ("https://www.instagram.com/", "https://instagram.com/"):
        if handle.startswith(prefix):
            handle = handle[len(prefix):].strip("/")
    handle = handle.lstrip("@")

    profile_url = _profile_url(handle)
    company_name = company_data.get("company_name", handle)

    loop = asyncio.get_event_loop()
    embed_preflight_url = profile_url.rstrip("/") + "/embed/"
    try:
        preflight_blocked = await loop.run_in_executor(_EXECUTOR, _httpx_preflight_blocked, embed_preflight_url)
    except Exception:
        preflight_blocked = False
    if preflight_blocked:
        logger.info(
            "Instagram scrape: preflight detected a login wall for %s before "
            "touching the browser pool - returning [] immediately "
            "(reason=login_wall, no Chromium launch spent on this).", embed_preflight_url,
        )
        return []

    def _run() -> List[str]:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        setup_start = time.monotonic()

        try:
            context = _ensure_context()
        except Exception:
            logger.exception("Could not start/obtain a browser context for %r.", profile_url)
            return []

        results: List[str] = []
        duplicates_removed = 0
        seen = set()

        def _add(txt: str) -> None:
            nonlocal duplicates_removed
            txt = (txt or "").strip()
            # Strip trailing UI affordances that sometimes get bundled
            # into the same text node as the caption/comment itself (e.g.
            # a lingering "Reply" button label or "See translation"
            # link), so two otherwise-identical comments aren't treated
            # as unique just because one has this trailing chrome and the
            # other doesn't.
            txt = re.sub(r"\s*(Reply|See translation)\s*$", "", txt).strip()
            if not txt or len(txt.split()) < 6 or _is_non_review(txt):
                return
            key = " ".join(txt.split()).lower()
            if key in seen:
                duplicates_removed += 1
                return
            seen.add(key)
            results.append(txt)

        page = None
        try:
            page = context.new_page()
            page.set_default_timeout(7000)

            # The internal time budget clock starts here, only AFTER
            # browser launch, context creation, and page creation have all
            # finished - not before. Starting the clock earlier meant
            # Chromium/context startup time silently ate into the budget
            # before navigation ever got a chance to run, which is exactly
            # what produced "Skipping navigation: out of time budget"
            # before the page had even been opened.
            setup_elapsed = time.monotonic() - setup_start
            start = time.monotonic()
            deadline = start + TIME_BUDGET_SECONDS
            logger.info(
                "Instagram scrape: browser/context/page setup took %.1fs; "
                "starting %ds navigation+scrape budget now.",
                setup_elapsed, TIME_BUDGET_SECONDS,
            )

            def time_left() -> float:
                return deadline - time.monotonic()

            # --- Primary attempt: public embed page (no login wall) -------
            embed_url = profile_url.rstrip("/") + "/embed/"
            post_links: List[str] = []
            profile_page_loaded = None  # None = fallback never attempted
            embed_page_loaded = _goto_with_retry(page, embed_url, timeout=_adaptive_nav_timeout(time_left), time_left=time_left)
            if embed_page_loaded:
                # Readiness-based wait: wait for actual content to show up
                # rather than sleeping a fixed amount of time regardless of
                # whether the page is ready sooner or slower than that.
                try:
                    page.wait_for_selector(
                        "article, [class*='Caption'], [class*='caption'], "
                        "a[href*='/p/'], a[href*='/reel/']",
                        timeout=4000,
                    )
                except Exception:
                    reason = _diagnose_page_state(page)
                    logger.info(
                        "Instagram scrape: embed page for %s never rendered "
                        "caption/post content within 4s (reason=%s).",
                        profile_url, reason,
                    )

                def _caption_count() -> int:
                    try:
                        loc, _sel = _first_matching_locator(page, _CAPTION_SELECTOR_CHAIN)
                        return loc.count() if loc is not None else -1
                    except Exception:
                        return -1

                scroll_container = _find_scroll_container(page)
                _scroll_until_idle(
                    page, time_left, _caption_count, MAX_INSTAGRAM_COMMENTS,
                    scroll_target=scroll_container,
                )
                caption_loc, used_sel = _first_matching_locator(page, _CAPTION_SELECTOR_CHAIN)
                if caption_loc is not None:
                    logger.info(
                        "Instagram scrape: embed page for %s using caption "
                        "selector %r.", profile_url, used_sel,
                    )
                    for loc in caption_loc.all():
                        try:
                            _add(loc.inner_text())
                        except Exception:
                            pass
                try:
                    for a in page.locator("a[href*='/p/'], a[href*='/reel/']").all()[:MAX_POSTS_TO_VISIT]:
                        href = a.get_attribute("href")
                        if href:
                            full = "https://www.instagram.com" + href if href.startswith("/") else href
                            if full not in post_links:
                                post_links.append(full)
                except Exception:
                    pass

            # --- Fallback: direct profile page, skipped instantly if it
            # redirects to a login/checkpoint wall ------------------------
            if len(results) < MIN_RESULTS_BEFORE_FALLBACK and time_left() > 4:
                profile_page_loaded = _goto_with_retry(page, profile_url, timeout=_adaptive_nav_timeout(time_left), time_left=time_left)
                if profile_page_loaded:
                    try:
                        page.wait_for_selector(
                            "h1, span._aacl, div._aacl, [role='main']", timeout=3000,
                        )
                    except Exception:
                        pass
                    reason = _diagnose_page_state(page)
                    if reason != "ok":
                        logger.info(
                            "Instagram scrape: profile fallback for %s "
                            "unavailable (reason=%s); skipping extraction.",
                            profile_url, reason,
                        )
                    else:
                        for sel in ["span._aacl", "div._aacl", "h1", "span"]:
                            for loc in page.locator(sel).all()[:50]:
                                try:
                                    txt = loc.inner_text()
                                    if txt and len(txt.split()) >= 6:
                                        _add(txt)
                                except Exception:
                                    pass
                        try:
                            for a in page.locator("a[href*='/p/'], a[href*='/reel/']").all()[:MAX_POSTS_TO_VISIT]:
                                href = a.get_attribute("href")
                                if href:
                                    full = "https://www.instagram.com" + href if href.startswith("/") else href
                                    if full not in post_links:
                                        post_links.append(full)
                        except Exception:
                            pass

            # --- Visit individual posts (captioned embeds) to pick up
            # additional captions/top comments beyond the profile grid,
            # continuing until the cap is hit, the links run out, or we
            # run low on time.
            for link in post_links:
                if len(results) >= MAX_INSTAGRAM_COMMENTS or time_left() <= 4:
                    break
                post_embed = link.rstrip("/") + "/embed/captioned/"
                if not _goto_with_retry(page, post_embed, timeout=_adaptive_nav_timeout(time_left), time_left=time_left):
                    continue
                try:
                    page.wait_for_selector(
                        "article, [class*='Caption'], [class*='caption']", timeout=3000,
                    )
                except Exception:
                    pass
                reason = _diagnose_page_state(page)
                if reason != "ok":
                    logger.info(
                        "Instagram scrape: post %s unavailable (reason=%s); skipping.",
                        post_embed, reason,
                    )
                    continue
                before = len(results)
                _expand_more_comments(page, time_left)
                caption_loc, used_sel = _first_matching_locator(page, _CAPTION_SELECTOR_CHAIN)
                if caption_loc is not None:
                    for loc in caption_loc.all():
                        try:
                            _add(loc.inner_text())
                        except Exception:
                            pass
                logger.info(
                    "Instagram scrape: post=%s contributed %d new item(s); "
                    "running total=%d/%d.",
                    post_embed, len(results) - before, len(results), MAX_INSTAGRAM_COMMENTS,
                )

            # --- One search fallback (single query), only if still short
            # on data and there's time left ------------------------------
            if len(results) < MIN_RESULTS_BEFORE_FALLBACK and time_left() > 4:
                q = f'site:instagram.com "{company_name}"'
                search_url = f"https://www.google.com/search?q={q.replace(' ', '+')}&hl=en&num=20"
                if _goto_with_retry(page, search_url, timeout=_adaptive_nav_timeout(time_left), time_left=time_left):
                    try:
                        page.wait_for_selector(
                            "div.VwiC3b, span.aCOpRe, div.IsZvec, div.g", timeout=3000,
                        )
                    except Exception:
                        pass
                    for sel in ["div.VwiC3b", "span.aCOpRe", "div.IsZvec",
                                "div.lyLwlc", "span.MUxGbd"]:
                        for loc in page.locator(sel).all()[:25]:
                            try:
                                _add(loc.inner_text())
                            except Exception:
                                pass

            if not results:
                _save_debug_artifacts(
                    page, profile_url, "zero_comments",
                    extra={
                        "embed_page_loaded": str(embed_page_loaded),
                        "profile_page_loaded": str(profile_page_loaded),
                        "login_wall_present": str(_looks_blocked(page)),
                        "diagnosis": _diagnose_page_state(page),
                    },
                )
        except Exception:
            logger.exception("Instagram scrape failed for %r.", profile_url)
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass

        final = normalize_comments(results)[:MAX_INSTAGRAM_COMMENTS]
        elapsed = time.monotonic() - start
        logger.info(
            "Instagram items for %r: raw=%d duplicates_removed=%d final=%d "
            "elapsed=%.1fs (cap=%d).",
            company_name, len(results), duplicates_removed, len(final), elapsed,
            MAX_INSTAGRAM_COMMENTS,
        )
        logger.info("Instagram returned %d comments", len(final))
        return final

    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(_EXECUTOR, _run)
    except Exception:
        logger.exception("Unhandled error scraping Instagram comments for %r.", profile_url)
        return []
