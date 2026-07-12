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

from scrapers.browser_utils import normalize_comments
from config import (
    MAX_YOUTUBE_COMMENTS,
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
# app.py (20s) so this scraper almost always returns on its own, with
# whatever it has collected so far, instead of being cut off cold by the
# outer timeout and losing partial results. Lowered from 16 -> 12 now that
# navigation itself fails fast (NAV_TIMEOUT_MS, NAVIGATION_RETRIES in
# config.py).
TIME_BUDGET_SECONDS = 17

# How many candidate video URLs to gather from the channel/search page
# before visiting them for comments. Listing videos is cheap; visiting
# each one for comments is the expensive part, so we keep some candidates
# in reserve in case early videos have comments disabled or turn out to
# have little content.
VIDEO_CANDIDATES_TO_COLLECT = 15

# Overall cap on how many comments this scraper will try to collect across
# all videos for one company/channel, sourced from config so it can be
# tuned without touching this file.
MAX_TOTAL_COMMENTS = MAX_YOUTUBE_COMMENTS

_VIDEO_ID_RE = re.compile(r'"videoId":"([\w-]{11})"')
# Shorts entries embed their video ID under a distinct "reelWatchEndpoint"
# key rather than the plain videoRenderer key that _VIDEO_ID_RE matches, so
# we can collect Shorts IDs separately and subtract them out. This is what
# lets the raw regex fallback (used when the /videos tab renders too little
# for the DOM-based selectors to find anything) avoid pulling in Shorts.
_SHORTS_ID_RE = re.compile(r'"reelWatchEndpoint":\{"videoId":"([\w-]{11})"')
_CONSENT_LABELS = ("Accept all", "I agree", "Accept the use of cookies", "Reject all")

# --- Structural selector fallbacks -----------------------------------------
# YouTube has shipped at least two different comment DOM shapes in
# production: the long-standing ytd-comment-renderer tree, and a newer
# "view model" rearchitecture (ytd-comment-view-model) that some sessions
# get instead. Relying on a single selector meant a whole scrape could
# silently return 0 comments on any session bucketed into the newer shape.
# Every comment-related locator below is a comma-joined CSS selector list
# (a native CSS/Playwright union — a match on any alternative counts)
# instead of one hardcoded string.
_COMMENT_TEXT_SELECTOR = (
    "ytd-comment-renderer #content-text, "
    "ytd-comment-view-model #content-text, "
    "ytd-comment-view-model #content-text span, "
    "ytd-comment-view-model .yt-core-attributed-string, "
    "ytd-comment-view-model yt-attributed-string, "
    "ytd-backstage-comment #content-text, "
    "#comment #content-text, "
    "ytd-comment-thread-renderer #content-text"
)
_COMMENTS_CONTAINER_SELECTOR = (
    "ytd-comments, #comments, ytd-item-section-renderer#sections, "
    "ytd-comment-thread-renderer, ytd-comments-header-renderer"
)

# "Read more" / "more replies" controls are scoped to *inside* the comments
# section (the leading "ytd-comments "). The unscoped selector this
# replaces, "ytd-expander #more", also matches the video description's own
# "...more" expander, which sits in an unrelated ytd-expander higher up the
# page — clicking it expanded the description panel instead of a truncated
# comment, i.e. exactly the "unrelated control" failure mode to avoid.
_READ_MORE_SELECTOR = (
    "ytd-comments ytd-expander #more, "
    "ytd-comments tp-yt-paper-button#more, "
    "ytd-comments ytd-comment-view-model ytd-expander #more, "
    "ytd-comments #more.ytd-expander"
)
_MORE_REPLIES_SELECTOR = (
    "ytd-comments ytd-comment-replies-renderer #more-replies, "
    "ytd-comments ytd-comment-replies-renderer tp-yt-paper-button#more-replies, "
    "ytd-comments ytd-comment-replies-renderer ytd-button-renderer#more-replies"
)

_VIDEO_TILE_WAIT_SELECTOR = "a#video-title, a#video-title-link, ytd-rich-grid-renderer"
_SEARCH_RESULT_WAIT_SELECTOR = "ytd-video-renderer a#video-title, a#video-title, ytd-item-section-renderer"

# Extra fallback locators for video discovery, tried in order inside
# _collect_video_urls after the original two. ytd-video-renderer is the
# search-results-page renderer (as opposed to the channel grid's
# ytd-rich-item-renderer); the href-based selector is a last-resort net for
# either page shape if YouTube renames the element/id again.
_VIDEO_LINK_SELECTORS = (
    "a#video-title",
    "a#video-title-link",
    "ytd-video-renderer a#video-title",
    "ytd-rich-item-renderer a#video-title-link",
    "a#thumbnail[href*='/watch']",
)

# Text markers used to tell a genuine "0 comments" video apart from a
# bot-check/login interstitial or an uploader-disabled comments section —
# all of which otherwise look identical from the outside, since our comment
# selectors simply find nothing either way.
_LOGIN_WALL_RE = re.compile(
    r"sign in to confirm|confirm you.?re not a bot|sign in to continue", re.I
)
_COMMENTS_DISABLED_RE = re.compile(
    r"comments (?:are|is) turned off|comments (?:are|is) disabled", re.I
)

# Text scraping never needs images or fonts; skipping them cuts page load
# time without affecting anything we read out of the DOM. JavaScript is
# left untouched since YouTube's comment feed is rendered client-side.
_BLOCKED_RESOURCE_TYPES = {"image", "font"}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_LAUNCH_ARGS = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]

# --- Browser pool -----------------------------------------------------------
# One Playwright browser + context per worker thread, kept alive across
# calls instead of being relaunched every time. This avoids paying
# Chromium process-startup cost and the consent-cookie dance on every
# single request, and lets cookies/consent state persist between calls on
# the same worker thread.
MAX_BROWSER_WORKERS = 3
MAX_USES_BEFORE_RECYCLE = 50

_thread_local = threading.local()
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_BROWSER_WORKERS, thread_name_prefix="youtube_scraper"
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

    browser = pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
    context = browser.new_context(
        user_agent=_USER_AGENT,
        locale="en-US",
        viewport={"width": 1280, "height": 900},
        java_script_enabled=True,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    # Mask the webdriver flag that YouTube uses for bot detection
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)
    context.set_default_timeout(8000)
    context.set_default_navigation_timeout(12000)
    _install_resource_blocking(context)

    handle.browser = browser
    handle.context = context
    handle.uses = 1
    logger.info("Launched a new browser context on %s.", threading.current_thread().name)
    return context


_CHANNEL_ID_RE = re.compile(r"^UC[\w-]{22}$")
_YT_PATH_RE = re.compile(
    r"(?:youtube\.com|youtu\.be)/(channel/[\w-]+|user/[\w.-]+|c/[\w.-]+|@[\w.-]+)",
    re.IGNORECASE,
)


def _channel_url(target: str) -> str:
    """Turn whatever the caller has on file for a channel into a canonical
    ``https://www.youtube.com/...`` URL.

    Handles, in order:
      * a full URL (any of channel/, user/, c/, @handle, or bare domain
        with no scheme),
      * a bare "channel/UC...", "user/name", or "c/name" path fragment,
      * a bare channel ID (starts with "UC", 24 chars),
      * anything else, treated as an @handle.

    The previous version only matched the "channel/", "user/", "c/"
    prefixes when they were the *very first* characters of the string, so
    a value like "youtube.com/channel/UCxxxx" (a URL missing its scheme,
    which is a common way this field gets populated) fell through to the
    "@handle" branch and produced a broken URL
    (``.../@youtube.com/channel/UCxxxx``). This version searches for the
    known path patterns anywhere in the string and normalizes them.
    """
    target = (target or "").strip()
    if not target:
        return "https://www.youtube.com/"

    if target.startswith(("http://", "https://")):
        # Already a full URL — just use it as-is (it may not even be a
        # youtube.com URL, e.g. a youtu.be short link to a specific video;
        # the caller-level fallback search handles that case downstream).
        return target

    m = _YT_PATH_RE.search(target)
    if m:
        return f"https://www.youtube.com/{m.group(1)}"

    if target.startswith(("channel/", "user/", "c/")):
        return f"https://www.youtube.com/{target}"

    stripped = target.lstrip("@")
    if _CHANNEL_ID_RE.match(stripped):
        return f"https://www.youtube.com/channel/{stripped}"

    return f"https://www.youtube.com/@{stripped}"


def _dismiss_consent(page, timeout_ms: int = 1500) -> None:
    """Dismiss a cookie/consent dialog if one is showing.

    Previously this relied on the *caller* sleeping ~1.5s first so the
    dialog had time to render, then doing an instant ``count() > 0`` check
    per label — meaning every single page load paid a fixed 1.5s whether or
    not a dialog ever appeared. This version waits on each label directly
    (via Playwright's own actionability wait) up to a shared
    ``timeout_ms`` budget, so it returns almost immediately when no dialog
    is present and only spends time waiting when one might still be
    rendering.
    """
    deadline = time.monotonic() + (timeout_ms / 1000)
    for label in _CONSENT_LABELS:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            btn = page.get_by_role("button", name=label).first
            btn.wait_for(state="visible", timeout=max(200, remaining * 1000))
            btn.click(timeout=600)
            page.wait_for_timeout(200)
            return
        except Exception:
            continue


def _consent_dialog_present(page) -> bool:
    """Non-waiting check for whether a consent/cookie dialog is currently
    showing. Used only for diagnostics after collection has already given
    up — not part of the dismissal attempt above, which needs the
    actionability wait instead of an instant count.
    """
    try:
        for label in _CONSENT_LABELS:
            if page.get_by_role("button", name=label).count() > 0:
                return True
    except Exception:
        pass
    return False


def _diagnose_blocking_state(page) -> str:
    """Classify why a *listing* page (channel videos tab or search
    results) rendered nothing — the same consent/login-wall checks used for
    a video's comments, minus the comments-specific container/selector
    logic that doesn't apply here.
    """
    if _consent_dialog_present(page):
        return "consent_dialog"
    try:
        html = page.content()
    except Exception:
        html = ""
    if html and _LOGIN_WALL_RE.search(html):
        return "login_wall"
    return "rendering_failure_or_no_results"


# Evidence directory for zero-result debug captures. Lives at
# <project_root>/debug (sibling of app.py's DOWNLOADS_DIR), since this file
# is itself at <project_root>/scrapers/youtube_scraper.py.
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
        caller passes in ``extra`` (e.g. consent-dialog appearance,
        video-grid load status).
    """
    try:
        _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_id = re.sub(r'[^a-zA-Z0-9]', '_', identifier)[:50].strip('_') or "unknown"
        stamp = f"{time.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 1000:03d}_{threading.get_ident()}"
        base = _DEBUG_DIR / f"youtube_{safe_id}_{stamp}"

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
            logger.warning("[YOUTUBE_DEBUG] could not capture screenshot for %s", identifier)

        try:
            with open(f"{base}.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            logger.warning("[YOUTUBE_DEBUG] could not capture HTML for %s", identifier)

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
            "[YOUTUBE_DEBUG] saved zero-result evidence for %s reason=%s: %s.{png,html,txt}",
            identifier, reason, base,
        )
    except Exception:
        logger.exception("[YOUTUBE_DEBUG] failed to save debug artifacts for %s", identifier)


def _adaptive_nav_timeout(time_left) -> int:
    """Scale the navigation timeout to how much of the internal time
    budget is actually left, instead of a fixed 5000ms for every attempt.

    Navigation always receives at least NAV_TIMEOUT_MS_MIN (8s) as long as
    that much time is actually left in the budget - the budget clock only
    starts once browser/context/page setup has finished (see _run_sync
    below), so this floor is real, not eaten by Chromium startup. When
    plenty of budget remains the timeout can scale up to NAV_TIMEOUT_MS_MAX
    (12s) for a genuinely slow page; when the budget itself is under the
    floor, we hand over whatever is left rather than blocking past the
    scraper's own deadline.
    """
    remaining_ms = max(0.0, time_left()) * 1000
    if remaining_ms <= NAV_TIMEOUT_MS_MIN:
        return max(3000, int(remaining_ms))
    return int(min(NAV_TIMEOUT_MS_MAX, max(NAV_TIMEOUT_MS_MIN, remaining_ms * 0.5)))


_YT_NAV_RETRY_BACKOFF_SECONDS = 0.75


def _goto_with_retry(page, url: str, *, timeout: int, time_left, retries: int = NAVIGATION_RETRIES) -> bool:
    # ``timeout`` is accepted for call-site compatibility but is intentionally
    # ignored: we recompute an adaptive timeout before every attempt so that
    # retries never ask for more time than the scraper actually has left.
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
                "YouTube nav retry attempt=%d/%d url=%s remaining=%.1fs chosen_timeout=%dms",
                attempt + 1, total_attempts, url, remaining, attempt_timeout,
            )
        try:
            # Use "load" instead of "domcontentloaded": YouTube's comment
            # section is bootstrapped by JS that fires during the load event,
            # not at DOMContentLoaded. Using domcontentloaded meant we started
            # scrolling before the comment lazy-loader JS had even been parsed.
            page.goto(url, wait_until="load", timeout=attempt_timeout)
            return True
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "YouTube nav attempt %d/%d to %s failed: %s",
                attempt + 1, total_attempts, url, exc,
            )
            if attempt < retries:
                # Cap the backoff sleep to what is actually left minus the
                # minimum guard (3 s) so we never sleep past the deadline.
                backoff = min(
                    _YT_NAV_RETRY_BACKOFF_SECONDS * (attempt + 1),
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


def _extract_video_urls_from_html(html: str, limit: int) -> List[str]:
    shorts_ids = set(_SHORTS_ID_RE.findall(html))
    seen: List[str] = []
    for vid in _VIDEO_ID_RE.findall(html):
        if vid in shorts_ids:
            continue
        url = f"https://www.youtube.com/watch?v={vid}"
        if url not in seen:
            seen.append(url)
        if len(seen) >= limit:
            break
    return seen


# Video renderers that live inside a Shorts shelf (as opposed to the normal
# uploads grid) are wrapped in one of these ancestor tags. We exclude
# anchors found inside them so a channel's Shorts don't get treated as
# regular videos to open for comments. Kept as bare tag names (no attribute
# selectors) since these are used inside an XPath ancestor:: check.
_SHORTS_CONTAINER_TAGS = (
    "ytd-reel-item-renderer",
    "ytd-reel-shelf-renderer",
    "ytm-shorts-lockup-view-model",
)


def _collect_video_urls(page, limit: int) -> List[str]:
    urls: List[str] = []
    shorts_skipped = 0

    for sel in _VIDEO_LINK_SELECTORS:
        for loc in page.locator(sel).all()[:limit]:
            try:
                href = loc.get_attribute("href")
            except Exception:
                continue
            if not href or "watch" not in href or "/shorts/" in href:
                if href and "/shorts/" in href:
                    shorts_skipped += 1
                continue
            try:
                # Skip anything whose ancestor chain marks it as a Shorts
                # shelf item, even though it happened to carry a /watch/
                # style href (rare, but seen on some Shorts renderer
                # variants).
                if any(
                    loc.locator(f"xpath=ancestor::{tag}").count() > 0
                    for tag in _SHORTS_CONTAINER_TAGS
                ):
                    shorts_skipped += 1
                    continue
            except Exception:
                pass
            full = "https://www.youtube.com" + href if href.startswith("/") else href
            if full not in urls:
                urls.append(full)
        if len(urls) >= limit:
            break

    if len(urls) < limit:
        try:
            html = page.content()
            for u in _extract_video_urls_from_html(html, limit * 2):
                if u not in urls:
                    urls.append(u)
                if len(urls) >= limit:
                    break
        except Exception:
            pass

    if shorts_skipped:
        logger.info("Video discovery: skipped %d Shorts entr(y/ies).", shorts_skipped)

    return urls[:limit]


def _expand_comment_extras(page, time_left, limit: int = 60) -> int:
    """Click "Read more" on truncated comments and expand reply threads.

    Both selectors are scoped to *inside* the comments section
    (``_READ_MORE_SELECTOR`` / ``_MORE_REPLIES_SELECTOR`` both lead with
    "ytd-comments "), so this never touches the video description's own
    "...more" expander, which shares the same underlying ytd-expander
    structure but lives higher up the page. Both are only clicked when
    Playwright reports the control as currently visible — a comment that's
    attached to the DOM but scrolled off-screen is left alone.
    """
    clicked = 0

    try:
        more_buttons = page.locator(_READ_MORE_SELECTOR).all()
    except Exception:
        more_buttons = []
    for btn in more_buttons[:limit]:
        if time_left() <= 2:
            return clicked
        try:
            if btn.is_visible():
                btn.click(timeout=600)
                clicked += 1
                page.wait_for_timeout(80)
        except Exception:
            continue

    try:
        reply_buttons = page.locator(_MORE_REPLIES_SELECTOR).all()
    except Exception:
        reply_buttons = []
    for btn in reply_buttons[:limit]:
        if time_left() <= 2:
            return clicked
        try:
            if btn.is_visible():
                btn.click(timeout=600)
                clicked += 1
                page.wait_for_timeout(150)
        except Exception:
            continue

    return clicked


def _wait_for_more(page, selector: str, prev_count: int, max_ms: int = 500) -> int:
    """Poll ``selector``'s match count in short steps until it exceeds
    ``prev_count`` or ``max_ms`` elapses, whichever comes first.

    Replaces an unconditional ``page.wait_for_timeout(max_ms)`` after a
    scroll: the worst-case wait is identical (this never runs longer than
    ``max_ms``), but as soon as new nodes attach — which is most of the
    time on a live feed — this returns immediately instead of always
    sitting out the full window. Returns the last observed count so
    callers can use it as the next call's baseline.
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


def _classify_comment_failure(page, time_left) -> str:
    """Classify why a video yielded zero comments.

    The previous version logged one generic "no comments rendered" message
    for every possible cause, which made a comments-disabled video, a
    bot-check interstitial, and an actual selector regression
    indistinguishable in the logs. This checks, in order: running out of
    time budget, a still-open consent dialog, a login/bot-check wall,
    comments explicitly turned off by the uploader, the comments container
    never attaching to the DOM at all (rendering failure), our selectors
    finding nothing despite the page itself reporting a nonzero comment
    count (selector mismatch — a real signal these selectors need
    updating), and finally a genuine absence of public comments.
    """
    if time_left() <= 2:
        return "timeout"

    if _consent_dialog_present(page):
        return "consent_dialog"

    try:
        html = page.content()
    except Exception:
        html = ""
    if html and _LOGIN_WALL_RE.search(html):
        return "login_wall"
    if html and _COMMENTS_DISABLED_RE.search(html):
        return "comments_disabled"

    try:
        has_container = page.locator(_COMMENTS_CONTAINER_SELECTOR).count() > 0
    except Exception:
        has_container = False
    if not has_container:
        return "rendering_failure"

    try:
        header_text = page.locator(
            "ytd-comments-header-renderer #count, "
            "ytd-comments-header-renderer yt-formatted-string"
        ).first.inner_text(timeout=1000)
    except Exception:
        header_text = ""
    if re.search(r"[1-9]\d*", header_text or ""):
        # The page itself claims a nonzero comment count, but none of our
        # known comment-text selectors matched anything inside the
        # container — a strong signal that YouTube changed the comment DOM
        # again rather than that this video truly has no comments.
        return "selector_mismatch"

    return "no_public_comments"


def _scroll_comments_until_idle(page, time_left, remaining_budget: int) -> List[str]:
    """Scroll a video's comments section, collecting text as it loads.

    Keeps scrolling (expanding "Read more" and reply threads along the
    way) until one of:
      * ``remaining_budget`` comments have been located on this video,
      * ``MAX_SCROLL_ITERATIONS`` scroll steps have happened,
      * ``SCROLL_IDLE_LIMIT`` consecutive scrolls produced no new comment
        nodes, or
      * the overall scraper time budget runs low.
    """
    comments: List[str] = []
    if remaining_budget <= 0:
        return comments

    _dismiss_consent(page)

    # Phase 1: Scroll down past the video player to trigger YouTube's
    # Intersection Observer that bootstraps the comment section.
    # YouTube only hydrates comments when #comments enters the viewport;
    # scroll_into_view_if_needed alone is not reliable because the element
    # may not exist yet when we call it. We do a series of timed scrolls
    # first to let the page JS attach the container, then attempt targeting.
    try:
        # Quick scroll past the video description to where comments live
        page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight * 0.3)")
        page.wait_for_timeout(600)
        page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight * 0.5)")
        page.wait_for_timeout(600)
    except Exception:
        pass

    container_found = True
    try:
        page.locator(_COMMENTS_CONTAINER_SELECTOR).first.scroll_into_view_if_needed(timeout=5000)
        # Nudge a little further so the container sits clearly inside the
        # viewport rather than right at its edge — YouTube's lazy comment
        # loader is keyed off actual visibility, and landing exactly on
        # the boundary has been observed to leave it un-triggered.
        page.evaluate("window.scrollBy(0, 400)")
        page.wait_for_timeout(400)
    except Exception:
        container_found = False

    if not container_found:
        # Container hasn't attached yet — blind scrolls to coax the lazy loader
        for step in (1200, 1500, 1800, 2000):
            try:
                page.evaluate(f"window.scrollBy(0, {step})")
            except Exception:
                break
            page.wait_for_timeout(400)
        try:
            page.locator(_COMMENTS_CONTAINER_SELECTOR).first.scroll_into_view_if_needed(timeout=4000)
            page.evaluate("window.scrollBy(0, 400)")
            page.wait_for_timeout(400)
        except Exception:
            pass

    # Phase 2: Wait for at least one comment node to appear. Use a longer
    # timeout here because YouTube's comment loader fires asynchronously
    # after the Intersection Observer triggers — this is the most common
    # place where comments exist but 0 are collected.
    try:
        page.wait_for_selector(_COMMENT_TEXT_SELECTOR, timeout=9000)
    except Exception:
        # One final aggressive scroll attempt before giving up
        try:
            page.evaluate("window.scrollBy(0, 600)")
            page.wait_for_timeout(800)
            page.wait_for_selector(_COMMENT_TEXT_SELECTOR, timeout=4000)
        except Exception:
            reason = _classify_comment_failure(page, time_left)
            logger.info(
                "Comment scroll: no comments collected for this video (reason=%s).",
                reason,
            )
            return comments

    iteration = 0
    idle = 0
    prev_count = -1
    start = time.monotonic()
    while True:
        iteration += 1
        if iteration > MAX_SCROLL_ITERATIONS:
            logger.info("Comment scroll: hit MAX_SCROLL_ITERATIONS (%d).", MAX_SCROLL_ITERATIONS)
            break
        if time_left() <= 2:
            logger.info("Comment scroll: stopping, low on overall time budget.")
            break

        try:
            page.evaluate("window.scrollBy(0, 1400)")
        except Exception:
            break
        _wait_for_more(page, _COMMENT_TEXT_SELECTOR, max(prev_count, 0), max_ms=800)

        if iteration % 3 == 0:
            _expand_comment_extras(page, time_left)

        try:
            count = page.locator(_COMMENT_TEXT_SELECTOR).count()
        except Exception:
            count = -1

        logger.info(
            "Comment scroll: iteration=%d current_count=%d idle_streak=%d elapsed=%.1fs",
            iteration, count, idle, time.monotonic() - start,
        )

        if count >= remaining_budget:
            logger.info("Comment scroll: reached this video's share of the overall target.")
            break
        if count <= prev_count:
            idle += 1
            if idle >= SCROLL_IDLE_LIMIT:
                logger.info(
                    "Comment scroll: no new comments after %d consecutive scrolls; stopping.",
                    SCROLL_IDLE_LIMIT,
                )
                break
        else:
            idle = 0
        prev_count = max(prev_count, count)

    _expand_comment_extras(page, time_left)

    for loc in page.locator(_COMMENT_TEXT_SELECTOR).all()[:remaining_budget]:
        try:
            txt = loc.inner_text().strip()
            if txt and len(txt.split()) >= 2:
                comments.append(txt)
        except Exception:
            pass
    return comments


def _run_sync(company_name: str, videos_url: str) -> List[str]:
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    setup_start = time.monotonic()

    try:
        context = _ensure_context()
    except Exception:
        logger.exception("Could not start/obtain a browser context for %r.", company_name)
        return []

    results: List[str] = []
    seen_keys = set()
    duplicates_removed = 0
    page = None
    try:
        page = context.new_page()
        page.set_default_timeout(12000)

        # The internal time budget clock starts here, only AFTER browser
        # launch, context creation, and page creation have all finished -
        # not before. Starting the clock earlier meant Chromium/context
        # startup time silently ate into the budget before navigation ever
        # got a chance to run, which is exactly what produced "Skipping
        # navigation: out of time budget" before the page had even been
        # opened.
        setup_elapsed = time.monotonic() - setup_start
        start = time.monotonic()
        deadline = start + TIME_BUDGET_SECONDS
        logger.info(
            "YouTube scrape: browser/context/page setup took %.1fs; "
            "starting %ds navigation+scrape budget now.",
            setup_elapsed, TIME_BUDGET_SECONDS,
        )

        def time_left() -> float:
            return deadline - time.monotonic()

        video_urls: List[str] = []
        consent_page_appeared = False
        video_grid_loaded = False
        if _goto_with_retry(page, videos_url, timeout=_adaptive_nav_timeout(time_left), time_left=time_left):
            consent_page_appeared = _consent_dialog_present(page)
            _dismiss_consent(page)
            # Explicitly wait for at least one video tile before scrolling,
            # instead of a fixed sleep beforehand. The previous version
            # went straight into scrolling/collecting behind only a fixed
            # 1500ms sleep, which on a slow-rendering channel page reads 0
            # videos before the grid has actually painted — the same class
            # of readiness bug the comment loop had. This gives slow
            # channels a real chance to render before we give up, without
            # paying a blind sleep on fast ones.
            try:
                page.wait_for_selector(_VIDEO_TILE_WAIT_SELECTOR, timeout=6000)
                video_grid_loaded = True
            except Exception:
                logger.info(
                    "YouTube scrape: no video tiles rendered on %s within 6s "
                    "(reason=%s).", videos_url, _diagnose_blocking_state(page),
                )
            tile_count = 0
            for _ in range(2):
                page.evaluate("window.scrollBy(0, 1200)")
                tile_count = _wait_for_more(page, _VIDEO_TILE_WAIT_SELECTOR, tile_count, max_ms=500)
            video_urls = _collect_video_urls(page, VIDEO_CANDIDATES_TO_COLLECT)

        if not video_urls and time_left() > 4:
            logger.info(
                "YouTube scrape: channel page %s yielded 0 videos — falling back "
                "to a single general YouTube search for %r.",
                videos_url, company_name,
            )
            search_url = (
                f"https://www.youtube.com/results?search_query="
                f"{company_name.replace(' ', '+')}"
            )
            if _goto_with_retry(page, search_url, timeout=_adaptive_nav_timeout(time_left), time_left=time_left):
                _dismiss_consent(page)
                try:
                    page.wait_for_selector(_SEARCH_RESULT_WAIT_SELECTOR, timeout=5000)
                except Exception:
                    logger.info(
                        "YouTube scrape: no search results rendered for %r within "
                        "5s (reason=%s).", company_name, _diagnose_blocking_state(page),
                    )
                video_urls = _collect_video_urls(page, VIDEO_CANDIDATES_TO_COLLECT)
            if not video_urls:
                logger.info(
                    "YouTube scrape: search fallback also yielded 0 videos for %r "
                    "(reason=%s) — reliable YouTube coverage needs an "
                    "authenticated session or the official YouTube Data API.",
                    company_name, _diagnose_blocking_state(page),
                )

        for video_url in video_urls:
            if len(results) >= MAX_TOTAL_COMMENTS or time_left() <= 3:
                break
            remaining_budget = MAX_TOTAL_COMMENTS - len(results)
            try:
                if not _goto_with_retry(page, video_url, timeout=_adaptive_nav_timeout(time_left), time_left=time_left):
                    continue
                video_comments = _scroll_comments_until_idle(page, time_left, remaining_budget)
                before = len(results)
                for c in video_comments:
                    # Normalized (stripped + lowercased) key, matching
                    # normalize_comments()'s own dedup rule exactly, so a
                    # comment isn't double-counted against the running
                    # budget here only to be merged away by that final
                    # pass anyway. A set lookup also replaces the previous
                    # O(n) "c in results" scan with an O(1) check.
                    key = c.strip().lower()
                    if not key or key in seen_keys:
                        duplicates_removed += 1
                        continue
                    seen_keys.add(key)
                    results.append(c)
                logger.info(
                    "YouTube scrape: video=%s contributed %d new comment(s); "
                    "running total=%d/%d.",
                    video_url, len(results) - before, len(results), MAX_TOTAL_COMMENTS,
                )
            except Exception:
                logger.exception("Failed scraping comments for video=%s.", video_url)
                continue

        if not results:
            _save_debug_artifacts(
                page, videos_url, "zero_comments",
                extra={
                    "consent_page_appeared": str(consent_page_appeared),
                    "video_grid_loaded": str(video_grid_loaded),
                    "videos_found": str(len(video_urls)),
                    "diagnosis": _diagnose_blocking_state(page),
                },
            )
    finally:
        if page is not None:
            try:
                page.close()
            except Exception:
                pass

    final = normalize_comments(results)[:MAX_TOTAL_COMMENTS]
    elapsed = time.monotonic() - start
    logger.info(
        "YouTube comments for %r: raw=%d duplicates_removed=%d final=%d "
        "elapsed=%.1fs (cap=%d).",
        company_name, len(results), duplicates_removed, len(final), elapsed,
        MAX_TOTAL_COMMENTS,
    )
    logger.info("YouTube returned %d comments", len(final))
    return final


async def scrape_youtube_comments(company_data: Dict[str, str]) -> List[str]:
    target = (
        company_data.get("youtube_url")
        or company_data.get("youtube")
        or company_data.get("company_name", "")
    )
    if not target:
        return []

    videos_url = _channel_url(target).rstrip("/") + "/videos"
    company_name = company_data.get("company_name", target)

    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(_EXECUTOR, _run_sync, company_name, videos_url)
    except Exception:
        logger.exception("Unhandled error scraping YouTube comments for %r.", company_name)
        return []
