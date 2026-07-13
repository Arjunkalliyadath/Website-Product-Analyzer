import asyncio
import concurrent.futures
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Awaitable, Callable, Iterator, List, TypeVar

_T = TypeVar("_T")

# ---------------------------------------------------------------------------
# Cross-module browser-launch throttle
# ---------------------------------------------------------------------------
# google_scraper.py / youtube_scraper.py / twitter_scraper.py /
# instagram_scraper.py each maintain their own independent worker pool and
# their own thread-local browser (see each module's `_ensure_context()`).
# That per-module pooling/reuse logic is unchanged by this addition.
#
# What was missing is any coordination *across* those four modules. Because
# app.py fires Google/Twitter/Instagram/YouTube from the same asyncio.gather(),
# a cold start (or a simultaneous recycle) can launch several Chromium
# processes in the very same instant - observed as 6 concurrent launches,
# each taking ~16s instead of the normal ~2-4s, because they're all
# contending for the same CPU cores at once.
#
# `browser_launch_slot()` is a small, shared gate that every module wraps
# around its own (otherwise-unchanged) `pw.chromium.launch(...)` call. It
# only limits how many Chromium processes may be *launching* at the same
# moment; it has no effect on an already-warm thread reusing its existing
# context (that code path returns before ever reaching the launch call), and
# no effect on how many browsers may be open at once (still each module's
# own MAX_BROWSER_WORKERS). This is purely a launch-time stagger to reduce
# CPU contention during simultaneous cold starts.
MAX_CONCURRENT_BROWSER_LAUNCHES = 2

_browser_launch_semaphore = threading.Semaphore(MAX_CONCURRENT_BROWSER_LAUNCHES)


@contextmanager
def browser_launch_slot() -> Iterator[None]:
    """Acquire one of the shared browser-launch slots.

    Usage (inside a scraper module's `_ensure_context()`, wrapping only the
    existing launch line):

        with browser_launch_slot():
            browser = pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)

    Blocks (on the calling worker thread only - never the asyncio event
    loop, since this is always invoked from inside a ThreadPoolExecutor
    worker) until a launch slot is free, then releases it as soon as
    `chromium.launch()` returns.
    """
    _browser_launch_semaphore.acquire()
    try:
        yield
    finally:
        _browser_launch_semaphore.release()

def get_storage_dir() -> Path:
    path = Path("downloads") / ".playwright"
    path.mkdir(parents=True, exist_ok=True)
    return path

def normalize_comments(items: List[str]) -> List[str]:
    cleaned = []
    seen = set()
    for item in items:
        text = (item or "").strip()
        if not text or len(text) < 3:
            continue
        if "http" in text.lower():
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        cleaned.append(text)
    return cleaned

def _run_playwright_in_fresh_loop(coro_factory: Callable[[], Awaitable[_T]]) -> _T:

    if sys.platform.startswith("win"):

        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro_factory())
    finally:
        loop.close()

async def run_playwright_async(coro_factory: Callable[[], Awaitable[_T]]) -> _T:

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(
            pool, _run_playwright_in_fresh_loop, coro_factory
        )
