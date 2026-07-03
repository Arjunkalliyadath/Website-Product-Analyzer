import asyncio
<<<<<<< HEAD
import concurrent.futures
import sys
from pathlib import Path
from typing import Awaitable, Callable, List, TypeVar

_T = TypeVar("_T")
=======
from pathlib import Path
from typing import List
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52


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
<<<<<<< HEAD


def _run_playwright_in_fresh_loop(coro_factory: Callable[[], Awaitable[_T]]) -> _T:
    """Runs inside a dedicated worker thread with its own event loop."""
    if sys.platform.startswith("win"):
        # async Playwright needs asyncio.create_subprocess_exec, which only
        # works on the Proactor loop on Windows.
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro_factory())
    finally:
        loop.close()


async def run_playwright_async(coro_factory: Callable[[], Awaitable[_T]]) -> _T:
    """
    Run an async-Playwright coroutine safely from inside FastAPI/uvicorn's
    event loop, regardless of platform.

    A fresh event loop in a dedicated worker thread avoids clashing with
    whatever loop policy the host server is already using, which is what
    was previously causing async Playwright calls (e.g. the social-link
    fallback in company_discovery.py) to fail silently.
    """
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(
            pool, _run_playwright_in_fresh_loop, coro_factory
        )
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
