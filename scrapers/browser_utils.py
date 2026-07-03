import asyncio
import concurrent.futures
import sys
from pathlib import Path
from typing import Awaitable, Callable, List, TypeVar

_T = TypeVar("_T")

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
