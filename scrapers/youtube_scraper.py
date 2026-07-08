import asyncio
import concurrent.futures
import logging
import re
import sys
import time
from typing import Dict, List

from scrapers.browser_utils import normalize_comments

logger = logging.getLogger(__name__)

# --- Hard internal time budget -----------------------------------------
# Kept a few seconds under the outer asyncio.wait_for() cap applied in
# app.py (20s) so this scraper almost always returns on its own, with
# whatever it has collected so far, instead of being cut off cold by the
# outer timeout and losing partial results.
TIME_BUDGET_SECONDS = 16

VIDEOS_TO_VISIT = 3        # was 6 — each visit is a full page nav + comment load
COMMENTS_PER_VIDEO = 20
MAX_TOTAL_COMMENTS = 60
SCROLL_STEPS = 4           # was 10

_VIDEO_ID_RE = re.compile(r'"videoId":"([\w-]{11})"')
_CONSENT_LABELS = ("Accept all", "I agree", "Accept the use of cookies", "Reject all")


def _channel_url(target: str) -> str:
    target = (target or "").strip()
    if target.startswith(("http://", "https://")):
        return target
    if target.startswith(("channel/", "user/", "c/")):
        return f"https://www.youtube.com/{target}"
    return f"https://www.youtube.com/@{target.lstrip('@')}"


def _dismiss_consent(page) -> None:
    for label in _CONSENT_LABELS:
        try:
            btn = page.get_by_role("button", name=label)
            if btn.count() > 0:
                btn.first.click(timeout=1200)
                page.wait_for_timeout(400)
                return
        except Exception:
            continue


def _extract_video_urls_from_html(html: str, limit: int) -> List[str]:
    seen: List[str] = []
    for vid in _VIDEO_ID_RE.findall(html):
        url = f"https://www.youtube.com/watch?v={vid}"
        if url not in seen:
            seen.append(url)
        if len(seen) >= limit:
            break
    return seen


def _collect_video_urls(page, limit: int) -> List[str]:
    urls: List[str] = []

    for sel in ("a#video-title", "a#video-title-link"):
        for loc in page.locator(sel).all()[:limit]:
            try:
                href = loc.get_attribute("href")
                if href and "watch" in href:
                    full = "https://www.youtube.com" + href if href.startswith("/") else href
                    if full not in urls:
                        urls.append(full)
            except Exception:
                pass
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
    return urls[:limit]


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

    def _run() -> List[str]:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        from playwright.sync_api import sync_playwright

        deadline = time.monotonic() + TIME_BUDGET_SECONDS

        def _time_left() -> float:
            return deadline - time.monotonic()

        def _scrape_comments_on_current_page(page) -> List[str]:
            comments: List[str] = []
            _dismiss_consent(page)
            try:
                page.locator("ytd-comments, #comments").first.scroll_into_view_if_needed(timeout=4000)
            except Exception:
                pass
            for _ in range(SCROLL_STEPS):
                if _time_left() <= 2:
                    return comments
                page.evaluate("window.scrollBy(0, 900)")
                page.wait_for_timeout(600)
            try:
                page.wait_for_selector("#content-text", timeout=6000)
            except Exception:
                # Likely a consent/bot-check interstitial rather than a real
                # absence of comments — stop on this video instead of waiting.
                return comments
            for _ in range(2):
                if _time_left() <= 1:
                    break
                page.evaluate("window.scrollBy(0, 1200)")
                page.wait_for_timeout(500)
            for loc in page.locator("#content-text").all()[:COMMENTS_PER_VIDEO]:
                try:
                    txt = loc.inner_text().strip()
                    if txt and len(txt.split()) >= 4:
                        comments.append(txt)
                except Exception:
                    pass
            return comments

        results: List[str] = []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage"],
                )
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    locale="en-US",
                )
                page = ctx.new_page()
                page.set_default_timeout(7000)

                video_urls: List[str] = []
                try:
                    page.goto(videos_url, wait_until="domcontentloaded", timeout=9000)
                    page.wait_for_timeout(1500)
                    _dismiss_consent(page)
                    for _ in range(2):
                        page.evaluate("window.scrollBy(0, 1200)")
                        page.wait_for_timeout(500)
                    video_urls = _collect_video_urls(page, VIDEOS_TO_VISIT)
                except Exception:
                    pass

                # One fallback only: a general search, then stop.
                if not video_urls and _time_left() > 4:
                    logger.info(
                        "YouTube scrape: channel page %s yielded 0 videos — "
                        "falling back to a single general YouTube search for %r.",
                        videos_url, company_name,
                    )
                    try:
                        search_url = (
                            f"https://www.youtube.com/results?search_query="
                            f"{company_name.replace(' ', '+')}"
                        )
                        page.goto(search_url, wait_until="domcontentloaded", timeout=9000)
                        page.wait_for_timeout(1200)
                        _dismiss_consent(page)
                        video_urls = _collect_video_urls(page, VIDEOS_TO_VISIT)
                    except Exception:
                        pass
                    if not video_urls:
                        logger.info(
                            "YouTube scrape: search fallback also yielded 0 videos "
                            "for %r — likely a bot-check/consent interstitial served "
                            "to this headless session rather than a real absence of "
                            "videos. Reliable YouTube coverage needs an authenticated "
                            "session or the official YouTube Data API.",
                            company_name,
                        )

                for video_url in video_urls:
                    if len(results) >= MAX_TOTAL_COMMENTS or _time_left() <= 3:
                        break
                    try:
                        page.goto(video_url, wait_until="domcontentloaded", timeout=8000)
                        page.wait_for_timeout(1500)
                        results.extend(_scrape_comments_on_current_page(page))
                    except Exception:
                        continue

                browser.close()
        except Exception:
            pass

        return normalize_comments(results)[:MAX_TOTAL_COMMENTS]

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(pool, _run)
