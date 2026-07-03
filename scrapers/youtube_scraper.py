"""
YouTube comments scraper.

Strategy:
1. Go to the channel's /videos page and collect links to several recent
   videos — not just the first one, since a single video's comments may be
   off, sparse, or slow to load.
2. Video discovery uses two layers: CSS selectors first, then a regex sweep
   over the raw page HTML for embedded "videoId" fields. YouTube's grid
   markup/class names change across redesigns, but the videoId values in
   its internal JSON payload are far more stable, so the regex sweep is
   what actually finds videos when the CSS selectors miss (which is what
   was happening before — only 1-2 videos were ever found).
3. For each video, dismiss the cookie-consent dialog if present (it can
   block scrolling/clicking), scroll the comments section into view, then
   scroll further to lazy-load comments and collect the top-level text.
4. Aggregate across all videos visited until we hit the overall cap.
5. If the channel page gives no videos at all, fall back to a direct
   YouTube search for the company and pull videos from those results.
"""

import asyncio
import concurrent.futures
<<<<<<< HEAD
import logging
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
import re
import sys
from typing import Dict, List

from scrapers.browser_utils import normalize_comments

<<<<<<< HEAD
logger = logging.getLogger(__name__)

=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
# Tunables
VIDEOS_TO_VISIT = 6            # how many videos we pull comments from
COMMENTS_PER_VIDEO = 20        # cap per video
MAX_TOTAL_COMMENTS = 60        # overall cap across all videos
SCROLL_STEPS = 10              # scroll iterations per video to load comments

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
                btn.first.click(timeout=1500)
                page.wait_for_timeout(600)
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
    # CSS-selector pass first (works when current markup matches).
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
    # Regex pass over the raw page HTML — robust to markup/class changes,
    # since it reads YouTube's embedded JSON data directly.
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

        def _scrape_comments_on_current_page(page) -> List[str]:
            comments: List[str] = []
            _dismiss_consent(page)
            try:
                page.locator("ytd-comments, #comments").first.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass
            for _ in range(SCROLL_STEPS):
                page.evaluate("window.scrollBy(0, 800)")
                page.wait_for_timeout(1000)
            try:
                page.wait_for_selector("#content-text", timeout=12000)
            except Exception:
                return comments
            for _ in range(3):
                page.evaluate("window.scrollBy(0, 1200)")
                page.wait_for_timeout(900)
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

                # --- Gather candidate video URLs from the channel ---
                video_urls: List[str] = []
                try:
                    page.goto(videos_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(2500)
                    _dismiss_consent(page)
                    for _ in range(3):
                        page.evaluate("window.scrollBy(0, 1200)")
                        page.wait_for_timeout(800)
                    video_urls = _collect_video_urls(page, VIDEOS_TO_VISIT)
                except Exception:
                    pass

                # --- Fallback: search results if the channel page gave nothing ---
                if not video_urls:
<<<<<<< HEAD
                    logger.info(
                        "YouTube scrape: channel page %s yielded 0 videos — "
                        "falling back to a general YouTube search for %r.",
                        videos_url, company_name,
                    )
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
                    try:
                        search_url = (
                            f"https://www.youtube.com/results?search_query="
                            f"{company_name.replace(' ', '+')}"
                        )
                        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(2000)
                        _dismiss_consent(page)
                        video_urls = _collect_video_urls(page, VIDEOS_TO_VISIT)
                    except Exception:
                        pass
<<<<<<< HEAD
                    if not video_urls:
                        # DIAGNOSTIC (not a fix) — if even a general search
                        # yields nothing, it's almost always YouTube serving
                        # a "confirm you're not a bot" / consent interstitial
                        # to this unauthenticated headless session (common
                        # for datacenter/cloud IPs), not an absence of
                        # videos. Logged so it's visible instead of silently
                        # returning 0 comments with no explanation.
                        logger.info(
                            "YouTube scrape: search fallback also yielded 0 videos "
                            "for %r — likely a bot-check/consent interstitial served "
                            "to this headless session rather than a real absence of "
                            "videos. Reliable YouTube coverage needs an authenticated "
                            "session or the official YouTube Data API.",
                            company_name,
                        )
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52

                # --- Visit each candidate video and collect comments ---
                for video_url in video_urls:
                    if len(results) >= MAX_TOTAL_COMMENTS:
                        break
                    try:
                        page.goto(video_url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(2500)
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
