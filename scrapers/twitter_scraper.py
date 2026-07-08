import asyncio
import concurrent.futures
import logging
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

MAX_TIMELINE_TWEETS = 30
MAX_TWEETS_TO_OPEN = 3          # was 6 — opening tweets is the expensive part (full nav each time)
MAX_REPLIES_PER_TWEET = 10
SCROLL_STEPS = 4                # was 8

_LOGIN_MARKERS = ("/login", "/i/flow/login", "/account/access")


def _profile_url(target: str) -> str:
    target = (target or "").strip()
    if target.startswith(("http://", "https://")):
        return target
    return f"https://x.com/{target.lstrip('@')}"


def _looks_blocked(page) -> bool:
    """Cheap, instant login-wall/redirect check (no waiting)."""
    try:
        url = page.url or ""
    except Exception:
        url = ""
    return any(marker in url for marker in _LOGIN_MARKERS)


async def scrape_twitter_comments(company_data: Dict[str, str]) -> List[str]:
    target = (
        company_data.get("twitter_url")
        or company_data.get("twitter")
        or company_data.get("company_name", "")
    )
    if not target:
        return []

    url = _profile_url(target)

    def _run_in_fresh_loop() -> List[str]:
        def _sync_scrape() -> List[str]:
            from playwright.sync_api import sync_playwright
            results: List[str] = []
            deadline = time.monotonic() + TIME_BUDGET_SECONDS

            def _time_left() -> float:
                return deadline - time.monotonic()

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
                        viewport={"width": 1280, "height": 1800},
                        locale="en-US",
                    )
                    page = ctx.new_page()
                    page.set_default_timeout(7000)

                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=9000)
                    except Exception:
                        browser.close()
                        return normalize_comments(results)

                    # Bail immediately on a login wall instead of waiting it out.
                    if _looks_blocked(page):
                        logger.info(
                            "Twitter/X scrape: redirected to a login wall for %s — "
                            "stopping immediately instead of waiting it out.", url,
                        )
                        browser.close()
                        return normalize_comments(results)

                    got_articles = True
                    try:
                        page.wait_for_selector("article", timeout=6000)
                    except Exception:
                        got_articles = False
                        try:
                            title = page.title()
                        except Exception:
                            title = "<unknown>"
                        logger.info(
                            "Twitter/X scrape: no tweets rendered for %s within 6s "
                            "(page title: %r) — likely a login wall or bot-check "
                            "served to the unauthenticated headless session, not a "
                            "scraper bug. Reliable X coverage needs an authenticated "
                            "session or the official X API.",
                            url, title,
                        )

                    if got_articles and _time_left() > 2:
                        for _ in range(SCROLL_STEPS):
                            if _time_left() <= 2:
                                break
                            page.mouse.wheel(0, 1800)
                            page.wait_for_timeout(500)

                        tweet_links: List[str] = []
                        for article in page.locator("article").all()[:MAX_TIMELINE_TWEETS]:
                            try:
                                text = article.inner_text()
                                if text and len(text.split()) > 4:
                                    results.append(text)
                            except Exception:
                                continue
                            try:
                                href = article.locator("a[href*='/status/']").first.get_attribute("href")
                                if href:
                                    tweet_links.append("https://x.com" + href if href.startswith("/") else href)
                            except Exception:
                                pass

                        seen_links = []
                        for link in tweet_links:
                            if link not in seen_links:
                                seen_links.append(link)

                        for link in seen_links[:MAX_TWEETS_TO_OPEN]:
                            if _time_left() <= 3:
                                break
                            try:
                                page.goto(link, wait_until="domcontentloaded", timeout=7000)
                                page.wait_for_timeout(1000)
                                page.mouse.wheel(0, 1200)
                                page.wait_for_timeout(600)
                                articles = page.locator("article").all()

                                for reply in articles[1:1 + MAX_REPLIES_PER_TWEET]:
                                    try:
                                        text = reply.inner_text()
                                        if text and len(text.split()) > 4:
                                            results.append(text)
                                    except Exception:
                                        continue
                            except Exception:
                                continue

                    browser.close()
            except Exception:
                pass
            return normalize_comments(results)

        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        return _sync_scrape()

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(pool, _run_in_fresh_loop)
