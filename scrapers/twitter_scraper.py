import asyncio
import concurrent.futures
<<<<<<< HEAD
import logging
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
import sys
from typing import Dict, List

from scrapers.browser_utils import normalize_comments

<<<<<<< HEAD
logger = logging.getLogger(__name__)

=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
# Tunables — raise/lower these if you want more or fewer results per run.
MAX_TIMELINE_TWEETS = 30     # tweets/replies pulled straight off the profile
MAX_TWEETS_TO_OPEN = 6        # how many of those tweets we open to read replies
MAX_REPLIES_PER_TWEET = 10    # replies collected per opened tweet
SCROLL_STEPS = 8              # how many times we scroll the timeline to lazy-load more


def _profile_url(target: str) -> str:
    target = (target or "").strip()
    if target.startswith(("http://", "https://")):
        return target
    return f"https://x.com/{target.lstrip('@')}"


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
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-setuid-sandbox",
                              "--disable-dev-shm-usage"],
                    )
                    # A real UA + viewport makes X far less likely to serve a
                    # stripped-down/blocked page than an unconfigured context.
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
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_selector("article", timeout=10000)
                    except Exception:
<<<<<<< HEAD
                        # DIAGNOSTIC (not a fix) — if X never renders a
                        # single <article> (tweet) within 10s, it almost
                        # always means X served a login-wall / "something
                        # went wrong, retry" page to this unauthenticated
                        # headless session rather than the real timeline.
                        # Logged so it's visible in the terminal instead of
                        # silently returning 0 comments with no explanation.
                        try:
                            title = page.title()
                        except Exception:
                            title = "<unknown>"
                        logger.info(
                            "Twitter/X scrape: no tweets rendered for %s within 10s "
                            "(page title: %r) — likely a login wall or bot-check "
                            "served to the unauthenticated headless session, not a "
                            "scraper bug. Reliable X coverage needs an authenticated "
                            "session or the official X API.",
                            url, title,
                        )
=======
                        pass
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52

                    # Scroll repeatedly so more than the first screenful of
                    # tweets gets lazy-loaded into the DOM.
                    for _ in range(SCROLL_STEPS):
                        page.mouse.wheel(0, 1800)
                        page.wait_for_timeout(900)

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

                    # Open a handful of individual tweets to pull in genuine
                    # audience replies (the profile timeline alone is mostly
                    # the brand's own posts, not customer comments).
                    seen_links = []
                    for link in tweet_links:
                        if link not in seen_links:
                            seen_links.append(link)
                    for link in seen_links[:MAX_TWEETS_TO_OPEN]:
                        try:
                            page.goto(link, wait_until="domcontentloaded", timeout=20000)
                            page.wait_for_timeout(2000)
                            page.mouse.wheel(0, 1200)
                            page.wait_for_timeout(1200)
                            articles = page.locator("article").all()
                            # Skip the first article (it's the original tweet,
                            # already captured above) — the rest are replies.
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
