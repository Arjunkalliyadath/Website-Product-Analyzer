"""
Google Reviews scraper — improved selector coverage and Maps navigation.
"""

import asyncio
import concurrent.futures
import re
import sys
from typing import Dict, List

from scrapers.browser_utils import normalize_comments

MAX_RESULTS = 30

_AGO_RE = re.compile(
    r"\b(?:\d+|a|an)\s+(?:second|minute|hour|day|week|month|year)s?\s+ago\b",
    re.IGNORECASE,
)

<<<<<<< HEAD
# BUG FIX — the generic SERP snippet selectors (div.VwiC3b, span.aCOpRe)
# return the text of ANY Google search result snippet, not specifically
# reviews. For less-common product queries this sometimes surfaces a social
# profile's bio blurb instead — e.g. "163K followers · 1.7K+ posts ·
# Purveyors of the World's finest headphones..." — which would otherwise
# get stored and displayed as if it were a genuine customer review.
_BIO_STATS_RE = re.compile(
    r"\d[\d,.]*\s*[kKmM]?\+?\s*(followers|following|posts|likes|subscribers)\b",
    re.IGNORECASE,
)


def _looks_like_review(text: str) -> bool:
    """Reject social-profile-bio snippets picked up by the SERP fallback."""
    # Two or more "163K followers" / "1.7K+ posts" style stat mentions in
    # one snippet is a strong signal it's a bio, not a review.
    if len(_BIO_STATS_RE.findall(text)) >= 2:
        return False
    return True

=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52

def _strip_review_boilerplate(text: str) -> str:
    t = text
    m = _AGO_RE.search(t)
    if m:
        t = t[m.end():]
    t = re.split(r"\.\.\.\s*More\b", t)[0]
    t = re.split(r"\bLike\b\s*\bShare\b", t)[0]
    t = re.sub(r"Response from the owner.*$", "", t, flags=re.IGNORECASE | re.DOTALL)
    t = re.sub(r"\bShare\b\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .|\u2022")
    return t


async def scrape_google_reviews(company_data: Dict[str, str]) -> List[str]:
    query = company_data.get("company_name", "")
    if not query:
        return []

    def _run() -> List[str]:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        from playwright.sync_api import sync_playwright
        raw_results: List[str] = []

        review_selectors = [
            "span.wiI7pd",            # Maps review body (clean)
            "div.jftiEf",             # Maps review card container
            "div[data-review-id]",
            "span.review-full-text",
            "div.review-snippet",
            "div.gws-localreviews__google-review",
        ]
        snippet_selectors = [
            "div.VwiC3b",
            "span.aCOpRe",
            "div.lyLwlc",
            "span.MUxGbd",
        ]

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-dev-shm-usage",
                          "--disable-blink-features=AutomationControlled"],
                )
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    locale="en-US",
                    viewport={"width": 1280, "height": 900},
                )
                page = ctx.new_page()

                # --- Attempt 1: Google Maps ---
                try:
                    maps_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
                    page.goto(maps_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(3000)

                    # Click first result
                    for selector in ["a.hfpxzc", "div[role='article']", "div.Nv2PK"]:
                        try:
                            first = page.locator(selector).first
                            if first.count() > 0:
                                first.click()
                                page.wait_for_timeout(2500)
                                break
                        except Exception:
                            pass

                    # Click Reviews tab
                    for tab_name in ["Reviews", "Review"]:
                        try:
                            tab = page.get_by_role("tab", name=tab_name)
                            if tab.count() > 0:
                                tab.first.click()
                                page.wait_for_timeout(2000)
                                break
                        except Exception:
                            pass

                    # Scroll to load more reviews
                    for _ in range(6):
                        page.mouse.wheel(0, 800)
                        page.wait_for_timeout(600)

                    # Expand "More" links
                    try:
                        for more_btn in page.locator("button[aria-label*='more']").all()[:10]:
                            try:
                                more_btn.click()
                                page.wait_for_timeout(300)
                            except Exception:
                                pass
                    except Exception:
                        pass

                    for sel in review_selectors:
                        for loc in page.locator(sel).all()[:25]:
                            try:
                                txt = loc.inner_text().strip()
                                if txt:
                                    raw_results.append(txt)
                            except Exception:
                                pass
                except Exception:
                    pass

                # --- Attempt 2: SERP "reviews" search ---
                if len(raw_results) < MAX_RESULTS:
                    for search_q in [
                        f"{query} reviews",
                        f"{query} customer reviews",
                        f'"{query}" review site:google.com OR site:trustpilot.com OR site:g2.com',
                    ]:
                        try:
                            url = f"https://www.google.com/search?q={search_q.replace(' ', '+')}&hl=en&gl=in"
                            page.goto(url, wait_until="domcontentloaded", timeout=25000)
                            page.wait_for_timeout(2500)
                            for sel in review_selectors + snippet_selectors:
                                for loc in page.locator(sel).all()[:20]:
                                    try:
                                        txt = loc.inner_text().strip()
                                        if txt:
                                            raw_results.append(txt)
                                    except Exception:
                                        pass
                        except Exception:
                            continue

                # --- Attempt 3: Trustpilot or similar ---
                if len(raw_results) < 5:
                    try:
                        search_q = f"{query} site:trustpilot.com OR site:g2.com reviews"
                        page.goto(
                            f"https://www.bing.com/search?q={search_q.replace(' ', '+')}",
                            wait_until="domcontentloaded", timeout=20000,
                        )
                        page.wait_for_timeout(2000)
                        for sel in ["p.b_algoSlug", "div.b_caption p"]:
                            for loc in page.locator(sel).all()[:20]:
                                try:
                                    txt = loc.inner_text().strip()
                                    if txt and len(txt.split()) >= 6:
                                        raw_results.append(txt)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                browser.close()
        except Exception:
            pass

        cleaned: List[str] = []
        for raw in raw_results:
            stripped = _strip_review_boilerplate(raw)
<<<<<<< HEAD
            if stripped and len(stripped.split()) >= 5 and _looks_like_review(stripped):
=======
            if stripped and len(stripped.split()) >= 5:
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
                cleaned.append(stripped)

        return normalize_comments(cleaned)[:MAX_RESULTS]

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(pool, _run)
