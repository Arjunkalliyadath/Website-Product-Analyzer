import asyncio
import concurrent.futures
import sys
import time
from typing import Dict, List

from scrapers.browser_utils import normalize_comments

MAX_RESULTS = 30
MIN_RESULTS_BEFORE_FALLBACK = 5

# --- Hard internal time budget -----------------------------------------
# Kept a few seconds under the outer asyncio.wait_for() cap applied in
# app.py (20s) so this scraper almost always returns on its own, with
# whatever it has collected so far, instead of being cut off cold by the
# outer timeout and losing partial results.
TIME_BUDGET_SECONDS = 16

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


def _looks_blocked(page) -> bool:
    """Cheap, instant login-wall/checkpoint check (no waiting)."""
    try:
        url = page.url or ""
    except Exception:
        url = ""
    return "login" in url or "accounts" in url or "challenge" in url


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

    def _run() -> List[str]:
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        from playwright.sync_api import sync_playwright

        results: List[str] = []
        deadline = time.monotonic() + TIME_BUDGET_SECONDS

        def _time_left() -> float:
            return deadline - time.monotonic()

        def _add(txt: str) -> None:
            txt = (txt or "").strip()
            if txt and len(txt.split()) >= 6 and not _is_non_review(txt):
                if txt not in results:
                    results.append(txt)

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
                page.set_default_timeout(7000)

                # --- Primary attempt: public embed page (no login wall) ----
                try:
                    page.goto(profile_url.rstrip("/") + "/embed/",
                               wait_until="domcontentloaded", timeout=8000)
                    page.wait_for_timeout(1200)
                    for loc in page.locator("[class*='Caption'], [class*='caption']").all()[:30]:
                        try:
                            _add(loc.inner_text())
                        except Exception:
                            pass
                except Exception:
                    pass

                # --- One fallback: direct profile page, skipped instantly if
                # it redirects to a login/checkpoint wall --------------------
                if len(results) < MIN_RESULTS_BEFORE_FALLBACK and _time_left() > 4:
                    try:
                        page.goto(profile_url, wait_until="domcontentloaded", timeout=8000)
                        page.wait_for_timeout(1200)
                        if not _looks_blocked(page):
                            for sel in ["span._aacl", "div._aacl", "h1", "span"]:
                                for loc in page.locator(sel).all()[:50]:
                                    try:
                                        txt = loc.inner_text()
                                        if txt and len(txt.split()) >= 6:
                                            _add(txt)
                                    except Exception:
                                        pass
                    except Exception:
                        pass

                # --- One search fallback (single query), only if still short
                # on data and there's time left ------------------------------
                if len(results) < MIN_RESULTS_BEFORE_FALLBACK and _time_left() > 4:
                    try:
                        q = f'site:instagram.com "{company_name}"'
                        page.goto(
                            f"https://www.google.com/search?q={q.replace(' ', '+')}&hl=en&num=20",
                            wait_until="domcontentloaded", timeout=8000,
                        )
                        page.wait_for_timeout(1200)
                        for sel in ["div.VwiC3b", "span.aCOpRe", "div.IsZvec",
                                    "div.lyLwlc", "span.MUxGbd"]:
                            for loc in page.locator(sel).all()[:25]:
                                try:
                                    _add(loc.inner_text())
                                except Exception:
                                    pass
                    except Exception:
                        pass

                browser.close()
        except Exception:
            pass

        return normalize_comments(results)[:MAX_RESULTS]

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return await loop.run_in_executor(pool, _run)
