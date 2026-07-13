"""Reddit discussion-comment scraper.

============================================================================
Position in the pipeline
----------------------------------------------------------------------------
Website -> Product Discovery -> ... -> Google Reviews -> Reddit -> YouTube
Reviews -> Twitter -> Instagram -> Sentiment Analysis -> ...

This module is a sibling of google_scraper.py / youtube_scraper.py /
twitter_scraper.py / instagram_scraper.py: it is called from app.py with a
per-job ``company_data`` dict and returns a flat ``List[str]`` of clean
comment text, exactly like every other scraper. It never touches sentiment
analysis, aspect intelligence, buying recommendations, the dashboard, or
the PDF report - those stages already consume whatever platform comment
lists app.py hands them, generically, by platform name.
----------------------------------------------------------------------------

Unlike Google Maps and YouTube, Reddit publishes a public, unauthenticated
JSON view of its search results and comment threads (append ``.json`` to
almost any reddit.com URL). That means this scraper does not need
Playwright/browser automation - a couple of plain HTTP GETs against
reddit.com's own JSON endpoints (with a descriptive User-Agent, as Reddit's
API etiquette asks for even for anonymous requests) is enough. The sync
HTTP calls are still run inside a small dedicated ThreadPoolExecutor rather
than directly on the event loop, mirroring exactly how google_scraper.py /
youtube_scraper.py run their (heavier) sync Playwright work - so this
module plugs into app.py's existing ``await scrape_x(...)`` call sites
without needing any special-casing.

----------------------------------------------------------------------------
Product-centric search priority (only Priority 1 is used for the "General"
company-wide job, which has no product_name - see _build_search_tiers):

  1. Product Name              e.g. "Tangzu Wan'er"
  2. Brand + Product            e.g. "Tangzu Wan'er Reddit"
     (or "<brand> <product>" when a distinct product_brand is supplied and
     isn't already part of the product name)
  3. Company + Product          e.g. "Headphone Zone Tangzu Wan'er"

A tier is only treated as "successful" - stopping the search - once its
candidate posts have actually been visited and yielded at least one usable
comment, not merely once a search returns candidate posts. This mirrors
the tier-fallback logic in youtube_scraper.py.
----------------------------------------------------------------------------

Discussion-post filtering ("ignore News / Advertisements / Image posts /
Videos"):
  * Reddit's own search operator ``self:yes`` restricts results to native
    text ("self") posts server-side - this alone removes essentially all
    link/image/video posts, since those aren't self posts.
  * Defense-in-depth client-side checks additionally drop anything flagged
    as a video, gallery, or externally-linked post, anything stickied or
    NSFW, and anything whose flair/title reads as news or an advertisement.
============================================================================
"""

import asyncio
import concurrent.futures
import html
import json
import logging
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urlencode

from scrapers.browser_utils import normalize_comments

logger = logging.getLogger(__name__)

# --- Concurrency ------------------------------------------------------------
# Small worker pool, kept deliberately modest (unlike the browser pools in
# google_scraper.py / youtube_scraper.py) since this scraper is plain HTTP
# against Reddit's public JSON endpoints, which rate-limit unauthenticated
# clients fairly aggressively. Exported so app.py can size its per-platform
# semaphore to match, exactly like it does today with youtube_scraper's
# MAX_BROWSER_WORKERS.
MAX_REDDIT_WORKERS = 2

_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=MAX_REDDIT_WORKERS, thread_name_prefix="reddit_scraper"
)

# --- Hard internal time budget -----------------------------------------
# Kept a few seconds under the outer asyncio.wait_for() cap applied per job
# in app.py (20s - see REDDIT_TIMEOUT_SECONDS there) so this scraper almost
# always returns on its own, with whatever it has collected so far, instead
# of being cut off cold by the outer timeout and losing partial results.
TIME_BUDGET_SECONDS = 16
_MIN_TIME_FOR_ANOTHER_TIER_SECONDS = 3

# --- Volume / politeness caps -------------------------------------------
MAX_CANDIDATE_POSTS_PER_TIER = 6   # discussion posts inspected per search tier
MAX_COMMENTS_PER_POST = 15         # top-level comments kept from any one post
MAX_TOTAL_COMMENTS = 60            # overall cap per scrape_reddit_comments() call
_REQUEST_DELAY_SECONDS = 0.4       # brief pause between successive Reddit requests

# Reddit asks even anonymous/unauthenticated clients to identify themselves
# with a descriptive User-Agent; generic ones are the fastest way to get
# soft-throttled.
_USER_AGENT = "ManobhavaAI/1.0 (product review discussion scraper; contact: support@manobhava.ai)"
_HEADERS = {"User-Agent": _USER_AGENT, "Accept": "application/json"}

# --- Source fallback chain --------------------------------------------------
# www.reddit.com/search.json returned a blanket HTTP 403 on *every single*
# query in production (see the log: identical 403 + identical Reddit
# "theme-beta" interstitial body, on the very first request of the process,
# regardless of query content). That signature is an edge/IP-reputation
# block on the www.reddit.com front-end itself, not a fixable per-request
# header/query tweak - so instead of retrying the same blocked endpoint,
# each tier now tries a short list of Reddit's *other* public JSON
# front-ends, in order, stopping at the first one that responds. This is a
# source fallback (per the brief: "if one source fails, automatically fall
# back to another"), not a retry of the same request - each domain is
# fetched at most once per call.
#   1. old.reddit.com  - the legacy front-end; served from different edge
#      infrastructure than www, and historically the least aggressively
#      gated for anonymous JSON requests.
#   2. www.reddit.com  - kept last rather than dropped: the block observed
#      in this log may be a temporary IP-reputation flag rather than a
#      permanent one, so it's still worth one attempt.
_SEARCH_DOMAINS = ["old.reddit.com", "www.reddit.com"]


def _search_url(domain: str) -> str:
    return f"https://{domain}/search.json"


def _comments_url(domain: str, subreddit: str, post_id: str) -> str:
    return (
        f"https://{domain}/r/{subreddit}/comments/{post_id}.json"
        f"?{urlencode({'limit': 100, 'sort': 'top', 'depth': 1})}"
    )


# --- Discussion-post filtering ----------------------------------------------
_EXCLUDED_FLAIR_RE = re.compile(
    r"\b(news|advertisement|advertising|promo|promotional|sponsored|announcement)\b",
    re.IGNORECASE,
)
_EXCLUDED_TITLE_RE = re.compile(
    r"\[(?:ad|advertisement|promo|sponsored)\]|\((?:ad|advertisement|promo|sponsored)\)",
    re.IGNORECASE,
)
_EXCLUDED_POST_HINTS = {"image", "hosted:video", "rich:video", "link"}


def _is_discussion_post(post: Dict) -> bool:
    """True only for genuine text-discussion posts - never news, ads,
    images, videos, or galleries."""
    if post.get("stickied") or post.get("over_18") or post.get("pinned"):
        return False
    if not post.get("is_self", False):
        return False
    if post.get("is_video") or post.get("is_gallery"):
        return False
    hint = (post.get("post_hint") or "").lower()
    if hint in _EXCLUDED_POST_HINTS:
        return False
    domain = (post.get("domain") or "").lower()
    if not domain.startswith("self."):
        return False
    if not post.get("num_comments"):
        return False
    flair = post.get("link_flair_text") or ""
    if _EXCLUDED_FLAIR_RE.search(flair):
        return False
    title = post.get("title") or ""
    if _EXCLUDED_TITLE_RE.search(title):
        return False
    return True


# --- Comment cleaning ---------------------------------------------------
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((?:[^)]+)\)")
_URL_RE = re.compile(r"https?://\S+")
_MD_EMPHASIS_RE = re.compile(r"[*_~`^]+")
_QUOTE_MARKER_RE = re.compile(r"^\s*>+\s?", re.MULTILINE)
_WHITESPACE_RE = re.compile(r"\s+")

_EXCLUDED_AUTHORS = {"automoderator", "[deleted]"}
_REMOVED_BODIES = {"[deleted]", "[removed]"}


def _clean_reddit_markdown(text: str) -> str:
    """Strips Reddit-specific markdown/HTML-entity noise. Generic cleanup
    (link stripping, whitespace normalization) is left to the shared
    clean_comment/remove_links/normalize_text pipeline already applied to
    every platform's comments in app.py, so this stays scoped to syntax
    that's specific to Reddit's comment markdown."""
    text = html.unescape(text)
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _URL_RE.sub("", text)
    text = _MD_EMPHASIS_RE.sub("", text)
    text = _QUOTE_MARKER_RE.sub("", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


@dataclass
class RedditComment:
    """Internal record kept for filtering/logging. Only ``comment_text`` is
    surfaced by scrape_reddit_comments() - the sentiment pipeline is never
    changed to accept anything richer than the List[str] it already gets
    from every other platform."""
    post_title: str
    comment_text: str
    author: str
    upvotes: int


# --- HTTP -----------------------------------------------------------------
def _get_json(url: str, timeout: float, retries: int = 1) -> Optional[Dict]:
    request = urllib.request.Request(url, headers=_HEADERS)
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(request, timeout=max(2.0, timeout)) as resp:
                raw = resp.read()
            return json.loads(raw.decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < retries:
                logger.warning("Reddit rate-limited us (429); backing off before retry: %s", url)
                time.sleep(1.5)
                attempt += 1
                continue
            # Diagnostic-only addition: a 403 (as opposed to 429) that's
            # consistent across every query variant, on the very first
            # request of the process, is the signature of an anti-bot /
            # IP-reputation block rather than a code-level bug - this is
            # not fixable from in-code header/retry tweaks alone. Logging
            # the response body (if any) here is purely additive so the
            # next zero-Reddit-results run shows Reddit's actual block
            # page/message instead of just the bare status code, without
            # changing any control flow or return value below.
            try:
                body_preview = exc.read(500).decode("utf-8", errors="ignore")
            except Exception:
                body_preview = ""
            logger.warning(
                "Reddit HTTP error %s for %s%s",
                exc.code, url,
                f" | body preview: {body_preview!r}" if body_preview else "",
            )
            return None
        except Exception:
            logger.warning("Reddit request failed for %s", url, exc_info=True)
            return None


def _search_reddit(query: str, time_left: float) -> "tuple[List[Dict], str]":
    """Search Reddit for discussion posts matching ``query``. Restricted to
    self (text) posts via Reddit's own ``self:yes`` search operator, with
    client-side filtering (_is_discussion_post) as a defense-in-depth
    second pass.

    Tries each domain in _SEARCH_DOMAINS in turn, stopping at the first
    one that returns a usable payload (a source fallback, not a retry -
    see the _SEARCH_DOMAINS comment above). Returns (posts, domain) so the
    caller can reuse the same working domain for the comment-thread
    fetches that follow, instead of re-discovering it per post.
    """
    params = {
        "q": f"{query} self:yes",
        "sort": "relevance",
        "t": "all",
        "limit": 25,
    }
    for domain in _SEARCH_DOMAINS:
        if time_left <= 1:
            break
        url = f"{_search_url(domain)}?{urlencode(params)}"
        payload = _get_json(url, time_left)
        if not payload:
            continue
        children = payload.get("data", {}).get("children", []) or []
        posts = [c.get("data", {}) for c in children if c.get("kind") == "t3"]
        discussion_posts = [p for p in posts if _is_discussion_post(p)]
        if not discussion_posts:
            # This domain answered but had nothing usable for this query -
            # that's a real "no results" signal, not a block, so don't
            # burn the remaining domains on the same query.
            return [], domain
        discussion_posts.sort(key=lambda p: p.get("num_comments", 0), reverse=True)
        return discussion_posts[:MAX_CANDIDATE_POSTS_PER_TIER], domain
    return [], _SEARCH_DOMAINS[-1]


def _fetch_post_comments(domain: str, subreddit: str, post_id: str, post_title: str, time_left: float) -> List[RedditComment]:
    """Top-level comments only (matches the ``depth=1`` request param) -
    plenty for sentiment purposes and keeps request volume low.

    Uses the same domain that successfully answered the search for this
    tier (passed in by the caller) rather than re-discovering a working
    domain per post - the search already proved that domain isn't blocked
    right now.
    """
    payload = _get_json(_comments_url(domain, subreddit, post_id), time_left)
    if not payload or not isinstance(payload, list) or len(payload) < 2:
        return []
    children = (payload[1].get("data", {}) or {}).get("children", []) or []

    out: List[RedditComment] = []
    for child in children:
        if child.get("kind") != "t1":
            continue
        data = child.get("data", {}) or {}
        if data.get("stickied"):
            continue
        author = (data.get("author") or "").strip()
        if author.lower() in _EXCLUDED_AUTHORS:
            continue
        body = (data.get("body") or "").strip()
        if not body or body in _REMOVED_BODIES:
            continue

        cleaned = _clean_reddit_markdown(body)
        if not cleaned or len(cleaned.split()) < 3:
            continue

        upvotes = data.get("score")
        if upvotes is None:
            upvotes = data.get("ups", 0)
        out.append(RedditComment(
            post_title=post_title,
            comment_text=cleaned,
            author=author or "unknown",
            upvotes=int(upvotes or 0),
        ))
        if len(out) >= MAX_COMMENTS_PER_POST:
            break
    return out


# --- Product-centric search tiers -------------------------------------------
def _build_search_tiers(company_name: str, product_name: str, product_brand: str) -> List["tuple[str, str]"]:
    """Priority order:
      1. product_name
      2. brand + product_name (or "<product_name> Reddit" when no distinct
         brand is available - see inline note below)
      3. company_name + product_name

    A job with no product_name (the "General" company-wide job) falls back
    to a single plain company-name search, mirroring youtube_scraper.py's
    behavior for its own product-less "General" job.
    """
    tiers: List["tuple[str, str]"] = []
    product_name = (product_name or "").strip()
    company_name = (company_name or "").strip()
    product_brand = (product_brand or "").strip()

    if product_name:
        tiers.append(("product_name", product_name))

        # Priority 2 ("Brand + Product"): if a distinct brand is supplied
        # and isn't already embedded in the product name, lead with
        # "<brand> <product>". Otherwise the product name already reads as
        # brand+model (e.g. "Tangzu Wan'er" - "Tangzu" is the brand), so
        # disambiguate with a "Reddit" suffix instead, matching how a
        # person would naturally search for third-party discussion of that
        # exact product (e.g. "Tangzu Wan'er Reddit").
        if product_brand and product_brand.lower() not in product_name.lower():
            tiers.append(("brand_product", f"{product_brand} {product_name}"))
        else:
            tiers.append(("brand_product", f"{product_name} Reddit"))

        if company_name and company_name.lower() not in product_name.lower():
            tiers.append(("company_product", f"{company_name} {product_name}"))
    elif company_name:
        tiers.append(("company_general", company_name))

    return tiers


def _scrape_sync(company_name: str, product_name: str, product_brand: str) -> List[RedditComment]:
    start = time.monotonic()

    def time_left() -> float:
        return TIME_BUDGET_SECONDS - (time.monotonic() - start)

    tiers = _build_search_tiers(company_name, product_name, product_brand)
    collected: List[RedditComment] = []
    seen_keys: set = set()
    source = "none"

    for label, query in tiers:
        if time_left() <= _MIN_TIME_FOR_ANOTHER_TIER_SECONDS:
            logger.info("Reddit scrape: skipping tier %r - out of time budget.", label)
            break

        posts, working_domain = _search_reddit(query, time_left())
        if not posts:
            logger.info("Reddit scrape: tier %r search (%r) found 0 discussion posts.", label, query)
            continue

        logger.info(
            "Reddit scrape: tier %r search (%r) found %d discussion post(s); "
            "fetching comments.", label, query, len(posts),
        )

        tier_comments: List[RedditComment] = []
        for post in posts:
            if time_left() <= _MIN_TIME_FOR_ANOTHER_TIER_SECONDS / 2:
                break
            subreddit = post.get("subreddit", "")
            post_id = post.get("id", "")
            title = post.get("title", "")
            if not subreddit or not post_id:
                continue

            comments = _fetch_post_comments(working_domain, subreddit, post_id, title, time_left())
            time.sleep(_REQUEST_DELAY_SECONDS)

            for c in comments:
                key = c.comment_text.strip().lower()
                if not key or key in seen_keys:
                    continue
                seen_keys.add(key)
                tier_comments.append(c)

            if len(collected) + len(tier_comments) >= MAX_TOTAL_COMMENTS:
                break

        if tier_comments:
            collected.extend(tier_comments)
            source = label
            logger.info("Reddit scrape: tier %r succeeded with %d comment(s).", label, len(tier_comments))
            break

        logger.info(
            "Reddit scrape: tier %r yielded posts but 0 usable comments; trying next tier.",
            label,
        )

    elapsed = time.monotonic() - start
    logger.info(
        "Reddit comments for company=%r product=%r: source=%s collected=%d elapsed=%.1fs (cap=%d).",
        company_name, product_name, source, len(collected), elapsed, MAX_TOTAL_COMMENTS,
    )
    return collected[:MAX_TOTAL_COMMENTS]


async def scrape_reddit_comments(company_data: Dict[str, str]) -> List[str]:
    """Public entry point - same contract as scrape_google_reviews() /
    scrape_youtube_comments() / scrape_twitter_comments() /
    scrape_instagram_comments(): takes the per-job company_data dict app.py
    already builds, returns a flat List[str] of clean comment text (no
    sentiment analysis, no product attribution - that all happens exactly
    as it does today for every other platform once app.py has this list).

    Reads (all optional except company_name, mirroring youtube_scraper.py):
      * company_name  - required; used for the "General" job and Priority 3
      * product_name  - drives Priority 1 & 2 when present
      * product_brand - refines Priority 2 when present
    """
    company_name = (company_data.get("company_name") or "").strip()
    product_name = (company_data.get("product_name") or "").strip()
    product_brand = (company_data.get("product_brand") or "").strip()

    if not company_name and not product_name:
        return []

    loop = asyncio.get_event_loop()
    try:
        comments = await loop.run_in_executor(
            _EXECUTOR, _scrape_sync, company_name, product_name, product_brand,
        )
    except Exception:
        logger.exception(
            "Unhandled error scraping Reddit comments for company=%r product=%r.",
            company_name, product_name,
        )
        return []

    texts = [c.comment_text for c in comments]
    final = normalize_comments(texts)[:MAX_TOTAL_COMMENTS]
    logger.info("Reddit returned %d comments", len(final))
    return final
