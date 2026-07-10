from typing import List

# Number of products returned by Product Discovery.
# We only analyze 5 later, but we want the user to be able
# to choose from the COMPLETE catalogue.

MAX_PRODUCTS: int = 500

MAX_PARALLEL_TASKS: int = 5

MAX_COMMENTS_PER_PRODUCT: int = 20

# ---------------------------------------------------------------------------
# Per-platform scrape volume caps.
#
# These bound how many raw reviews/comments/posts each scraper will try to
# collect before giving up. They are intentionally separate from
# MAX_COMMENTS_PER_PRODUCT above (which controls how many of the collected
# comments are later used for analysis) so that raising the analysis sample
# size and raising how much raw data is scraped can be tuned independently.
#
# Raising these lets a scraper keep going past the old ~20-item cutoff and
# collect as much as a product actually has, up to the number below. Each
# scraper still respects its own internal TIME_BUDGET_SECONDS and the
# scroll-idle detection below, so it will return early (with whatever
# partial results it has) if the platform simply has fewer items, if
# nothing new is loading anymore, or if time runs out.
MAX_GOOGLE_REVIEWS: int = 200
MAX_YOUTUBE_COMMENTS: int = 200
MAX_TWITTER_POSTS: int = 100
MAX_INSTAGRAM_COMMENTS: int = 100

# ---------------------------------------------------------------------------
# Scroll / navigation tuning shared by all scrapers.
#
# MAX_SCROLL_ITERATIONS: hard ceiling on how many scroll steps a scraper will
#   perform on any single page, regardless of whether new content is still
#   appearing. This is a safety valve, not the primary stopping condition.
# SCROLL_IDLE_LIMIT: number of consecutive scrolls that must produce zero new
#   items before a scraper concludes "nothing more is loading" and stops.
# NAVIGATION_RETRIES: number of times a scraper will retry a failed page
#   navigation (timeout, transient network error) before giving up on that
#   URL and moving on / falling back.
MAX_SCROLL_ITERATIONS: int = 100
SCROLL_IDLE_LIMIT: int = 5

# Reduced from 3 -> 1. A blocked/dead site (login wall, bot-check,
# rate-limit) essentially never recovers on retry #2/#3 - it just burns
# the scraper's whole internal time budget waiting on a page that was
# never going to load, and it gets cut off cold by app.py's outer hard
# timeout with 0 results. One retry gives transient network blips a
# chance to clear while keeping the fail-fast guarantee below.
NAVIGATION_RETRIES: int = 1

# Hard per-navigation-attempt timeout. Previously each scraper hardcoded
# its own value (8000-10000ms), and with NAVIGATION_RETRIES=3 that meant
# up to ~40s could be spent just waiting on a single blocked URL before
# any scroll/collect logic ever ran. Centralized here so every scraper
# fails fast and consistently: worst case for one URL is now
# NAV_TIMEOUT_MS * (NAVIGATION_RETRIES + 1) plus a small backoff, i.e.
# ~10-11s instead of ~40s.
NAV_TIMEOUT_MS: int = 5000

# Ceiling for the adaptive navigation timeout used by the scrapers
# (google/twitter/instagram/youtube). NAV_TIMEOUT_MS above is now treated
# as the fast-page floor rather than a fixed value: a scraper scales the
# timeout it hands to page.goto() up to this ceiling based on how much of
# its own internal time budget is still left, so a merely slow (not dead)
# page isn't cut off after a token 5s, while a navigation call is still
# never allowed to ask for more time than the scraper actually has left.
NAV_TIMEOUT_MS_MAX: int = 12000

# Floor guaranteed to the FIRST navigation attempt of any scrape, once the
# browser/context/page for that attempt already exist. The internal time
# budget clock for each scraper starts only after browser launch + context
# creation + page creation have finished (see each scraper's _run/_run_sync
# entry point), specifically so this floor is meaningful and isn't silently
# eaten by Chromium startup time.
NAV_TIMEOUT_MS_MIN: int = 8000

# TTL for the per-business Google Reviews cache (google_scraper.py).
# Google Reviews belong to the company/business, not to an individual
# product, so once we've scraped a business's Maps listing we reuse that
# same result for every product job in the same analysis run instead of
# opening Maps again per product. A single analysis run completes in well
# under this window, so every job in one run shares one scrape; a fresh
# analysis of the same company later still gets a fresh scrape once the
# TTL has passed.
GOOGLE_REVIEW_CACHE_TTL_SECONDS: int = 900

BLACKLIST_WORDS: List[str] = [

    "page not found", "404", "not found", "page",

    "all collections", "collection", "collections",

    "community", "deal", "deals", "budget",

    "under", "above", "best headphones", "best earphones",

    "shop all", "shop", "accessories", "accessory", "brands", "brand",

    "about", "about us", "contact", "contact us", "wishlist",

    "support", "help", "helpdesk",

    "replacement cable",

    "filters", "filter", "sort", "sort by", "view all", "see all",
    "explore", "load more", "show more",

    "menu", "navigation", "nav", "footer", "sidebar", "breadcrumb",
    "search", "cart", "checkout", "login", "log in", "sign in",
    "sign up", "register", "account", "my account",
    "blog", "news", "faq", "faqs", "terms", "privacy", "careers",
    "jobs", "press", "media", "investors", "sitemap", "cookie",
    "sign out", "logout", "close", "skip to content", "subscribe",
    "newsletter", "language", "currency", "back", "next", "previous",
    "read more", "learn more",
]

PROMINENCE_TIERS: List[List[str]] = [
    ["best seller", "bestseller", "best-seller", "top rated", "top seller"],
    ["featured", "editor's pick", "editors pick", "staff pick"],
    ["trending", "most popular", "popular"],
    ["latest", "new arrival", "just launched", "newly added", "new"],
]

EXACT_BADGE_TERMS: List[str] = [
    "new", "sale", "sold out", "best seller", "bestseller", "best-seller",
    "featured", "trending", "hot", "limited edition", "exclusive",
    "top rated", "popular", "most popular", "staff pick", "editor's pick",
    "editors pick", "top seller", "new arrival", "just launched",
    "newly added", "in stock", "out of stock", "low stock", "coming soon",
    "just in", "back in stock",
]

MIN_CANDIDATE_LEN: int = 2
MAX_CANDIDATE_LEN: int = 45

FEATURE_KEYWORDS: List[str] = [
    "price", "quality", "delivery", "shipping", "packaging", "sound",
    "battery", "comfort", "design", "durability", "customer service",
    "service", "support", "warranty", "size", "fit", "material",
    "performance", "value", "build quality", "noise cancellation", "app",
]