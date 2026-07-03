"""
config.py — Centralized tunable configuration
================================================
BONUS IMPROVEMENT (per product-discovery brief): every "magic number" and
blacklist that used to be hardcoded inside product_discovery.py / app.py now
lives here, in one place, so future tuning (raising the product cap, adding
a new noise word, changing concurrency) never requires touching business
logic in multiple files.

Nothing in this file changes behaviour by itself — it is imported by
product_discovery.py and app.py, which read these values instead of their
old inline constants.
"""

from typing import List

# ---------------------------------------------------------------------------
# CHANGE 1 — product cap
# ---------------------------------------------------------------------------
# Hard ceiling on how many products discovery is allowed to surface AND
# scrape. Was previously an inline `MAX_PRODUCTS_TO_SCRAPE = 5` in
# product_discovery.py that only limited *scraping*, while the underlying
# `products` list (shown on the dashboard) had no cap at all — that's what
# let ~73 noisy entries reach the UI. Now one number governs both.
MAX_PRODUCTS: int = 10

# ---------------------------------------------------------------------------
# CHANGE 4 — parallel scraping
# ---------------------------------------------------------------------------
# Max concurrent product-review scraping tasks (asyncio.Semaphore in app.py).
# Keeps us polite to target sites while still parallelizing.
MAX_PARALLEL_TASKS: int = 5

# ---------------------------------------------------------------------------
# CHANGE 5 / CHANGE 6 — review volume per product
# ---------------------------------------------------------------------------
# Caps how many reviews/comments we keep for a single product once combined
# across platforms, so one very "talkative" product can't drown out the
# executive summary or blow up the exported files.
MAX_COMMENTS_PER_PRODUCT: int = 20

# ---------------------------------------------------------------------------
# CHANGE 2 / CHANGE 3 — real-product filtering
# ---------------------------------------------------------------------------
# Any candidate whose text contains one of these words/phrases (case
# insensitive, substring match) is rejected as navigation/menu/footer noise
# rather than a real product. This directly encodes the examples from the
# brief ("Page not found", "All Collections", "Replacement Cable", ...).
BLACKLIST_WORDS: List[str] = [
    # generic page / error noise
    "page not found", "404", "not found", "page",
    # collections / catalog navigation
    "all collections", "collection", "collections",
    # community / marketing hubs
    "community", "deal", "deals", "budget",
    # price-range / comparison landing pages
    "under", "above", "best headphones", "best earphones",
    # store chrome
    "shop all", "shop", "accessories", "accessory", "brands", "brand",
    "about", "about us", "contact", "contact us", "wishlist",
    "support", "help", "helpdesk",
    # specific noisy item from the brief
    "replacement cable",
    # UI controls
    "filters", "filter", "sort", "sort by", "view all", "see all",
    "explore", "load more", "show more",
    # site structure
    "menu", "navigation", "nav", "footer", "sidebar", "breadcrumb",
    "search", "cart", "checkout", "login", "log in", "sign in",
    "sign up", "register", "account", "my account",
    "blog", "news", "faq", "faqs", "terms", "privacy", "careers",
    "jobs", "press", "media", "investors", "sitemap", "cookie",
    "sign out", "logout", "close", "skip to content", "subscribe",
    "newsletter", "language", "currency", "back", "next", "previous",
    "read more", "learn more",
]

# Priority tiers used to rank/select the final product list (Change 1):
# best-seller beats featured beats trending beats latest beats "whatever
# else was on the page, in the order it appeared" (handled as the
# leftover/fallback tier in product_discovery._rank_and_select).
PROMINENCE_TIERS: List[List[str]] = [
    ["best seller", "bestseller", "best-seller", "top rated", "top seller"],
    ["featured", "editor's pick", "editors pick", "staff pick"],
    ["trending", "most popular", "popular"],
    ["latest", "new arrival", "just launched", "newly added", "new"],
]

# BUG FIX (July 2026 field test) — a candidate whose text is *entirely* one
# of these badge/ribbon words (nothing else) is a UI badge, not a product.
# Real storefronts stamp "Best Seller" / "New" / "#1" ribbons on top of
# product cards; our card-selector sometimes grabs the ribbon element
# itself rather than the title next to it, producing fake "products" like
# "#1 Best Seller", "#2 Best Seller", "New". Substring matching (used for
# BLACKLIST_WORDS above) is deliberately NOT used here, because "new" or
# "best seller" can legitimately be part of a real product's name (e.g.
# "Wave Buds Pro (Best Seller)") — only an EXACT, whole-candidate match to
# one of these bare badge words is rejected.
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

# ---------------------------------------------------------------------------
# Executive summary — lightweight feature-mention lexicon
# ---------------------------------------------------------------------------
# Used to compute "Most Mentioned Feature" without pulling in a heavy NLP
# dependency — same philosophy as the existing keyword-based sentiment
# fallback in sentiment.py.
FEATURE_KEYWORDS: List[str] = [
    "price", "quality", "delivery", "shipping", "packaging", "sound",
    "battery", "comfort", "design", "durability", "customer service",
    "service", "support", "warranty", "size", "fit", "material",
    "performance", "value", "build quality", "noise cancellation", "app",
]
