from typing import List

# Number of products returned by Product Discovery.
# We only analyze 5 later, but we want the user to be able
# to choose from the COMPLETE catalogue.

MAX_PRODUCTS: int = 500

MAX_PARALLEL_TASKS: int = 5

MAX_COMMENTS_PER_PRODUCT: int = 20

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