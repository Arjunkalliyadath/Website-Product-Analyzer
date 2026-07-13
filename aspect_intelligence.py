"""
aspect_intelligence.py
------------------------
Phase 1 of the Product Aspect Intelligence layer.

This module is intentionally separate from `product_intelligence.py`.
`product_intelligence.py` already owns a different meaning of "Product
Intelligence" in this codebase - CATALOG data (price, availability,
specs, FAQ, on-page rating) fetched once from each product's own page.
This module instead works on REVIEW TEXT already collected by the
existing Google/Twitter/Instagram/YouTube scrapers and already
sentiment-scored by sentiment.py. To avoid redefining or colliding with
the existing "Product Intelligence" name/contract, this new layer is
called "Aspect Intelligence" throughout.

Scope of THIS phase only:
    - Aspect detection: a fixed, product-review-relevant taxonomy
      (Sound Quality, Bass, Treble, Vocals, Comfort, Build Quality,
      Cable, Accessories, Microphone, Battery, Connectivity, Price,
      Value for Money, Packaging, Delivery, Customer Service, Warranty,
      Durability, Fit, Gaming, Noise Isolation), matched via a curated
      keyword/phrase lexicon.
    - Per-aspect sentiment: classified on the LOCAL snippet of text
      around each aspect mention (not the whole comment), reusing the
      existing, already-vetted sentiment.py pipeline
      (analyze_sentiment_batch / get_sentiment_pipeline) so a comment
      like "great sound but the battery life is disappointing" yields
      Sound Quality: positive and Battery: negative, instead of one
      blended label for the whole comment.
    - Aggregation per product: Aspect Score (0-100, same weighting and
      label bands as the brand/product score already used elsewhere in
      app.py - positive counts fully, neutral counts half), Frequency
      (how many / what % of that product's reviews mention this aspect),
      and Importance (this aspect's share of ALL aspect mentions for the
      product - how much of the aspect-level conversation is about this
      aspect specifically, relative to every other aspect discussed).

Explicitly OUT of scope for this phase (future phases, by agreement):
    - Confidence Score (review count, platform diversity, cross-platform
      agreement, duplicate handling, sentiment consistency -> 0-100)
    - Recommendation Engine (Highly Recommended / Recommended / Mixed /
      Buy with Caution / Not Recommended) and its human-readable
      explanation
    - Any change to the PDF report

Design notes:
    - Aspect extraction only ever runs over `product_rows` (comments
      already attributed to a selected product by the existing
      _split_product_and_brand_rows() in app.py) - never over
      `brand_rows`/General/Google-Maps comments. This keeps Brand
      Reputation completely separate from product-level intelligence,
      matching the separation the pipeline already enforces for
      product_sentiment vs brand_reputation.
    - "Other meaningful aspects if present" (freeform aspect discovery
      beyond the fixed taxonomy) is NOT implemented in this phase - doing
      that reliably needs NLP/LLM-based key-phrase extraction, and this
      codebase is deliberately offline/network-free at analysis time (see
      sentiment.py). The fixed taxonomy below is easy to extend with more
      phrases or additional aspects without touching the extraction logic.
    - Nothing here raises. Any failure degrades to "no aspect data for
      this product" rather than breaking the existing comment_rows /
      product_sentiment / brand_reputation / summary / PDF pipeline that
      already works.

Public entry point (the only thing app.py should call):
    build_aspect_intelligence_by_product(product_rows, pipe=None)
        -> Dict[str, List[Dict[str, Any]]]   # product name -> aspects,
                                              # sorted by mention count desc
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from sentiment import analyze_sentiment_batch, get_sentiment_pipeline

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Aspect taxonomy
# --------------------------------------------------------------------------
# Ordered as requested. Each aspect maps to a list of trigger phrases,
# most-specific first (checked in order; first phrase that matches wins
# so "sound quality" is preferred over a bare "sound" match when both
# would apply to the same span of text).
# --------------------------------------------------------------------------
ASPECT_LEXICON: Dict[str, List[str]] = {
    "Sound Quality": [
        "sound quality", "audio quality", "sound performance",
        "sound output", "audio output", "sound",
    ],
    "Bass": ["bass response", "bass quality", "low end", "bass"],
    "Treble": ["treble", "highs", "high frequencies"],
    "Vocals": ["vocal clarity", "vocals", "voice clarity"],
    "Comfort": [
        "comfort level", "comfortable to wear", "comfortable",
        "comfort", "ear fatigue", "lightweight design",
    ],
    "Build Quality": [
        "build quality", "build material", "solidly built",
        "cheaply made", "feels sturdy", "feels cheap", "flimsy",
    ],
    "Cable": ["cable quality", "charging cable", "cable", "wire", "cord"],
    "Accessories": [
        "accessories", "carrying case", "carry case", "pouch",
        "included adapter", "in the box",
    ],
    "Microphone": ["microphone quality", "mic quality", "microphone", "mic", "call quality"],
    "Battery": [
        "battery life", "battery backup", "battery performance",
        "battery drain", "battery", "charging time", "charging speed",
    ],
    "Connectivity": [
        "bluetooth connectivity", "bluetooth range", "bluetooth connection",
        "pairing issue", "pairing", "connectivity issue", "connectivity",
        "connection drops", "wireless range", "bluetooth",
    ],
    "Price": ["price point", "price tag", "overpriced", "price", "expensive", "pricey", "cheap"],
    "Value for Money": [
        "value for money", "worth the money", "worth the price",
        "worth it", "bang for the buck", "bang for buck", "good value",
    ],
    "Packaging": ["packaging quality", "packaging", "box condition", "unboxing"],
    "Delivery": [
        "delivery time", "delivery was", "late delivery", "delayed delivery",
        "delivery", "shipping time", "shipping was", "shipment",
    ],
    "Customer Service": [
        "customer service", "customer support", "customer care",
        "support team", "helpline", "support staff",
    ],
    "Warranty": ["warranty claim", "warranty period", "warranty", "guarantee"],
    "Durability": [
        "durability", "durable", "long lasting", "long-lasting",
        "stopped working", "broke after", "wear and tear", "fell apart",
    ],
    "Fit": ["fits well", "tight fit", "loose fit", "snug fit", "fit"],
    "Gaming": ["gaming performance", "gaming experience", "input lag", "gaming", "latency"],
    "Noise Isolation": [
        "noise cancellation", "noise cancelling", "noise canceling",
        "noise isolation", "ambient noise", "background noise", "anc",
    ],
}

# Snippet window (characters on each side of the matched phrase) used as
# the OUTER bound when scoping sentiment classification to the LOCAL
# context of a mention.
_SNIPPET_RADIUS_CHARS = 70

# Clause boundaries: sentence punctuation plus contrastive/coordinating
# conjunctions. Used to clamp each snippet to the surrounding clause
# rather than a fixed character window, so a review like "great sound
# quality but the battery life is disappointing" correctly isolates
# "the battery life is disappointing" for Battery instead of also pulling
# in "great...quality" from the unrelated clause on the other side of
# "but". Falls back to the fixed radius above when no boundary is found
# nearby (e.g. short single-clause comments).
_BOUNDARY_RE = re.compile(
    r"[.!?;]+|\b(?:but|however|although|though|while|except|yet|whereas|and|so|because)\b",
    re.IGNORECASE,
)

# Precompiled, case-insensitive, word-bounded patterns for every phrase,
# built once at import time (pure regex compilation - no network, no I/O,
# safe to do eagerly unlike sentiment.py's model loading).
_ASPECT_PATTERNS: Dict[str, List[re.Pattern]] = {
    aspect: [re.compile(r"\b" + re.escape(phrase) + r"\b", re.IGNORECASE) for phrase in phrases]
    for aspect, phrases in ASPECT_LEXICON.items()
}

MAX_SAMPLE_SNIPPETS = 2


# --------------------------------------------------------------------------
# Score / label bands - identical weighting to app.py's
# _compute_brand_score(), duplicated locally (small, pure function) so this
# module has no import-time dependency on app.py, mirroring the same
# "small local copy" pattern product_intelligence.py already uses for its
# own fetch settings.
# --------------------------------------------------------------------------

def _score(positive: int, negative: int, neutral: int, total: int) -> Tuple[float, str]:
    if not total:
        return 0, "No Data"
    score = round(((positive * 100) + (neutral * 50)) / total, 1)
    if score >= 80:
        label = "Very Positive"
    elif score >= 60:
        label = "Positive"
    elif score >= 40:
        label = "Mixed"
    elif score >= 20:
        label = "Negative"
    else:
        label = "Very Negative"
    return score, label


# --------------------------------------------------------------------------
# Aspect mention extraction
# --------------------------------------------------------------------------

def _extract_snippet(text: str, start: int, end: int) -> str:
    """Local clause around a matched aspect phrase.

    Clamps to the nearest clause boundary (sentence punctuation or a
    contrastive/coordinating conjunction) on each side, within an outer
    radius of _SNIPPET_RADIUS_CHARS, so sentiment classification sees only
    the clause actually describing this aspect - not an adjacent clause
    with different (possibly opposite) sentiment. Falls back to the plain
    radius, trimmed to a word boundary, when no clause boundary is found
    within range (typical for short, single-clause comments).
    """
    lo_limit = max(0, start - _SNIPPET_RADIUS_CHARS)
    hi_limit = min(len(text), end + _SNIPPET_RADIUS_CHARS)

    lo = lo_limit
    for m in _BOUNDARY_RE.finditer(text, lo_limit, start):
        lo = m.end()  # keep the LAST boundary before the match = clause start
    if lo == lo_limit and lo > 0:
        space_at = text.find(" ", lo, start)
        if space_at != -1:
            lo = space_at + 1

    hi = hi_limit
    m = _BOUNDARY_RE.search(text, end, hi_limit)
    if m:
        hi = m.start()  # first boundary after the match = clause end
    elif hi < len(text):
        space_at = text.rfind(" ", end, hi)
        if space_at != -1:
            hi = space_at

    snippet = text[lo:hi].strip(" ,.;:-")
    return snippet if snippet else text[max(0, start - 20):min(len(text), end + 20)].strip()


def _find_aspect_mentions(comment: str) -> Dict[str, str]:
    """Returns {aspect_name: local_snippet} for every aspect mentioned in
    this comment. At most one mention per aspect per comment (first
    matching phrase, first occurrence) - frequency counts then mean
    "how many reviews mention this aspect", not raw keyword hits."""
    mentions: Dict[str, str] = {}
    for aspect, patterns in _ASPECT_PATTERNS.items():
        for pattern in patterns:
            match = pattern.search(comment)
            if match:
                mentions[aspect] = _extract_snippet(comment, match.start(), match.end())
                break
    return mentions


# --------------------------------------------------------------------------
# Aggregation
# --------------------------------------------------------------------------

def _unique_snippets(records: List[Dict[str, Any]], sentiment: str, limit: int) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for record in records:
        if record["sentiment"] != sentiment:
            continue
        snippet = record["snippet"]
        key = snippet.lower()
        if snippet and key not in seen:
            seen.add(key)
            out.append(snippet)
        if len(out) >= limit:
            break
    return out


def _summarize_aspect(
    aspect: str,
    records: List[Dict[str, Any]],
    total_mentions_for_product: int,
    total_reviews_for_product: int,
) -> Dict[str, Any]:
    positive = sum(1 for r in records if r["sentiment"] == "positive")
    negative = sum(1 for r in records if r["sentiment"] == "negative")
    neutral = sum(1 for r in records if r["sentiment"] == "neutral")
    total = len(records)

    score, score_label = _score(positive, negative, neutral, total)

    return {
        "aspect": aspect,
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "total": total,
        "positive_pct": round((positive / total) * 100, 1) if total else 0.0,
        "negative_pct": round((negative / total) * 100, 1) if total else 0.0,
        "neutral_pct": round((neutral / total) * 100, 1) if total else 0.0,
        "score": score,
        "score_label": score_label,
        # Frequency: how many / what % of THIS PRODUCT'S reviews mention
        # this aspect at all.
        "frequency": total,
        "frequency_pct": (
            round((total / total_reviews_for_product) * 100, 1)
            if total_reviews_for_product else 0.0
        ),
        # Importance: this aspect's share of ALL aspect mentions for this
        # product - i.e. how much of the aspect-level conversation is
        # about this aspect specifically, relative to every other aspect
        # discussed for the same product.
        "importance_pct": (
            round((total / total_mentions_for_product) * 100, 1)
            if total_mentions_for_product else 0.0
        ),
        "sample_positive": _unique_snippets(records, "positive", MAX_SAMPLE_SNIPPETS),
        "sample_negative": _unique_snippets(records, "negative", MAX_SAMPLE_SNIPPETS),
    }


def build_aspect_intelligence_by_product(
    product_rows: List[Dict[str, Any]],
    pipe: Optional[Any] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Builds per-product aspect intelligence from comment rows that are
    already attributed to a selected product (see
    app._split_product_and_brand_rows() - pass its `product_rows` output
    here, never `brand_rows`).

    `pipe` may be the already-loaded sentiment pipeline object the caller
    obtained from sentiment.get_sentiment_pipeline() (recommended, avoids
    a redundant lookup); if omitted, this module loads/reuses it itself.

    Returns {product_name: [aspect_summary, ...]}, aspects sorted by
    mention count (descending). Products with no detected aspect mentions
    are simply absent from the result - callers/templates should treat a
    missing or empty dict as "no aspect data available" and degrade
    gracefully (dashboard.html only renders this section when non-empty).

    Never raises: any internal failure logs and returns {} rather than
    propagating, so it can never break the existing sentiment / summary /
    PDF pipeline that already works.
    """
    if not product_rows:
        return {}

    try:
        active_pipe = pipe if pipe is not None else get_sentiment_pipeline()

        # Step 1 - extract aspect mentions per comment (pure text/regex
        # work, no model calls yet), tracking each product's total review
        # count along the way for the Frequency calculation below.
        mention_records: List[Dict[str, Any]] = []
        product_review_counts: Dict[str, int] = {}
        for row in product_rows:
            product = row.get("product") or "General"
            product_review_counts[product] = product_review_counts.get(product, 0) + 1

            comment = row.get("comment") or ""
            if not comment:
                continue
            mentions = _find_aspect_mentions(comment)
            for aspect, snippet in mentions.items():
                mention_records.append({
                    "product": product,
                    "aspect": aspect,
                    "snippet": snippet,
                    "platform": row.get("platform", ""),
                })

        if not mention_records:
            return {}

        # Step 2 - one batched sentiment call for every extracted snippet
        # across every product, reusing the same pipeline/keyword-fallback
        # logic already vetted for whole-comment sentiment elsewhere in
        # this app, instead of a second hand-rolled lexicon.
        snippet_texts = [m["snippet"] for m in mention_records]
        snippet_sentiments = analyze_sentiment_batch(active_pipe, snippet_texts)
        for record, sentiment in zip(mention_records, snippet_sentiments):
            record["sentiment"] = sentiment

        # Step 3 - group by product, then by aspect, and aggregate.
        by_product: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for record in mention_records:
            by_product.setdefault(record["product"], {}).setdefault(record["aspect"], []).append(record)

        result: Dict[str, List[Dict[str, Any]]] = {}
        for product, aspects in by_product.items():
            total_mentions_for_product = sum(len(v) for v in aspects.values())
            total_reviews_for_product = product_review_counts.get(product, 0)
            summaries = [
                _summarize_aspect(aspect, records, total_mentions_for_product, total_reviews_for_product)
                for aspect, records in aspects.items()
            ]
            summaries.sort(key=lambda s: s["total"], reverse=True)
            result[product] = summaries

        return result

    except Exception:
        logger.exception(
            "Aspect intelligence build failed unexpectedly - returning no "
            "aspect data for this run rather than breaking the pipeline."
        )
        return {}
