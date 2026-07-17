"""
Module Name: aspect_intelligence.py

Purpose:
    Phase 1 of the Product Aspect Intelligence layer. Detects fixed-taxonomy
    product aspects (Sound Quality, Bass, Treble, Vocals, Comfort, Build
    Quality, Cable, Accessories, Microphone, Battery, Connectivity, Price,
    Value for Money, Packaging, Delivery, Customer Service, Warranty,
    Durability, Fit, Gaming, Noise Isolation) inside review text already
    collected by the existing Google/Twitter/Instagram/YouTube scrapers,
    classifies sentiment on the LOCAL snippet around each mention (not the
    whole comment), and aggregates per-product, per-aspect Score,
    Frequency, and Importance metrics.

    This module is intentionally separate from `product_intelligence.py`,
    which owns a different meaning of "Product Intelligence" in this
    codebase (CATALOG data - price, availability, specs, FAQ, on-page
    rating - fetched once from each product's own page). To avoid
    colliding with that existing name/contract, this layer is called
    "Aspect Intelligence" throughout.

Responsibilities:
    - Match a curated keyword/phrase lexicon against comment text to
      detect aspect mentions (most-specific phrase first, at most one
      mention per aspect per comment).
    - Isolate the local clause around each mention so a comment such as
      "great sound quality but the battery life is disappointing" yields
      independent sentiment for Sound Quality and Battery instead of one
      blended label.
    - Classify sentiment on every extracted snippet via the existing,
      already-vetted sentiment.py pipeline (analyze_sentiment_batch /
      get_sentiment_pipeline) rather than a second hand-rolled lexicon.
    - Aggregate, per product and per aspect: Score (0-100, same weighting
      and label bands as app.py's brand/product score - positive counts
      fully, neutral counts half), Frequency (how many / what % of that
      product's reviews mention this aspect), and Importance (this
      aspect's share of all aspect mentions for that product).
    - Degrade to "no aspect data for this product" on any internal failure
      rather than raising, so this phase can never break the existing
      comment_rows / product_sentiment / brand_reputation / summary / PDF
      pipeline.

Architecture:
    Aspect extraction runs only over `product_rows` (comments already
    attributed to a selected product by app.py's
    _split_product_and_brand_rows()) - never over `brand_rows`/General/
    Google-Maps comments - keeping Brand Reputation separate from
    product-level intelligence, matching the existing product_sentiment
    vs brand_reputation separation.

    "Other meaningful aspects if present" (freeform aspect discovery
    beyond the fixed taxonomy) is out of scope for this phase - reliable
    freeform discovery needs NLP/LLM-based key-phrase extraction, and this
    codebase is deliberately offline/network-free at analysis time (see
    sentiment.py). The fixed taxonomy is easy to extend with more phrases
    or aspects without touching the extraction logic.

    Explicitly out of scope for this phase (future phases, by agreement):
    Confidence Score, the Recommendation Engine and its explanation, and
    any change to the PDF report.

    Public entry point (the only thing app.py should call):
        build_aspect_intelligence_by_product(product_rows, pipe=None)
            -> Dict[str, List[Dict[str, Any]]]

Aspect Pipeline:
    1. For every row in `product_rows`, track each product's total review
       count (for Frequency), then run the fixed lexicon against the
       comment text and extract a local clause snippet around every
       aspect mention found - pure text/regex work, no model calls yet.
    2. Run one batched sentiment call (analyze_sentiment_batch) across
       every extracted snippet from every product, and attach the
       resulting label back onto each mention record.
    3. Group mention records by product, then by aspect, and aggregate
       each aspect's positive/negative/neutral counts, percentages,
       Score, Frequency, Importance, and up to MAX_SAMPLE_SNIPPETS
       positive/negative sample snippets. Aspects are sorted by mention
       count (descending) within each product; products with no detected
       aspect mentions are absent from the result.

Inputs:
    product_rows: List[Dict[str, Any]] - comment rows already attributed
        to a selected product (each with at least "product", "comment",
        and "platform" keys).
    pipe: Optional[Any] - an already-loaded sentiment pipeline object from
        sentiment.get_sentiment_pipeline(); loaded/reused internally when
        omitted.

Outputs:
    Dict[str, List[Dict[str, Any]]] mapping product name to a list of
    per-aspect summaries (aspect, positive/negative/neutral counts and
    percentages, score, score_label, frequency, frequency_pct,
    importance_pct, sample_positive, sample_negative), sorted by mention
    count descending. Returns {} when there are no product_rows, no
    detected aspect mentions, or on any internal failure - never raises.

Dependencies:
    logging, re, typing, and sentiment (analyze_sentiment_batch,
    get_sentiment_pipeline).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from sentiment import analyze_sentiment_batch, get_sentiment_pipeline

logger = logging.getLogger(__name__)

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

_SNIPPET_RADIUS_CHARS = 70

_BOUNDARY_RE = re.compile(
    r"[.!?;]+|\b(?:but|however|although|though|while|except|yet|whereas|and|so|because)\b",
    re.IGNORECASE,
)

_ASPECT_PATTERNS: Dict[str, List[re.Pattern]] = {
    aspect: [re.compile(r"\b" + re.escape(phrase) + r"\b", re.IGNORECASE) for phrase in phrases]
    for aspect, phrases in ASPECT_LEXICON.items()
}

MAX_SAMPLE_SNIPPETS = 2


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


def _extract_snippet(text: str, start: int, end: int) -> str:
    lo_limit = max(0, start - _SNIPPET_RADIUS_CHARS)
    hi_limit = min(len(text), end + _SNIPPET_RADIUS_CHARS)

    lo = lo_limit
    for m in _BOUNDARY_RE.finditer(text, lo_limit, start):
        lo = m.end()
    if lo == lo_limit and lo > 0:
        space_at = text.find(" ", lo, start)
        if space_at != -1:
            lo = space_at + 1

    hi = hi_limit
    m = _BOUNDARY_RE.search(text, end, hi_limit)
    if m:
        hi = m.start()
    elif hi < len(text):
        space_at = text.rfind(" ", end, hi)
        if space_at != -1:
            hi = space_at

    snippet = text[lo:hi].strip(" ,.;:-")
    return snippet if snippet else text[max(0, start - 20):min(len(text), end + 20)].strip()


def _find_aspect_mentions(comment: str) -> Dict[str, str]:
    mentions: Dict[str, str] = {}
    for aspect, patterns in _ASPECT_PATTERNS.items():
        for pattern in patterns:
            match = pattern.search(comment)
            if match:
                mentions[aspect] = _extract_snippet(comment, match.start(), match.end())
                break
    return mentions


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


def _count_by_sentiment(records: List[Dict[str, Any]], sentiment: str) -> int:
    return sum(1 for r in records if r["sentiment"] == sentiment)


def _summarize_aspect(
    aspect: str,
    records: List[Dict[str, Any]],
    total_mentions_for_product: int,
    total_reviews_for_product: int,
) -> Dict[str, Any]:
    positive = _count_by_sentiment(records, "positive")
    negative = _count_by_sentiment(records, "negative")
    neutral = _count_by_sentiment(records, "neutral")
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
        "frequency": total,
        "frequency_pct": (
            round((total / total_reviews_for_product) * 100, 1)
            if total_reviews_for_product else 0.0
        ),
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
    if not product_rows:
        return {}

    try:
        active_pipe = pipe if pipe is not None else get_sentiment_pipeline()

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

        snippet_texts = [m["snippet"] for m in mention_records]
        snippet_sentiments = analyze_sentiment_batch(active_pipe, snippet_texts)
        for record, sentiment in zip(mention_records, snippet_sentiments):
            record["sentiment"] = sentiment

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
