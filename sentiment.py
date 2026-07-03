"""
Sentiment analysis with two layers:
1. Try the HuggingFace transformers pipeline (needs a working model load).
2. If pipeline is None or throws, fall back to a fast keyword-based analyser
   so we always return a real positive/negative/neutral label instead of
   defaulting everything to neutral.
"""

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Keyword-based fallback (no external dependencies)
# ---------------------------------------------------------------------------
_POSITIVE_WORDS = {
    "good", "great", "excellent", "amazing", "awesome", "fantastic",
    "love", "loved", "best", "perfect", "wonderful", "happy", "glad",
    "satisfied", "recommend", "outstanding", "superb", "brilliant",
    "quality", "helpful", "fast", "quick", "nice", "neat", "pleased",
    "impressive", "smooth", "easy", "enjoy", "enjoyed", "beautiful",
    "stunning", "delighted", "polite", "friendly", "efficient", "clean",
    "fresh", "genuine", "authentic", "value", "worth", "affordable",
    "reasonable", "reliable", "trust", "trusted", "legit", "legitimate",
    "prompt", "responsive", "professional", "top", "positive", "wow",
}

_NEGATIVE_WORDS = {
    "bad", "worst", "terrible", "horrible", "awful", "poor", "hate",
    "hated", "disappointed", "disappointing", "useless", "broken",
    "scam", "fake", "fraud", "rude", "slow", "late", "delay", "delayed",
    "expensive", "overpriced", "waste", "wasted", "wrong", "defective",
    "damaged", "missing", "lost", "never", "never again", "refund",
    "return", "problem", "issue", "complaint", "complain", "angry",
    "frustrated", "unhappy", "pathetic", "ridiculous", "cheated",
    "lied", "ignored", "no response", "avoid", "don't buy", "do not buy",
    "not worth", "money wasted", "misleading", "broken", "fail", "failed",
}

_NEGATION = {"not", "no", "never", "don't", "doesn't", "didn't",
             "won't", "can't", "isn't", "wasn't", "hardly", "barely"}


def _keyword_sentiment(text: str) -> str:
    tokens = re.findall(r"\b\w+\b", text.lower())
    pos = neg = 0
    negate = False
    for tok in tokens:
        if tok in _NEGATION:
            negate = True
            continue
        if tok in _POSITIVE_WORDS:
            if negate:
                neg += 1
            else:
                pos += 1
        elif tok in _NEGATIVE_WORDS:
            if negate:
                pos += 1
            else:
                neg += 1
        negate = False          # reset after one word
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def analyze_sentiment(pipeline, text: str) -> str:
    """
    Return 'positive', 'negative', or 'neutral'.

    Uses the HuggingFace pipeline when available; falls back to the fast
    keyword analyser otherwise.
    """
    if not text:
        return "neutral"

    # Try the ML pipeline first
    if pipeline is not None:
        try:
            result = pipeline(text)[0]
            label = result["label"].lower()
            if label.startswith("pos"):
                return "positive"
            if label.startswith("neg"):
                return "negative"
            return "neutral"
        except Exception:
            pass  # fall through to keyword fallback

    # Keyword-based fallback – always returns a meaningful label
    return _keyword_sentiment(text)
