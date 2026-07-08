import re
from typing import List, Optional

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
        negate = False
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"

def analyze_sentiment(pipeline, text: str) -> str:
    if not text:
        return "neutral"
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
            pass
    return _keyword_sentiment(text)


def _map_label(label: str) -> str:
    """Shared label-mapping logic, identical to the mapping inline in
    analyze_sentiment(). Factored out so analyze_sentiment_batch() can reuse
    it without touching analyze_sentiment()."""
    label = label.lower()
    if label.startswith("pos"):
        return "positive"
    if label.startswith("neg"):
        return "negative"
    return "neutral"


def analyze_sentiment_batch(pipeline, texts: List[str], batch_size: int = 32) -> List[str]:
    """
    Batched counterpart to analyze_sentiment().

    Returns one sentiment label ("positive" / "negative" / "neutral") per
    input text, in the same order as `texts`. Semantically equivalent to
    calling analyze_sentiment(pipeline, text) for each text individually —
    same empty-text short-circuit, same label mapping, same keyword-based
    fallback on failure — but issues far fewer HuggingFace pipeline calls by
    passing chunks of texts to the pipeline at once (native batching) instead
    of one call per text.

    analyze_sentiment() itself is unchanged and remains available for
    single-item use.
    """
    results: List[Optional[str]] = [None] * len(texts)

    # Same short-circuit as analyze_sentiment(): empty text -> "neutral",
    # no model call needed.
    for i, t in enumerate(texts):
        if not t:
            results[i] = "neutral"

    non_empty_indices = [i for i, t in enumerate(texts) if t]

    if pipeline is not None and non_empty_indices:
        # Process in chunks (native HuggingFace batching per chunk) so a
        # failure only affects the chunk it occurred in, mirroring the
        # per-text isolation of analyze_sentiment's try/except, and so
        # memory use stays bounded regardless of how many comments come in.
        for start in range(0, len(non_empty_indices), batch_size):
            chunk_indices = non_empty_indices[start:start + batch_size]
            chunk_texts = [texts[i] for i in chunk_indices]
            try:
                chunk_results = pipeline(chunk_texts)
                for idx, result in zip(chunk_indices, chunk_results):
                    results[idx] = _map_label(result["label"])
            except Exception:
                pass  # leave as None; filled by keyword fallback below

    # Anything not resolved by the model (pipeline is None, or a chunk
    # raised an exception) falls back to keyword sentiment, exactly as
    # analyze_sentiment() does on its except-branch.
    for idx in non_empty_indices:
        if results[idx] is None:
            results[idx] = _keyword_sentiment(texts[idx])

    return results
