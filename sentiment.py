"""
Module Name
-----------
sentiment.py

Purpose
-------
Provides sentiment classification for product review and comment text,
returning one of three labels ("positive", "negative", "neutral") for a
single text or for a batch of texts.

Responsibilities
-----------------
- Guarantee zero network activity at import time and throughout the life
  of the process: HuggingFace offline mode is forced, and the local model
  cache is checked via direct disk I/O only, never a network call.
- Lazily load and cache, in-process, a single instance of the transformer-
  based sentiment-analysis pipeline, shared by all callers, on first use.
- Enforce a process-wide circuit breaker: once the model pipeline is
  confirmed unavailable (not cached locally, or a load/inference failure),
  every subsequent call skips the pipeline entirely and uses the
  keyword-based fallback classifier, with no further pipeline or network
  attempts for the rest of the process.
- Expose two public entry points with identical label semantics:
  `analyze_sentiment` for a single text and `analyze_sentiment_batch` for
  a list of texts, the latter using native pipeline batching for
  efficiency.
- Provide a deterministic, dependency-free keyword-based sentiment
  classifier used whenever the transformer pipeline is unavailable or
  fails.

Architecture
------------
Lazy-initialization singleton pattern guarded by module-level threading
locks (`_pipeline_lock`, `_offline_mode_lock`, `_model_state_lock`).
State is tracked with module-level flags/caches: whether offline mode has
been configured, whether a pipeline load has been attempted, the cached
pipeline instance itself, and whether the model has been confirmed
unavailable for the remainder of the process. No setup work runs at
import time; all of it is deferred to the first call of
`analyze_sentiment`, `analyze_sentiment_batch`, or
`get_sentiment_pipeline`.

Sentiment Pipeline
-------------------
Model: cardiffnlp/twitter-roberta-base-sentiment-latest, loaded through
`transformers.pipeline("sentiment-analysis", ...)` with
`model_kwargs={"local_files_only": True}` (guaranteeing no network access
during model/tokenizer loading) and `truncation=True, max_length=512`
(guaranteeing safe handling of inputs longer than the model's token
limit). Model labels are normalized to "positive" / "negative" / "neutral"
by matching the "pos"/"neg" prefix of the returned label, defaulting to
"neutral" otherwise.

Inputs
------
- `analyze_sentiment(pipeline, text)`: an optional pre-loaded pipeline
  object (or None to use the module's lazily-loaded shared pipeline) and
  a single text string.
- `analyze_sentiment_batch(pipeline, texts, batch_size)`: an optional
  pre-loaded pipeline object, a list of text strings, and a batch chunk
  size (default 32).

Outputs
-------
- `analyze_sentiment`: a single sentiment label string.
- `analyze_sentiment_batch`: a list of sentiment label strings, one per
  input text, in the same order as `texts`.

Dependencies
------------
Standard library: `logging`, `os`, `re`, `sys`, `threading`, `typing`.
Optional third-party: `huggingface_hub`, `transformers` - both imported
lazily inside functions, only when a real pipeline load or cache scan is
actually attempted, so the module has no hard dependency on either at
import time.
"""

import logging
import os
import re
import sys
import threading
from typing import List, Optional

logger = logging.getLogger(__name__)

_SENTIMENT_MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment-latest"


def _force_offline_mode() -> None:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        import huggingface_hub
        huggingface_hub.constants.HF_HUB_OFFLINE = True
    except Exception:
        pass

    _offline_attr_names = ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "_is_offline_mode")
    for mod_name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        if not (mod_name.startswith("huggingface_hub") or mod_name.startswith("transformers")):
            continue
        for attr in _offline_attr_names:
            if hasattr(mod, attr):
                try:
                    setattr(mod, attr, True)
                except Exception:
                    pass


def _model_cached_locally(model_id: str = _SENTIMENT_MODEL_ID) -> bool:
    try:
        from huggingface_hub import scan_cache_dir
        cache_info = scan_cache_dir()
        return any(repo.repo_id == model_id for repo in cache_info.repos)
    except Exception as exc:
        logger.debug("Local HuggingFace cache scan failed: %s", exc)
        return False


_model_state_lock = threading.Lock()
_model_confirmed_unavailable = False


def _mark_model_unavailable(reason: str) -> None:
    global _model_confirmed_unavailable
    with _model_state_lock:
        already_known = _model_confirmed_unavailable
        _model_confirmed_unavailable = True
    if not already_known:
        logger.warning(
            "Sentiment model marked unavailable for the rest of this "
            "process (%s). All further calls will use keyword-based "
            "sentiment with no further pipeline/network attempts.",
            reason,
        )


def _model_should_be_skipped() -> bool:
    with _model_state_lock:
        return _model_confirmed_unavailable


_offline_mode_lock = threading.Lock()
_offline_mode_configured = False


def _ensure_offline_mode_configured() -> None:
    global _offline_mode_configured
    with _offline_mode_lock:
        if _offline_mode_configured:
            return
        _offline_mode_configured = True

        os.environ.setdefault("HF_HUB_ETAG_TIMEOUT", "2")
        _force_offline_mode()

        if _model_cached_locally():
            logger.info(
                "Model %r found in the local HuggingFace cache; it will "
                "load from disk with zero network attempts.",
                _SENTIMENT_MODEL_ID,
            )
            return

        logger.warning(
            "Model %r is not cached locally. Marking the sentiment "
            "pipeline unavailable for the rest of this process so every "
            "call goes straight to keyword-based sentiment with no "
            "pipeline/network attempts at all.",
            _SENTIMENT_MODEL_ID,
        )
        _mark_model_unavailable("model not cached locally; offline mode forced, no downloads permitted")


_pipeline_lock = threading.Lock()
_cached_pipeline = None
_pipeline_load_attempted = False


def get_sentiment_pipeline():
    global _cached_pipeline, _pipeline_load_attempted

    with _pipeline_lock:
        if _pipeline_load_attempted:
            return _cached_pipeline
        _pipeline_load_attempted = True

        _ensure_offline_mode_configured()

        if _model_should_be_skipped():
            return None

        try:
            from transformers import pipeline as _hf_pipeline
            _cached_pipeline = _hf_pipeline(
                "sentiment-analysis",
                model=_SENTIMENT_MODEL_ID,
                tokenizer=_SENTIMENT_MODEL_ID,
                model_kwargs={"local_files_only": True},
                truncation=True,
                max_length=512,
            )
            logger.info(
                "Sentiment pipeline loaded from local cache (local_files_only=True "
                "via model_kwargs, zero network attempts) and cached in memory for "
                "reuse by all future calls."
            )
        except Exception as exc:
            _mark_model_unavailable(f"pipeline load raised: {exc!r}")
            _cached_pipeline = None

        return _cached_pipeline


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


def _resolve_pipeline(pipeline):
    return pipeline if pipeline is not None else get_sentiment_pipeline()


def _map_label(label: str) -> str:
    label = label.lower()
    if label.startswith("pos"):
        return "positive"
    if label.startswith("neg"):
        return "negative"
    return "neutral"


def analyze_sentiment(pipeline, text: str) -> str:
    if not text:
        return "neutral"

    active_pipeline = _resolve_pipeline(pipeline)

    if active_pipeline is not None and not _model_should_be_skipped():
        try:
            result = active_pipeline(text)[0]
            return _map_label(result["label"])
        except Exception as exc:
            _mark_model_unavailable(f"pipeline call raised: {exc!r}")
    return _keyword_sentiment(text)


def analyze_sentiment_batch(pipeline, texts: List[str], batch_size: int = 32) -> List[str]:
    results: List[Optional[str]] = [None] * len(texts)

    for i, t in enumerate(texts):
        if not t:
            results[i] = "neutral"

    non_empty_indices = [i for i, t in enumerate(texts) if t]

    active_pipeline = _resolve_pipeline(pipeline)

    if active_pipeline is not None and non_empty_indices and not _model_should_be_skipped():
        for start in range(0, len(non_empty_indices), batch_size):
            if _model_should_be_skipped():
                break
            chunk_indices = non_empty_indices[start:start + batch_size]
            chunk_texts = [texts[i] for i in chunk_indices]
            try:
                chunk_results = active_pipeline(chunk_texts)
                for idx, result in zip(chunk_indices, chunk_results):
                    results[idx] = _map_label(result["label"])
            except Exception as exc:
                _mark_model_unavailable(f"pipeline batch call raised: {exc!r}")

    for idx in non_empty_indices:
        if results[idx] is None:
            results[idx] = _keyword_sentiment(texts[idx])

    return results
