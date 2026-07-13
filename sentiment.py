import logging
import os
import re
import sys
import threading
from typing import List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Zero-network-at-import design.
#
# Previous behavior: at *import time* (i.e. on every `uvicorn` startup) this
# module made a real `requests.head()` call to huggingface.co as a
# "preflight" to decide whether to force offline mode. On networks where the
# TLS handshake to huggingface.co fails/hangs (exactly what production saw -
# SSLV3_ALERT_HANDSHAKE_FAILURE), that single blocking call alone was
# responsible for the multi-second delay before "Application startup
# complete".
#
# New behavior: nothing in this module touches the network, ever, at any
# point - not at import, not lazily, not on the first analysis request.
# Offline mode is *unconditionally* forced (no preflight needed to decide
# this - there is only one mode now), and the very first time sentiment
# analysis is actually requested, this module checks the local HuggingFace
# cache directly (pure disk I/O, no network). If the model is cached, the
# pipeline loads from disk. If it is not cached, the pipeline is never
# attempted and every call goes straight to keyword-based sentiment - no
# download, no retry, no SSL handshake, ever.
#
# All of this setup work is deferred until the first call to
# analyze_sentiment()/analyze_sentiment_batch() (or an explicit call to
# get_sentiment_pipeline()) - never at module import - so
# `python -m uvicorn app:app --reload` never waits on any of it.
# ---------------------------------------------------------------------------

_SENTIMENT_MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment-latest"


def _force_offline_mode() -> None:
    """Flip every offline switch we know of so a pipeline load never
    attempts a network request - only a local cache lookup.
    """
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        import huggingface_hub
        # Some huggingface_hub versions read the env var once into a
        # module-level constant at import time; setting the env var alone
        # can be too late if huggingface_hub was already imported elsewhere.
        # Patching the constant directly guarantees a later pipeline load
        # sees it.
        huggingface_hub.constants.HF_HUB_OFFLINE = True
    except Exception:
        pass

    # Belt-and-suspenders for the same import-order problem: several
    # huggingface_hub/transformers submodules do
    # `from .constants import HF_HUB_OFFLINE` (or similar) at their OWN
    # import time, which creates an independent local name binding that
    # patching huggingface_hub.constants above does not update. So walk
    # every already-loaded huggingface_hub/transformers submodule and flip
    # any offline-flag-looking attribute found directly on it too.
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
    """Check the local HuggingFace cache directly - no network involved -
    to see whether this model's weights are already present on disk.
    """
    try:
        from huggingface_hub import scan_cache_dir
        cache_info = scan_cache_dir()
        return any(repo.repo_id == model_id for repo in cache_info.repos)
    except Exception as exc:
        logger.debug("Local HuggingFace cache scan failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Runtime circuit breaker.
#
# The very first time the model is confirmed unavailable (not cached
# locally, or a real pipeline call/load fails for any reason), remember
# that for the rest of the process and never attempt the pipeline again -
# every later call goes straight to keyword sentiment with zero
# pipeline/network attempts. This bounds the worst case to "at most one
# failed attempt, ever, per process".
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Lazy pipeline loading. Nothing below this point runs at import time.
# ---------------------------------------------------------------------------
_offline_mode_lock = threading.Lock()
_offline_mode_configured = False


def _ensure_offline_mode_configured() -> None:
    """Runs once, lazily, on the first sentiment-analysis request rather
    than at module import. Pure local work: force offline env/flags, then
    check the local cache directly (no network in either step).
    """
    global _offline_mode_configured
    with _offline_mode_lock:
        if _offline_mode_configured:
            return
        _offline_mode_configured = True

        # Defense in depth: bound any per-request etag/HEAD timeout in case
        # something downstream still tries the network despite offline
        # mode, so it can never turn into a long stall.
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
    """Lazily load and cache the local sentiment-analysis pipeline.

    Only does any work the first time it's called (i.e. the first time
    sentiment analysis is actually requested) - never at import time.
    Never performs a network request: offline mode is forced first, and if
    the model isn't already present in the local HuggingFace cache, this
    returns None immediately and permanently (for the life of the process)
    so callers fall back to keyword-based sentiment with no further
    attempts, downloads, or retries.
    """
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
                # BUGFIX: local_files_only must be passed via model_kwargs,
                # not as a bare keyword to pipeline(). pipeline() only
                # forwards model_kwargs to AutoConfig.from_pretrained(),
                # AutoModelForSequenceClassification.from_pretrained(), and
                # (copied) AutoTokenizer.from_pretrained() - the three
                # places that actually accept local_files_only.
                #
                # A bare local_files_only=True kwarg, by contrast, is *not*
                # consumed by pipeline()'s named parameters, so it falls
                # into pipeline()'s **kwargs, which get passed straight
                # through to the pipeline class's __init__ and from there
                # into TextClassificationPipeline._sanitize_parameters(),
                # which buckets any unrecognized kwarg into preprocess_params.
                # Those preprocess_params are re-applied on every single
                # inference call as tokenizer_kwargs, i.e.
                # self.tokenizer(text, ..., local_files_only=True). That is
                # what reached PreTrainedTokenizerFast._batch_encode_plus(),
                # which has no such argument and raised the TypeError seen
                # at inference time (not at load time, since loading happens
                # lazily on first use here) - and which then permanently
                # tripped the circuit breaker down to keyword fallback.
                #
                # Passed this way instead, local_files_only still fully
                # blocks any network round-trip during model/tokenizer
                # loading (same guarantee as before) - it just no longer
                # leaks into the per-call tokenizer arguments where it
                # isn't a supported option.
                model_kwargs={"local_files_only": True},
                # Carried over from the old app.py-local pipeline loader
                # this consolidates: without truncation, a comment/review
                # longer than the model's 512-token limit would raise at
                # inference time instead of being safely truncated.
                # truncation/max_length ARE valid _batch_encode_plus
                # arguments, so these are correctly left as pipeline-level
                # kwargs (they're meant to reach the tokenizer call).
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


def analyze_sentiment(pipeline, text: str) -> str:
    if not text:
        return "neutral"

    # If the caller didn't already hand us a loaded pipeline, lazily load
    # (and cache) our own on this, the first real request - never at
    # import time.
    active_pipeline = pipeline if pipeline is not None else get_sentiment_pipeline()

    if active_pipeline is not None and not _model_should_be_skipped():
        try:
            result = active_pipeline(text)[0]
            label = result["label"].lower()
            if label.startswith("pos"):
                return "positive"
            if label.startswith("neg"):
                return "negative"
            return "neutral"
        except Exception as exc:
            # First failure trips the breaker for every future call too,
            # instead of only this one - see the circuit breaker section
            # above for why.
            _mark_model_unavailable(f"pipeline call raised: {exc!r}")
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

    # If the caller didn't already hand us a loaded pipeline, lazily load
    # (and cache) our own on this, the first real request - never at
    # import time.
    active_pipeline = pipeline if pipeline is not None else get_sentiment_pipeline()

    if active_pipeline is not None and non_empty_indices and not _model_should_be_skipped():
        # Process in chunks (native HuggingFace batching per chunk) so a
        # failure only affects the chunk it occurred in, mirroring the
        # per-text isolation of analyze_sentiment's try/except, and so
        # memory use stays bounded regardless of how many comments come in.
        for start in range(0, len(non_empty_indices), batch_size):
            if _model_should_be_skipped():
                # A previous chunk in this same batch just tripped the
                # breaker - stop attempting further chunks too, instead of
                # repeating the same retry storm once per remaining chunk.
                break
            chunk_indices = non_empty_indices[start:start + batch_size]
            chunk_texts = [texts[i] for i in chunk_indices]
            try:
                chunk_results = active_pipeline(chunk_texts)
                for idx, result in zip(chunk_indices, chunk_results):
                    results[idx] = _map_label(result["label"])
            except Exception as exc:
                _mark_model_unavailable(f"pipeline batch call raised: {exc!r}")
                # leave as None; filled by keyword fallback below

    # Anything not resolved by the model (pipeline is None, or a chunk
    # raised an exception) falls back to keyword sentiment, exactly as
    # analyze_sentiment() does on its except-branch.
    for idx in non_empty_indices:
        if results[idx] is None:
            results[idx] = _keyword_sentiment(texts[idx])

    return results
