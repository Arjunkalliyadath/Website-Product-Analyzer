"""
Module Name
    app.py — ManobhavaAI Social Media Product Intelligence Service

Purpose
    FastAPI backend that turns a company website URL into a multi-platform
    Product Intelligence report: it discovers a company's products, scrapes
    reviews/comments about them from Google, Twitter, Instagram, YouTube,
    Reddit and the company's own website, runs sentiment and aspect
    analysis over the results, and renders both an interactive dashboard
    and a downloadable PDF report.

Responsibilities
    - Resolve a submitted company name/URL into normalized website metadata
      (logo, social handles, etc.) via company_discovery.
    - Discover a company's product catalogue via product_discovery, and let
      the user hand-pick which products to analyze when there are many.
    - Fan out concurrent, time-bounded scraping jobs per platform per
      product (Google Reviews, Twitter, Instagram, YouTube, Reddit,
      website reviews), each guarded by its own semaphore and hard
      timeout so no single platform can block the whole request.
    - Clean, deduplicate, and batch-run sentiment analysis (sentiment.py)
      over every collected comment, then aggregate positive/negative/
      neutral statistics per product, per platform, and for brand-wide
      ("General") mentions separately (Brand Reputation vs Product
      Intelligence).
    - Derive aspect-level sentiment (aspect_intelligence.py), a Confidence
      Score, and a Buying Recommendation per product and overall, purely
      from already-computed aggregates.
    - Generate a narrative executive summary and a branded multi-page PDF
      report (reportlab), and render the results dashboard (Jinja2).

Architecture
    - Two parallel entry points share an identical scrape -> sentiment ->
      aggregate -> summarize -> PDF -> dashboard pipeline:
        * POST /analyze          — auto-discovered products (<=10).
        * POST /analyze_selected — user-selected products from
          select_products.html (also enriches via product_intelligence.py
          and scrapes each product's own page for reviews).
    - POST /discover_products lets a user browse/select from the full
      catalogue regardless of size; GET /discover exposes metadata
      resolution as a small JSON API; GET /download/{path} serves
      generated export files.
    - A dedicated single-worker ThreadPoolExecutor runs the (synchronous)
      sentiment pipeline off the asyncio event loop; it is preloaded once
      at startup so the first real request doesn't pay the model-load cost.
    - Per-request stage timing/logging is provided by the shared
      _make_pipeline_profiler() factory, used identically by both
      /analyze and /analyze_selected.
    - A collection of module-level helper functions (prefixed with `_`)
      implement the aggregation, confidence-scoring, recommendation, and
      PDF-rendering logic as small, independently testable units that only
      ever read data already computed earlier in the pipeline.

Major Functions
    - analyze() / analyze_selected(): the two main request handlers
      implementing the full scrape -> sentiment -> report pipeline.
    - discover_products_endpoint() / discover(): product/metadata
      discovery endpoints used ahead of a full analysis.
    - _aggregate_by_key(), _compute_overall_stats(),
      _aggregate_brand_reputation(): sentiment aggregation helpers.
    - _compute_confidence_score(), _compute_buying_recommendation():
      Confidence Score and Buying Recommendation computation.
    - generate_summary(): builds the narrative executive summary and
      per-product summaries shown on the dashboard and PDF.
    - generate_pdf_report(): renders the branded, multi-section PDF report.

Inputs
    - HTML form fields: company_name (a website URL) for /analyze and
      /discover_products; website + selected_products (a JSON array of
      product objects) for /analyze_selected.
    - Query parameter: website for GET /discover.
    - Configuration constants from config.py (scrape volume caps, timeouts,
      concurrency limits, blacklist/prominence/feature keyword lists).

Outputs
    - Rendered HTML (index.html, select_products.html, dashboard.html) via
      Jinja2Templates.
    - JSON metadata from GET /discover.
    - Generated PDF/CSV/Excel export files served from /downloads.

Dependencies
    - FastAPI, Jinja2, pandas, reportlab.
    - Local modules: config, company_discovery, product_discovery,
      product_intelligence, aspect_intelligence, sentiment, url_utils,
      utils, and the scrapers.* package (google/twitter/instagram/
      youtube/reddit/website_review scrapers).
"""

import asyncio
import concurrent.futures
import json
import logging
import re
import ssl
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

try:
    if not hasattr(ssl, "PROTOCOL_SSLv23"):
        fallback_tls = getattr(ssl, "PROTOCOL_TLS", None)
        if fallback_tls is None:
            fallback_tls = getattr(ssl, "PROTOCOL_TLS_CLIENT", None)
        if fallback_tls is not None:
            ssl.PROTOCOL_SSLv23 = fallback_tls
except Exception:
    pass

import collections
import collections.abc as _abc
for _name in ("Callable", "MutableMapping", "Mapping", "Iterable",
              "MutableSequence", "MappingView"):
    if not hasattr(collections, _name) and hasattr(_abc, _name):
        setattr(collections, _name, getattr(_abc, _name))

import pandas as pd
from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from dataclasses import dataclass, asdict

import config
from company_discovery import extract_website_metadata
from product_discovery import discover_products
from product_intelligence import build_product_intelligence_batch
from aspect_intelligence import build_aspect_intelligence_by_product
from sentiment import analyze_sentiment_batch, get_sentiment_pipeline
from url_utils import derive_company_name, is_url, normalize_url
from utils import clean_comment, normalize_text, remove_links, unique_comments
from scrapers.google_scraper import scrape_google_reviews
from scrapers.twitter_scraper import scrape_twitter_comments
from scrapers.instagram_scraper import scrape_instagram_comments
from scrapers.youtube_scraper import scrape_youtube_comments, MAX_BROWSER_WORKERS
from scrapers.reddit_scraper import scrape_reddit_comments, MAX_REDDIT_WORKERS
from scrapers.website_review_scraper import scrape_website_reviews, MAX_WORKERS as MAX_WEBSITE_REVIEW_WORKERS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SelectedProduct:
    name: str
    url: str = ""
    brand: str = ""
    image: str = ""
    category: str = ""

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


def _parse_selected_products(raw: str) -> List[SelectedProduct]:
    try:
        parsed = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        logger.warning("selected_products payload was not valid JSON; ignoring it.")
        parsed = []

    if not isinstance(parsed, list):
        parsed = []

    products: List[SelectedProduct] = []
    seen: set = set()
    for item in parsed:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        if not name:
            continue
        url = (item.get("url") or "").strip()
        dedupe_key = url.lower() if url else f"name:{name.lower()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        products.append(SelectedProduct(
            name=name,
            url=url,
            brand=(item.get("brand") or "").strip(),
            image=(item.get("image") or "").strip(),
            category=(item.get("category") or "").strip(),
        ))
    return products

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DOWNLOADS_DIR = BASE_DIR / "downloads"
TEMPLATES_DIR = BASE_DIR / "templates"

DOWNLOADS_DIR.mkdir(exist_ok=True)

app = FastAPI(title="ManobhavaAI — Social Media Analyzer")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/downloads", StaticFiles(directory=str(DOWNLOADS_DIR)), name="downloads")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


_SENTIMENT_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="sentiment_pipeline"
)


@app.on_event("startup")
async def _preload_sentiment_pipeline() -> None:
    loop = asyncio.get_running_loop()
    _start = time.perf_counter()
    try:
        await loop.run_in_executor(_SENTIMENT_EXECUTOR, get_sentiment_pipeline)
    except Exception:
        logger.exception(
            "Sentiment pipeline preload failed at startup; it will be "
            "loaded lazily on the first request instead."
        )
        return
    logger.info(
        "Sentiment pipeline preloaded at startup in %.2fs — the first "
        "analysis request will not pay the model-load cost.",
        time.perf_counter() - _start,
    )


@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={})

def _make_pipeline_profiler():
    pipeline_start = time.perf_counter()
    stage_times: Dict[str, float] = {}

    def log_stage(name: str, elapsed: float) -> None:
        stage_times[name] = stage_times.get(name, 0.0) + elapsed
        logger.info(
            "\n%s\nStage: %s\nTime: %.2f sec\n%s",
            "=" * 50, name, elapsed, "=" * 50,
        )

    async def timed(name: str, coro, timeout: float = None):
        _start = time.perf_counter()
        try:
            if timeout is not None:
                try:
                    return await asyncio.wait_for(coro, timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning(
                        "%s exceeded its %.0fs hard timeout — returning [] "
                        "(no partial results are recoverable from a "
                        "cancelled coroutine; any underlying background "
                        "work may keep running unseen after this point).",
                        name, timeout,
                    )
                    return []
            return await coro
        finally:
            log_stage(name, time.perf_counter() - _start)

    def log_total_and_breakdown() -> None:
        total_elapsed = time.perf_counter() - pipeline_start
        logger.info(
            "\n==========================\nTOTAL EXECUTION TIME\n==========================\n%.2f sec",
            total_elapsed,
        )
        if total_elapsed > 0 and stage_times:
            breakdown = ["Stage runtime share (% of total execution time):"]
            for stage_name, elapsed in stage_times.items():
                pct = (elapsed / total_elapsed) * 100
                breakdown.append(f"  {stage_name}: {pct:.1f}%")
            logger.info("\n".join(breakdown))

    return log_stage, timed, log_total_and_breakdown


@app.post("/analyze")
async def analyze(request: Request, company_name: str = Form(...)):
    _log_stage, _timed, _log_total_and_breakdown = _make_pipeline_profiler()

    company_name = company_name.strip()
    if not company_name:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={"error": "Please enter a website URL (e.g. https://www.example.com)."}
        )

    try:
        _stage_start = time.perf_counter()

        if not is_url(company_name):
            return templates.TemplateResponse(
                request=request, name="index.html",
                context={
                    "error": (
                        f"'{company_name}' doesn't look like a website URL. "
                        "Please enter one like example.com or https://www.example.com."
                    )
                },
            )

        normalized_website = normalize_url(company_name)
        if not normalized_website:
            return templates.TemplateResponse(
                request=request, name="index.html",
                context={
                    "error": (
                        f"'{company_name}' looks like a URL but isn't valid. "
                        "Try a format like example.com or https://www.example.com."
                    )
                },
            )

        metadata = await extract_website_metadata(normalized_website)
        company_data = {
            "company_name":   metadata.get("company_name") or derive_company_name(normalized_website),
            "website":        metadata.get("website") or normalized_website,
            "logo":           metadata.get("logo", ""),
            "twitter":        metadata.get("twitter", ""),
            "instagram":      metadata.get("instagram", ""),
            "youtube":        metadata.get("youtube", ""),
            "twitter_url":    metadata.get("twitter_url", ""),
            "instagram_url":  metadata.get("instagram_url", ""),
            "youtube_url":    metadata.get("youtube_url", ""),
            "facebook":       metadata.get("facebook", ""),
            "linkedin":       metadata.get("linkedin", ""),
            "facebook_url":   metadata.get("facebook_url", ""),
            "linkedin_url":   metadata.get("linkedin_url", ""),
            "google_business": metadata.get("google_business", ""),
            "discovery_version": metadata.get("discovery_version", ""),
        }
        logger.info("Website metadata resolved: %s", company_data)
        _log_stage("Website Metadata Extraction", time.perf_counter() - _stage_start)

        company_name = company_data["company_name"]

        _stage_start = time.perf_counter()
        product_data = await discover_products(company_data)
        _log_stage("Product Discovery", time.perf_counter() - _stage_start)
        products = product_data.get("products", [])

        if len(products) > 10:
            # select_products.html renders product objects (product.name /
            # product.json / product.category), not bare name strings —
            # passing the plain name list here rendered empty cards and,
            # worse, dropped every product URL on the floor, which starved
            # the website review scraper downstream. Build the same rich
            # objects /discover_products builds.
            rich_products = []
            for item in product_data.get("catalogue", []):
                if item.get("is_service") or not item.get("name"):
                    continue
                product_obj = {
                    "name": item.get("name", ""),
                    "url": item.get("url", ""),
                    "brand": item.get("brand", ""),
                    "image": item.get("image", ""),
                    "category": item.get("category", ""),
                }
                product_obj["json"] = json.dumps(product_obj, ensure_ascii=True)
                rich_products.append(product_obj)
            return templates.TemplateResponse(
                request=request,
                name="select_products.html",
                context={
                    "request": request,
                    "products": rich_products[: config.MAX_PRODUCTS],
                    "website": normalized_website,
                    "company": company_data,
                },
            )
        scrape_targets: List[str] = product_data.get("scrape_targets", [])
        logger.info(
            "Product discovery: %d products, %d services, scrape_targets=%s (method=%s)",
            product_data.get("products_found", 0),
            product_data.get("services_found", 0),
            scrape_targets,
            product_data.get("discovery_method"),
        )

        google_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product in scrape_targets:
            product_company_data = dict(company_data)
            product_company_data["company_name"] = f"{company_data['company_name']} {product}"
            google_jobs.append({"label": product, "data": product_company_data})

        youtube_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product in scrape_targets:
            yt_product_data = dict(company_data)
            yt_product_data["product_name"] = product
            youtube_jobs.append({"label": product, "data": yt_product_data})

        reddit_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product in scrape_targets:
            reddit_product_data = dict(company_data)
            reddit_product_data["product_name"] = product
            reddit_jobs.append({"label": product, "data": reddit_product_data})

        # Website review jobs — previously only /analyze_selected ran these,
        # so any site with few enough products to skip the selection screen
        # always came back with zero "Website" comments. The catalogue holds
        # the product URLs the scraper needs.
        catalogue_url_by_name: Dict[str, str] = {}
        for item in product_data.get("catalogue", []):
            item_name = (item.get("name") or "").strip()
            item_url = (item.get("url") or "").strip()
            if item_name and item_url and not item.get("is_service"):
                catalogue_url_by_name.setdefault(item_name, item_url)

        website_review_jobs: List[Dict[str, Any]] = []
        for product in scrape_targets:
            product_url = catalogue_url_by_name.get(product, "")
            if not product_url:
                continue
            website_review_data = dict(company_data)
            website_review_data["product_url"] = product_url
            website_review_data["product_name"] = product
            website_review_jobs.append({"label": product, "data": website_review_data})

        google_semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)
        youtube_semaphore = asyncio.Semaphore(MAX_BROWSER_WORKERS)
        reddit_semaphore = asyncio.Semaphore(MAX_REDDIT_WORKERS)
        website_review_semaphore = asyncio.Semaphore(MAX_WEBSITE_REVIEW_WORKERS)

        GOOGLE_JOB_TIMEOUT_SECONDS = 48
        TWITTER_TIMEOUT_SECONDS = 32
        INSTAGRAM_TIMEOUT_SECONDS = 34
        YOUTUBE_TIMEOUT_SECONDS = 30
        REDDIT_TIMEOUT_SECONDS = 20
        WEBSITE_REVIEW_TIMEOUT_SECONDS = 60

        async def _scrape_google_job(job: Dict[str, Any]) -> List[str]:
            async with google_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_google_reviews(job["data"]),
                        timeout=GOOGLE_JOB_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Google Reviews job for %r exceeded its %.0fs hard "
                        "timeout — no partial results are recoverable here "
                        "(asyncio.wait_for cancels the awaited coroutine on "
                        "timeout and this returns []); any in-progress "
                        "background scrape may still finish and populate "
                        "the cache for a later request, see "
                        "scrape_google_reviews()'s own logging for that.",
                        job.get("label"), GOOGLE_JOB_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("Google Reviews job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("Google Reviews job for %r failed", job.get("label"))
                    return []

        async def _scrape_google_jobs_group() -> List[Any]:
            _group_start = time.perf_counter()
            try:
                return await asyncio.gather(
                    *(_scrape_google_job(job) for job in google_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Google Review Collection", time.perf_counter() - _group_start)

        async def _scrape_youtube_job(job: Dict[str, Any]) -> List[str]:
            async with youtube_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_youtube_comments(job["data"]),
                        timeout=YOUTUBE_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "YouTube Scraper job for %r exceeded its %.0fs hard "
                        "timeout — returning [] (no partial results are "
                        "recoverable: asyncio.wait_for cancels the awaiting "
                        "coroutine, but the underlying executor thread's "
                        "synchronous Playwright work keeps running to "
                        "completion in the background, unseen, holding a "
                        "browser-worker slot until it finishes on its own).",
                        job.get("label"), YOUTUBE_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("YouTube Scraper job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("YouTube Scraper job for %r failed", job.get("label"))
                    return []

        async def _scrape_youtube_jobs_group() -> List[Dict[str, Any]]:
            _group_start = time.perf_counter()
            try:
                per_job_results = await asyncio.gather(
                    *(_scrape_youtube_job(job) for job in youtube_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("YouTube Scraper", time.perf_counter() - _group_start)
            seen_keys: set = set()
            by_product: List[Dict[str, Any]] = []
            for job, outcome in zip(youtube_jobs, per_job_results):
                comments = list(outcome) if not isinstance(outcome, BaseException) else []
                deduped: List[str] = []
                for c in comments:
                    key = c.strip().lower()
                    if not key or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    deduped.append(c)
                by_product.append({"product": job["label"], "comments": deduped})
            return by_product

        async def _scrape_reddit_job(job: Dict[str, Any]) -> List[str]:
            async with reddit_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_reddit_comments(job["data"]),
                        timeout=REDDIT_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Reddit Scraper job for %r exceeded its %.0fs hard "
                        "timeout — returning [] (no partial results are "
                        "recoverable here).",
                        job.get("label"), REDDIT_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("Reddit Scraper job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("Reddit Scraper job for %r failed", job.get("label"))
                    return []

        async def _scrape_reddit_jobs_group() -> List[Dict[str, Any]]:
            _group_start = time.perf_counter()
            try:
                per_job_results = await asyncio.gather(
                    *(_scrape_reddit_job(job) for job in reddit_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Reddit Scraper", time.perf_counter() - _group_start)
            seen_keys: set = set()
            by_product: List[Dict[str, Any]] = []
            for job, outcome in zip(reddit_jobs, per_job_results):
                comments = list(outcome) if not isinstance(outcome, BaseException) else []
                deduped: List[str] = []
                for c in comments:
                    key = c.strip().lower()
                    if not key or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    deduped.append(c)
                by_product.append({"product": job["label"], "comments": deduped})
            return by_product

        async def _scrape_website_review_job(job: Dict[str, Any]) -> List[str]:
            async with website_review_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_website_reviews(job["data"]),
                        timeout=WEBSITE_REVIEW_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Website Review job for %r exceeded its %.0fs hard "
                        "timeout — returning [] (no partial results are "
                        "recoverable here).",
                        job.get("label"), WEBSITE_REVIEW_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("Website Review job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("Website Review job for %r failed", job.get("label"))
                    return []

        async def _scrape_website_review_jobs_group() -> List[Any]:
            _group_start = time.perf_counter()
            try:
                return await asyncio.gather(
                    *(_scrape_website_review_job(job) for job in website_review_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Website Review Scraper", time.perf_counter() - _group_start)

        results = await asyncio.gather(
            _scrape_google_jobs_group(),
            _timed("Twitter Scraper", scrape_twitter_comments(company_data), timeout=TWITTER_TIMEOUT_SECONDS),
            _timed("Instagram Scraper", scrape_instagram_comments(company_data), timeout=INSTAGRAM_TIMEOUT_SECONDS),
            _scrape_youtube_jobs_group(),
            _scrape_reddit_jobs_group(),
            _scrape_website_review_jobs_group(),
            return_exceptions=True,
        )

        google_results = results[0]
        twitter_comments, instagram_comments = results[1], results[2]
        youtube_by_product: List[Dict[str, Any]] = results[3] if not isinstance(results[3], BaseException) else []
        reddit_by_product: List[Dict[str, Any]] = results[4] if not isinstance(results[4], BaseException) else []
        website_review_job_results = results[5] if not isinstance(results[5], BaseException) else []

        website_review_texts: List["tuple[str, str]"] = []
        _seen_website_review_keys: set = set()
        for job, outcome in zip(website_review_jobs, website_review_job_results):
            comments = list(outcome) if not isinstance(outcome, BaseException) else []
            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            for text in comments:
                text = (text or "").strip()
                if not text:
                    continue
                key = re.sub(r"\s+", " ", text).lower()
                if key in _seen_website_review_keys:
                    continue
                _seen_website_review_keys.add(key)
                website_review_texts.append((job["label"], text))

        google_by_product: List[Dict[str, Any]] = []
        google_all_comments: List[str] = []
        for job, outcome in zip(google_jobs, google_results):
            comments = list(outcome) if not isinstance(outcome, BaseException) else []
            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            google_by_product.append({"product": job["label"], "comments": comments})
            google_all_comments.extend(comments)

        youtube_all_comments: List[str] = []
        for entry in youtube_by_product:
            youtube_all_comments.extend(entry["comments"])

        reddit_all_comments: List[str] = []
        for entry in reddit_by_product:
            reddit_all_comments.extend(entry["comments"])

        platform_comments = {
            "Google":    google_all_comments,
            "Twitter":   list(twitter_comments)   if not isinstance(twitter_comments,   BaseException) else [],
            "Instagram": list(instagram_comments) if not isinstance(instagram_comments, BaseException) else [],
            "YouTube":   youtube_all_comments,
            "Reddit":    reddit_all_comments,
            "Website":   [text for _, text in website_review_texts],
        }

        comment_product_lookup: Dict[str, str] = {}
        for entry in google_by_product:
            for comment in entry["comments"]:
                comment_product_lookup.setdefault(comment, entry["product"])

        youtube_product_lookup: Dict[str, str] = {}
        for entry in youtube_by_product:
            for comment in entry["comments"]:
                youtube_product_lookup.setdefault(comment, entry["product"])

        reddit_product_lookup: Dict[str, str] = {}
        for entry in reddit_by_product:
            for comment in entry["comments"]:
                reddit_product_lookup.setdefault(comment, entry["product"])

        website_product_lookup: Dict[str, str] = {}
        for product_name, text in website_review_texts:
            website_product_lookup.setdefault(text, product_name)

        platform_product_lookups = {
            "Google": comment_product_lookup,
            "YouTube": youtube_product_lookup,
            "Reddit": reddit_product_lookup,
            "Website": website_product_lookup,
        }

        combined = []
        for platform, comments in platform_comments.items():
            lookup = platform_product_lookups.get(platform, {})
            for comment in comments:
                product_label = lookup.get(comment, "General")
                cleaned = normalize_text(remove_links(clean_comment(comment)))
                if cleaned:
                    combined.append((platform, cleaned, product_label))

        unique = unique_comments([(p, c) for p, c, _ in combined])

        product_lookup_by_key = {(p, c.lower()): prod for p, c, prod in combined}

        _stage_start = time.perf_counter()
        _sentiment_loop = asyncio.get_running_loop()
        pipe = await _sentiment_loop.run_in_executor(_SENTIMENT_EXECUTOR, get_sentiment_pipeline)

        comment_texts = [comment for _, comment in unique]
        sentiments = await _sentiment_loop.run_in_executor(
            _SENTIMENT_EXECUTOR, analyze_sentiment_batch, pipe, comment_texts
        )

        comment_rows: List[Dict[str, Any]] = []
        for (platform, comment), sentiment in zip(unique, sentiments):
            product_label = product_lookup_by_key.get((platform, comment.lower()), "General")
            comment_rows.append({
                "comment":   comment,
                "platform":  platform,
                "sentiment": sentiment,
                "product":   product_label,
                "timestamp": datetime.utcnow().isoformat(),
            })
        _log_stage("Sentiment Analysis", time.perf_counter() - _stage_start)

        _stage_start = time.perf_counter()
        df = pd.DataFrame(comment_rows)
        if df.empty:
            df = pd.DataFrame(columns=["comment", "platform", "sentiment", "product", "timestamp"])

        product_rows, brand_rows = _split_product_and_brand_rows(comment_rows)

        overall_stats = _compute_overall_stats(product_rows)
        positive, negative, neutral, total = (
            overall_stats["positive"], overall_stats["negative"],
            overall_stats["neutral"], overall_stats["total"],
        )
        positive_pct, negative_pct, neutral_pct = (
            overall_stats["positive_pct"], overall_stats["negative_pct"], overall_stats["neutral_pct"],
        )
        brand_score, brand_label = overall_stats["score"], overall_stats["score_label"]

        product_sentiment = _aggregate_by_key(product_rows, "product")
        platform_sentiment = _aggregate_by_key(product_rows, "platform")
        brand_reputation = _aggregate_brand_reputation(brand_rows)

        try:
            aspect_intelligence = build_aspect_intelligence_by_product(product_rows, pipe=pipe)
        except Exception:
            logger.exception("Aspect intelligence build failed for %s", company_name)
            aspect_intelligence = {}

        try:
            platforms_covered_count = len([p for p, c in platform_comments.items() if c])
            overall_platform_agreement = _platform_agreement(platform_sentiment)
            overall_aspect_consistency = _aspect_consistency(
                [a for aspects in aspect_intelligence.values() for a in aspects]
            )
            confidence_score = _compute_confidence_score(
                review_count=total,
                platforms_covered=platforms_covered_count,
                platform_agreement=overall_platform_agreement,
                aspect_consistency=overall_aspect_consistency,
            )

            product_recommendations: Dict[str, Any] = {}
            for product_name in scrape_targets:
                p_stats = product_sentiment.get(product_name, {})
                p_aspects = aspect_intelligence.get(product_name, [])
                p_confidence = _compute_confidence_score(
                    review_count=p_stats.get("total", 0),
                    platforms_covered=platforms_covered_count,
                    platform_agreement=overall_platform_agreement,
                    aspect_consistency=_aspect_consistency(p_aspects),
                )
                product_recommendations[product_name] = _compute_buying_recommendation(
                    product_score=p_stats.get("score", 0),
                    aspects=p_aspects,
                    review_count=p_stats.get("total", 0),
                    confidence=p_confidence,
                )

            overall_buying_recommendation = _compute_buying_recommendation(
                product_score=brand_score,
                aspects=[a for aspects in aspect_intelligence.values() for a in aspects],
                review_count=total,
                confidence=confidence_score,
            )
        except Exception:
            logger.exception(
                "Confidence score / buying recommendation computation failed for %s",
                company_name,
            )
            confidence_score = {"score": 0, "label": "No Data", "css_class": "no-data"}
            product_recommendations = {}
            overall_buying_recommendation = {
                "label": "No Data", "css_class": "no-data",
                "explanation": "Not enough data to generate a recommendation.",
            }

        real_products = {k: v for k, v in product_sentiment.items() if k != "General"}
        top_positive_product = (
            max(real_products.items(), key=lambda kv: kv[1]["positive_pct"])[0]
            if real_products else ""
        )
        top_negative_product = (
            max(real_products.items(), key=lambda kv: kv[1]["negative_pct"])[0]
            if real_products else ""
        )

        most_discussed_product = (
            max(real_products.items(), key=lambda kv: kv[1]["total"])[0]
            if real_products else ""
        )

        summary = generate_summary(
            company_name=company_name,
            comment_rows=product_rows,
            brand_score=brand_score,
            brand_label=brand_label,
            products_scraped=len(scrape_targets),
            platform_comments=platform_comments,
            most_discussed_product=most_discussed_product,
            top_positive_product=top_positive_product,
            top_negative_product=top_negative_product,
            selected_products=scrape_targets,
            product_sentiment=product_sentiment,
            brand_reputation=brand_reputation,
            aspect_intelligence=aspect_intelligence,
            confidence_score=confidence_score,
            buying_recommendation=overall_buying_recommendation,
            product_recommendations=product_recommendations,
        )

        insight_tabs = _build_insight_tabs(product_rows, platform_comments)

        product_title = ", ".join(scrape_targets) if scrape_targets else company_name

        export_base = Path("downloads") / f"{re.sub(r'[^a-zA-Z0-9]+', '_', company_name).strip('_').lower()}"
        export_base.mkdir(parents=True, exist_ok=True)
        _log_stage("Report Generation", time.perf_counter() - _stage_start)

        report_generated = datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC")

        platform_counts = {
            "Google":    len(platform_comments["Google"]),
            "Twitter":   len(platform_comments["Twitter"]),
            "Instagram": len(platform_comments["Instagram"]),
            "YouTube":   len(platform_comments["YouTube"]),
            "Reddit":    len(platform_comments["Reddit"]),
            "Website":   len(platform_comments["Website"]),
        }

        pdf_path_str = ""
        try:
            pdf_path = generate_pdf_report(
                company_data=company_data,
                summary=summary,
                brand_score=brand_score,
                brand_label=brand_label,
                positive_pct=positive_pct,
                negative_pct=negative_pct,
                neutral_pct=neutral_pct,
                total=total,
                platform_counts=platform_counts,
                product_sentiment=product_sentiment,
                selected_products=scrape_targets,
                products_scraped=len(scrape_targets),
                report_generated=report_generated,
                pdf_path=export_base / "report.pdf",
                brand_reputation=brand_reputation,
                product_title=product_title,
            )
            pdf_path_str = pdf_path.as_posix()
        except Exception:
            logger.exception("PDF report generation failed for %s", company_name)

        _stage_start = time.perf_counter()
        response = templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          company_data,
                "product_title":    product_title,
                "products":         product_data.get("products", []),
                "services":         product_data.get("services", []),
                "products_found":   product_data.get("products_found", 0),
                "services_found":   product_data.get("services_found", 0),
                "products_scraped": len(scrape_targets),
                "reviews_collected": len(platform_comments["Google"]),
                "top_positive_product": top_positive_product,
                "top_negative_product": top_negative_product,
                "most_discussed_product": most_discussed_product,
                "brand_score":      brand_score,
                "brand_label":      brand_label,
                "product_sentiment": product_sentiment,
                "platform_sentiment": platform_sentiment,
                "brand_reputation": brand_reputation,
                "aspect_intelligence": aspect_intelligence,
                "confidence_score":     confidence_score,
                "buying_recommendation": overall_buying_recommendation,
                "product_recommendations": product_recommendations,
                "platform_comments": platform_comments,
                "comments":         comment_rows,
                "insight_tabs":     insight_tabs,
                "positive":         positive,
                "negative":         negative,
                "neutral":          neutral,
                "total":            total,
                "positive_pct":     positive_pct,
                "negative_pct":     negative_pct,
                "neutral_pct":      neutral_pct,
                "platform_counts": platform_counts,
                "chart_payload": {
                    "labels": ["Positive", "Negative", "Neutral"],
                    "values": [positive, negative, neutral],
                },
                "summary":        summary,
                "pdf_path":       pdf_path_str,
                "download_dir":   str(export_base).replace("\\", "/"),
                "report_generated": report_generated,
            },
        )
        _log_stage("Dashboard Rendering", time.perf_counter() - _stage_start)
        return response

    except Exception:
        logger.exception("Analysis failed")
        _empty_summary = {
            "executive_summary": "Analysis unavailable due to an error.",
            "overall_sentiment":       "No Data",
            "platforms_covered":       0,
            "most_discussed_product":  "",
            "most_mentioned_feature":  "Not enough data",
            "key_insights":            [],
            "key_positive_insights":   [],
            "key_complaints":          [],
            "top_complaints":          [],
            "top_positive_topics":     [],
            "recommendations":         [],
            "product_summaries":       [],
            "brand_reputation_summary": "No brand-wide (non-product) reviews were collected for this run.",
            "best_aspects": [], "worst_aspects": [],
            "confidence_score": {"score": 0, "label": "No Data", "css_class": "no-data"},
            "buying_recommendation": {
                "label": "No Data", "css_class": "no-data",
                "explanation": "Analysis unavailable due to an error.",
            },
            "product_recommendations": {},
        }
        _empty_brand_reputation = {
            "positive": 0, "negative": 0, "neutral": 0, "total": 0,
            "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
            "score": 0, "score_label": "No Data",
            "by_platform": {},
            "google_maps": {"positive": 0, "negative": 0, "neutral": 0, "total": 0,
                             "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
                             "score": 0, "score_label": "No Data"},
        }
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          {"company_name": company_name},
                "product_title":    company_name,
                "products": [], "services": [],
                "products_found": 0, "services_found": 0, "products_scraped": 0,
                "reviews_collected": 0,
                "top_positive_product": "", "top_negative_product": "",
                "most_discussed_product": "",
                "brand_score": 0, "brand_label": "No Data",
                "product_sentiment": {}, "platform_sentiment": {},
                "brand_reputation": _empty_brand_reputation,
                "aspect_intelligence": {},
                "confidence_score": {"score": 0, "label": "No Data", "css_class": "no-data"},
                "buying_recommendation": {
                    "label": "No Data", "css_class": "no-data",
                    "explanation": "Analysis unavailable due to an error.",
                },
                "product_recommendations": {},
                "platform_comments": {"Google": [], "Twitter": [], "Instagram": [], "YouTube": [], "Reddit": []},
                "comments":         [],
                "insight_tabs":     {"top_positive": [], "top_negative": [], "by_platform": {}, "sample": []},
                "positive": 0, "negative": 0, "neutral": 0, "total": 0,
                "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
                "platform_counts": {"Google": 0, "Twitter": 0, "Instagram": 0, "YouTube": 0},
                "chart_payload": {"labels": ["Positive", "Negative", "Neutral"], "values": [0, 0, 0]},
                "summary":     _empty_summary,
                "pdf_path": "",
                "download_dir": "downloads",
                "report_generated": datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC"),
            },
        )

    finally:
        _log_total_and_breakdown()
@app.post("/discover_products")
async def discover_products_endpoint(request: Request, company_name: str = Form(...)):
    company_name = company_name.strip()
    if not company_name:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={"error": "Please enter a website URL (e.g. https://www.example.com)."}
        )

    if not is_url(company_name):
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={
                "error": (
                    f"'{company_name}' doesn't look like a website URL. "
                    "Please enter one like example.com or https://www.example.com."
                )
            },
        )

    normalized_website = normalize_url(company_name)
    if not normalized_website:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={
                "error": (
                    f"'{company_name}' looks like a URL but isn't valid. "
                    "Try a format like example.com or https://www.example.com."
                )
            },
        )

    try:
        metadata = await extract_website_metadata(normalized_website)
        company_data = {
            "company_name":   metadata.get("company_name") or derive_company_name(normalized_website),
            "website":        metadata.get("website") or normalized_website,
            "logo":           metadata.get("logo", ""),
            "twitter":        metadata.get("twitter", ""),
            "instagram":      metadata.get("instagram", ""),
            "youtube":        metadata.get("youtube", ""),
            "twitter_url":    metadata.get("twitter_url", ""),
            "instagram_url":  metadata.get("instagram_url", ""),
            "youtube_url":    metadata.get("youtube_url", ""),
            "facebook":       metadata.get("facebook", ""),
            "linkedin":       metadata.get("linkedin", ""),
            "facebook_url":   metadata.get("facebook_url", ""),
            "linkedin_url":   metadata.get("linkedin_url", ""),
            "google_business": metadata.get("google_business", ""),
            "discovery_version": metadata.get("discovery_version", ""),
        }
        logger.info("Website metadata resolved (manual discovery): %s", company_data)

        product_data = await discover_products(company_data)

        catalogue = product_data.get("catalogue", [])
        products = []
        for item in catalogue:
            if item.get("is_service") or not item.get("name"):
                continue
            product_obj = {
                "name": item.get("name", ""),
                "url": item.get("url", ""),
                "brand": item.get("brand", ""),
                "image": item.get("image", ""),
                "category": item.get("category", ""),
            }
            product_obj["json"] = json.dumps(product_obj, ensure_ascii=True)
            products.append(product_obj)
        products = products[: config.MAX_PRODUCTS]

        logger.info(
            "Manual product discovery: %d products, %d services found (method=%s)",
            product_data.get("products_found", 0),
            product_data.get("services_found", 0),
            product_data.get("discovery_method"),
        )
        return templates.TemplateResponse(
            request=request,
            name="select_products.html",
            context={
                "request": request,
                "products": products,
                "website": normalized_website,
                "company": company_data,
            },
        )

    except Exception:
        logger.exception("Product discovery failed for %s", normalized_website)
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={
                "error": (
                    f"Something went wrong while discovering products for "
                    f"'{normalized_website}'. Please try again."
                )
            },
        )

@app.post("/analyze_selected")
async def analyze_selected(
    request: Request,
    website: str = Form(...),
    selected_products: str = Form(...),
):
    _log_stage, _timed, _log_total_and_breakdown = _make_pipeline_profiler()

    website = website.strip()
    company_name = website

    selected_product_objects: List[SelectedProduct] = _parse_selected_products(selected_products)

    scrape_targets: List[str] = [p.name for p in selected_product_objects]

    if not website:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={"error": "Missing website — please start the analysis again."}
        )

    if not scrape_targets:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={"error": "Please select at least one product to analyze."}
        )

    try:
        _stage_start = time.perf_counter()

        normalized_website = normalize_url(website) if is_url(website) else website

        metadata = await extract_website_metadata(normalized_website)
        company_data = {
            "company_name":   metadata.get("company_name") or derive_company_name(normalized_website),
            "website":        metadata.get("website") or normalized_website,
            "logo":           metadata.get("logo", ""),
            "twitter":        metadata.get("twitter", ""),
            "instagram":      metadata.get("instagram", ""),
            "youtube":        metadata.get("youtube", ""),
            "twitter_url":    metadata.get("twitter_url", ""),
            "instagram_url":  metadata.get("instagram_url", ""),
            "youtube_url":    metadata.get("youtube_url", ""),
            "facebook":       metadata.get("facebook", ""),
            "linkedin":       metadata.get("linkedin", ""),
            "facebook_url":   metadata.get("facebook_url", ""),
            "linkedin_url":   metadata.get("linkedin_url", ""),
            "google_business": metadata.get("google_business", ""),
            "discovery_version": metadata.get("discovery_version", ""),
        }
        logger.info("Website metadata resolved (analyze_selected): %s", company_data)
        _log_stage("Website Metadata Extraction", time.perf_counter() - _stage_start)

        company_name = company_data["company_name"]

        logger.info(
            "Analyzing %d user-selected product(s) for %s: %s",
            len(scrape_targets), company_name, scrape_targets,
        )

        google_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product_obj in selected_product_objects:
            product_company_data = dict(company_data)
            product_company_data["company_name"] = f"{company_data['company_name']} {product_obj.name}"
            product_company_data["product"] = product_obj.as_dict()
            google_jobs.append({"label": product_obj.name, "data": product_company_data})

        youtube_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product_obj in selected_product_objects:
            yt_product_data = dict(company_data)
            yt_product_data["product_name"] = product_obj.name
            yt_product_data["product_brand"] = product_obj.brand
            youtube_jobs.append({"label": product_obj.name, "data": yt_product_data})

        reddit_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product_obj in selected_product_objects:
            reddit_product_data = dict(company_data)
            reddit_product_data["product_name"] = product_obj.name
            reddit_product_data["product_brand"] = product_obj.brand
            reddit_jobs.append({"label": product_obj.name, "data": reddit_product_data})

        website_review_jobs: List[Dict[str, Any]] = []
        for product_obj in selected_product_objects:
            website_review_data = dict(company_data)
            website_review_data["product_url"] = product_obj.url
            website_review_data["product_name"] = product_obj.name
            website_review_jobs.append({"label": product_obj.name, "data": website_review_data})

        google_semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)
        youtube_semaphore = asyncio.Semaphore(MAX_BROWSER_WORKERS)
        reddit_semaphore = asyncio.Semaphore(MAX_REDDIT_WORKERS)
        website_review_semaphore = asyncio.Semaphore(MAX_WEBSITE_REVIEW_WORKERS)

        GOOGLE_JOB_TIMEOUT_SECONDS = 48
        TWITTER_TIMEOUT_SECONDS = 32
        INSTAGRAM_TIMEOUT_SECONDS = 34
        YOUTUBE_TIMEOUT_SECONDS = 30
        REDDIT_TIMEOUT_SECONDS = 20
        WEBSITE_REVIEW_TIMEOUT_SECONDS = 60

        async def _scrape_google_job(job: Dict[str, Any]) -> List[str]:
            async with google_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_google_reviews(job["data"]),
                        timeout=GOOGLE_JOB_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Google Reviews job for %r exceeded its %.0fs hard "
                        "timeout — no partial results are recoverable here "
                        "(asyncio.wait_for cancels the awaited coroutine on "
                        "timeout and this returns []); any in-progress "
                        "background scrape may still finish and populate "
                        "the cache for a later request, see "
                        "scrape_google_reviews()'s own logging for that.",
                        job.get("label"), GOOGLE_JOB_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("Google Reviews job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("Google Reviews job for %r failed", job.get("label"))
                    return []

        async def _scrape_google_jobs_group() -> List[Any]:
            _group_start = time.perf_counter()
            try:
                return await asyncio.gather(
                    *(_scrape_google_job(job) for job in google_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Google Review Collection", time.perf_counter() - _group_start)

        async def _scrape_youtube_job(job: Dict[str, Any]) -> List[str]:
            async with youtube_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_youtube_comments(job["data"]),
                        timeout=YOUTUBE_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "YouTube Scraper job for %r exceeded its %.0fs hard "
                        "timeout — returning [] (no partial results are "
                        "recoverable: asyncio.wait_for cancels the awaiting "
                        "coroutine, but the underlying executor thread's "
                        "synchronous Playwright work keeps running to "
                        "completion in the background, unseen, holding a "
                        "browser-worker slot until it finishes on its own).",
                        job.get("label"), YOUTUBE_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("YouTube Scraper job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("YouTube Scraper job for %r failed", job.get("label"))
                    return []

        async def _scrape_youtube_jobs_group() -> List[Dict[str, Any]]:
            _group_start = time.perf_counter()
            try:
                per_job_results = await asyncio.gather(
                    *(_scrape_youtube_job(job) for job in youtube_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("YouTube Scraper", time.perf_counter() - _group_start)
            seen_keys: set = set()
            by_product: List[Dict[str, Any]] = []
            for job, outcome in zip(youtube_jobs, per_job_results):
                comments = list(outcome) if not isinstance(outcome, BaseException) else []
                deduped: List[str] = []
                for c in comments:
                    key = c.strip().lower()
                    if not key or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    deduped.append(c)
                by_product.append({"product": job["label"], "comments": deduped})
            return by_product

        async def _scrape_reddit_job(job: Dict[str, Any]) -> List[str]:
            async with reddit_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_reddit_comments(job["data"]),
                        timeout=REDDIT_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Reddit Scraper job for %r exceeded its %.0fs hard "
                        "timeout — returning [] (no partial results are "
                        "recoverable here).",
                        job.get("label"), REDDIT_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("Reddit Scraper job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("Reddit Scraper job for %r failed", job.get("label"))
                    return []

        async def _scrape_reddit_jobs_group() -> List[Dict[str, Any]]:
            _group_start = time.perf_counter()
            try:
                per_job_results = await asyncio.gather(
                    *(_scrape_reddit_job(job) for job in reddit_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Reddit Scraper", time.perf_counter() - _group_start)
            seen_keys: set = set()
            by_product: List[Dict[str, Any]] = []
            for job, outcome in zip(reddit_jobs, per_job_results):
                comments = list(outcome) if not isinstance(outcome, BaseException) else []
                deduped: List[str] = []
                for c in comments:
                    key = c.strip().lower()
                    if not key or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    deduped.append(c)
                by_product.append({"product": job["label"], "comments": deduped})
            return by_product

        async def _scrape_website_review_job(job: Dict[str, Any]) -> List[str]:
            async with website_review_semaphore:
                try:
                    return await asyncio.wait_for(
                        scrape_website_reviews(job["data"]),
                        timeout=WEBSITE_REVIEW_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Website Review job for %r exceeded its %.0fs hard "
                        "timeout — returning [] (no partial results are "
                        "recoverable here).",
                        job.get("label"), WEBSITE_REVIEW_TIMEOUT_SECONDS,
                    )
                    return []
                except asyncio.CancelledError:
                    logger.warning("Website Review job for %r was cancelled", job.get("label"))
                    return []
                except Exception:
                    logger.exception("Website Review job for %r failed", job.get("label"))
                    return []

        async def _scrape_website_review_jobs_group() -> List[Any]:
            _group_start = time.perf_counter()
            try:
                return await asyncio.gather(
                    *(_scrape_website_review_job(job) for job in website_review_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Website Review Scraper", time.perf_counter() - _group_start)

        results = await asyncio.gather(
            _scrape_google_jobs_group(),
            _timed("Twitter Scraper", scrape_twitter_comments(company_data), timeout=TWITTER_TIMEOUT_SECONDS),
            _timed("Instagram Scraper", scrape_instagram_comments(company_data), timeout=INSTAGRAM_TIMEOUT_SECONDS),
            _scrape_youtube_jobs_group(),
            _scrape_reddit_jobs_group(),
            _timed(
                "Product Intelligence Enrichment",
                build_product_intelligence_batch([p.as_dict() for p in selected_product_objects]),
            ),
            _scrape_website_review_jobs_group(),
            return_exceptions=True,
        )

        google_results = results[0]
        twitter_comments, instagram_comments = results[1], results[2]
        youtube_by_product: List[Dict[str, Any]] = results[3] if not isinstance(results[3], BaseException) else []
        reddit_by_product: List[Dict[str, Any]] = results[4] if not isinstance(results[4], BaseException) else []
        product_intelligence_results = results[5] if not isinstance(results[5], BaseException) else []
        product_intelligence: List[Dict[str, Any]] = [
            pi.as_dict() if hasattr(pi, "as_dict") else pi
            for pi in (product_intelligence_results or [])
        ]
        website_review_job_results = results[6] if not isinstance(results[6], BaseException) else []

        google_by_product: List[Dict[str, Any]] = []
        google_all_comments: List[str] = []
        for job, outcome in zip(google_jobs, google_results):
            comments = list(outcome) if not isinstance(outcome, BaseException) else []
            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            google_by_product.append({"product": job["label"], "comments": comments})
            google_all_comments.extend(comments)

        website_review_texts: List["tuple[str, str]"] = []
        _seen_website_review_keys: set = set()

        def _add_website_review(product_name: str, text: str) -> None:
            text = (text or "").strip()
            if not text:
                return
            key = re.sub(r"\s+", " ", text).strip().lower()
            if key in _seen_website_review_keys:
                return
            _seen_website_review_keys.add(key)
            website_review_texts.append((product_name, text))

        for product_obj, pi in zip(selected_product_objects, product_intelligence):
            for review in (pi.get("website_reviews") or []):
                _add_website_review(product_obj.name, review.get("text") or "")

        for job, outcome in zip(website_review_jobs, website_review_job_results):
            comments = list(outcome) if not isinstance(outcome, BaseException) else []
            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            for text in comments:
                _add_website_review(job["label"], text)

        youtube_all_comments: List[str] = []
        for entry in youtube_by_product:
            youtube_all_comments.extend(entry["comments"])

        reddit_all_comments: List[str] = []
        for entry in reddit_by_product:
            reddit_all_comments.extend(entry["comments"])

        platform_comments = {
            "Google":    google_all_comments,
            "Twitter":   list(twitter_comments)   if not isinstance(twitter_comments,   BaseException) else [],
            "Instagram": list(instagram_comments) if not isinstance(instagram_comments, BaseException) else [],
            "YouTube":   youtube_all_comments,
            "Reddit":    reddit_all_comments,
            "Website":   [text for _, text in website_review_texts],
        }

        comment_product_lookup: Dict[str, str] = {}
        for entry in google_by_product:
            for comment in entry["comments"]:
                comment_product_lookup.setdefault(comment, entry["product"])

        youtube_product_lookup: Dict[str, str] = {}
        for entry in youtube_by_product:
            for comment in entry["comments"]:
                youtube_product_lookup.setdefault(comment, entry["product"])

        reddit_product_lookup: Dict[str, str] = {}
        for entry in reddit_by_product:
            for comment in entry["comments"]:
                reddit_product_lookup.setdefault(comment, entry["product"])

        website_product_lookup: Dict[str, str] = {}
        for product_name, text in website_review_texts:
            website_product_lookup.setdefault(text, product_name)

        platform_product_lookups = {
            "Google": comment_product_lookup,
            "YouTube": youtube_product_lookup,
            "Reddit": reddit_product_lookup,
            "Website": website_product_lookup,
        }

        combined = []
        for platform, comments in platform_comments.items():
            lookup = platform_product_lookups.get(platform, {})
            for comment in comments:
                product_label = lookup.get(comment, "General")
                cleaned = normalize_text(remove_links(clean_comment(comment)))
                if cleaned:
                    combined.append((platform, cleaned, product_label))
        unique = unique_comments([(p, c) for p, c, _ in combined])
        product_lookup_by_key = {(p, c.lower()): prod for p, c, prod in combined}

        _stage_start = time.perf_counter()
        _sentiment_loop = asyncio.get_running_loop()
        pipe = await _sentiment_loop.run_in_executor(_SENTIMENT_EXECUTOR, get_sentiment_pipeline)

        comment_texts = [comment for _, comment in unique]
        sentiments = await _sentiment_loop.run_in_executor(
            _SENTIMENT_EXECUTOR, analyze_sentiment_batch, pipe, comment_texts
        )

        comment_rows: List[Dict[str, Any]] = []
        for (platform, comment), sentiment in zip(unique, sentiments):
            product_label = product_lookup_by_key.get((platform, comment.lower()), "General")
            comment_rows.append({
                "comment":   comment,
                "platform":  platform,
                "sentiment": sentiment,
                "product":   product_label,
                "timestamp": datetime.utcnow().isoformat(),
            })
        _log_stage("Sentiment Analysis", time.perf_counter() - _stage_start)

        _stage_start = time.perf_counter()
        df = pd.DataFrame(comment_rows)
        if df.empty:
            df = pd.DataFrame(columns=["comment", "platform", "sentiment", "product", "timestamp"])

        product_rows, brand_rows = _split_product_and_brand_rows(comment_rows)

        overall_stats = _compute_overall_stats(product_rows)
        positive, negative, neutral, total = (
            overall_stats["positive"], overall_stats["negative"],
            overall_stats["neutral"], overall_stats["total"],
        )
        positive_pct, negative_pct, neutral_pct = (
            overall_stats["positive_pct"], overall_stats["negative_pct"], overall_stats["neutral_pct"],
        )
        brand_score, brand_label = overall_stats["score"], overall_stats["score_label"]

        product_sentiment = _aggregate_by_key(product_rows, "product")
        platform_sentiment = _aggregate_by_key(product_rows, "platform")
        brand_reputation = _aggregate_brand_reputation(brand_rows)

        try:
            aspect_intelligence = build_aspect_intelligence_by_product(product_rows, pipe=pipe)
        except Exception:
            logger.exception("Aspect intelligence build failed for %s", company_name)
            aspect_intelligence = {}

        try:
            platforms_covered_count = len([p for p, c in platform_comments.items() if c])
            overall_platform_agreement = _platform_agreement(platform_sentiment)
            overall_aspect_consistency = _aspect_consistency(
                [a for aspects in aspect_intelligence.values() for a in aspects]
            )
            confidence_score = _compute_confidence_score(
                review_count=total,
                platforms_covered=platforms_covered_count,
                platform_agreement=overall_platform_agreement,
                aspect_consistency=overall_aspect_consistency,
            )

            product_recommendations: Dict[str, Any] = {}
            for product_name in scrape_targets:
                p_stats = product_sentiment.get(product_name, {})
                p_aspects = aspect_intelligence.get(product_name, [])
                p_confidence = _compute_confidence_score(
                    review_count=p_stats.get("total", 0),
                    platforms_covered=platforms_covered_count,
                    platform_agreement=overall_platform_agreement,
                    aspect_consistency=_aspect_consistency(p_aspects),
                )
                product_recommendations[product_name] = _compute_buying_recommendation(
                    product_score=p_stats.get("score", 0),
                    aspects=p_aspects,
                    review_count=p_stats.get("total", 0),
                    confidence=p_confidence,
                )

            overall_buying_recommendation = _compute_buying_recommendation(
                product_score=brand_score,
                aspects=[a for aspects in aspect_intelligence.values() for a in aspects],
                review_count=total,
                confidence=confidence_score,
            )
        except Exception:
            logger.exception(
                "Confidence score / buying recommendation computation failed for %s",
                company_name,
            )
            confidence_score = {"score": 0, "label": "No Data", "css_class": "no-data"}
            product_recommendations = {}
            overall_buying_recommendation = {
                "label": "No Data", "css_class": "no-data",
                "explanation": "Not enough data to generate a recommendation.",
            }

        real_products = {k: v for k, v in product_sentiment.items() if k != "General"}
        top_positive_product = (
            max(real_products.items(), key=lambda kv: kv[1]["positive_pct"])[0]
            if real_products else ""
        )
        top_negative_product = (
            max(real_products.items(), key=lambda kv: kv[1]["negative_pct"])[0]
            if real_products else ""
        )
        most_discussed_product = (
            max(real_products.items(), key=lambda kv: kv[1]["total"])[0]
            if real_products else ""
        )

        summary = generate_summary(
            company_name=company_name,
            comment_rows=product_rows,
            brand_score=brand_score,
            brand_label=brand_label,
            products_scraped=len(scrape_targets),
            platform_comments=platform_comments,
            most_discussed_product=most_discussed_product,
            top_positive_product=top_positive_product,
            top_negative_product=top_negative_product,
            selected_products=scrape_targets,
            product_sentiment=product_sentiment,
            brand_reputation=brand_reputation,
            aspect_intelligence=aspect_intelligence,
            confidence_score=confidence_score,
            buying_recommendation=overall_buying_recommendation,
            product_recommendations=product_recommendations,
        )

        insight_tabs = _build_insight_tabs(product_rows, platform_comments)

        product_title = ", ".join(scrape_targets) if scrape_targets else company_name

        export_base = Path("downloads") / f"{re.sub(r'[^a-zA-Z0-9]+', '_', company_name).strip('_').lower()}"
        export_base.mkdir(parents=True, exist_ok=True)
        _log_stage("Report Generation", time.perf_counter() - _stage_start)

        report_generated = datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC")

        platform_counts = {
            "Google":    len(platform_comments["Google"]),
            "Twitter":   len(platform_comments["Twitter"]),
            "Instagram": len(platform_comments["Instagram"]),
            "YouTube":   len(platform_comments["YouTube"]),
            "Reddit":    len(platform_comments["Reddit"]),
            "Website":   len(platform_comments["Website"]),
        }

        pdf_path_str = ""
        try:
            pdf_path = generate_pdf_report(
                company_data=company_data,
                summary=summary,
                brand_score=brand_score,
                brand_label=brand_label,
                positive_pct=positive_pct,
                negative_pct=negative_pct,
                neutral_pct=neutral_pct,
                total=total,
                platform_counts=platform_counts,
                product_sentiment=product_sentiment,
                selected_products=scrape_targets,
                products_scraped=len(scrape_targets),
                report_generated=report_generated,
                pdf_path=export_base / "report.pdf",
                brand_reputation=brand_reputation,
                product_title=product_title,
            )
            pdf_path_str = pdf_path.as_posix()
        except Exception:
            logger.exception("PDF report generation failed for %s", company_name)

        _stage_start = time.perf_counter()
        response = templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          company_data,
                "product_title":    product_title,
                "products":         scrape_targets,
                "services":         [],
                "products_found":   len(scrape_targets),
                "services_found":   0,
                "products_scraped": len(scrape_targets),
                "reviews_collected": len(platform_comments["Google"]),
                "top_positive_product": top_positive_product,
                "top_negative_product": top_negative_product,
                "most_discussed_product": most_discussed_product,
                "brand_score":      brand_score,
                "brand_label":      brand_label,
                "product_sentiment": product_sentiment,
                "platform_sentiment": platform_sentiment,
                "brand_reputation": brand_reputation,
                "platform_comments": platform_comments,
                "comments":         comment_rows,
                "insight_tabs":     insight_tabs,
                "positive":         positive,
                "negative":         negative,
                "neutral":          neutral,
                "total":            total,
                "positive_pct":     positive_pct,
                "negative_pct":     negative_pct,
                "neutral_pct":      neutral_pct,
                "platform_counts": platform_counts,
                "chart_payload": {
                    "labels": ["Positive", "Negative", "Neutral"],
                    "values": [positive, negative, neutral],
                },
                "summary":        summary,
                "product_intelligence": product_intelligence,
                "aspect_intelligence": aspect_intelligence,
                "confidence_score":     confidence_score,
                "buying_recommendation": overall_buying_recommendation,
                "product_recommendations": product_recommendations,
                "pdf_path":       pdf_path_str,
                "download_dir":   str(export_base).replace("\\", "/"),
                "report_generated": report_generated,
            },
        )
        _log_stage("Dashboard Rendering", time.perf_counter() - _stage_start)
        return response

    except Exception:
        logger.exception("Analysis of selected products failed")
        _empty_summary = {
            "executive_summary": "Analysis unavailable due to an error.",
            "overall_sentiment":       "No Data",
            "platforms_covered":       0,
            "most_discussed_product":  "",
            "most_mentioned_feature":  "Not enough data",
            "key_insights":            [],
            "key_positive_insights":   [],
            "key_complaints":          [],
            "top_complaints":          [],
            "top_positive_topics":     [],
            "recommendations":         [],
            "product_summaries":       [],
            "brand_reputation_summary": "No brand-wide (non-product) reviews were collected for this run.",
            "best_aspects": [], "worst_aspects": [],
            "confidence_score": {"score": 0, "label": "No Data", "css_class": "no-data"},
            "buying_recommendation": {
                "label": "No Data", "css_class": "no-data",
                "explanation": "Analysis unavailable due to an error.",
            },
            "product_recommendations": {},
        }
        _empty_brand_reputation = {
            "positive": 0, "negative": 0, "neutral": 0, "total": 0,
            "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
            "score": 0, "score_label": "No Data",
            "by_platform": {},
            "google_maps": {"positive": 0, "negative": 0, "neutral": 0, "total": 0,
                             "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
                             "score": 0, "score_label": "No Data"},
        }
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          {"company_name": company_name},
                "product_title":    ", ".join(scrape_targets) if scrape_targets else company_name,
                "products": scrape_targets, "services": [],
                "products_found": len(scrape_targets), "services_found": 0,
                "products_scraped": 0,
                "reviews_collected": 0,
                "top_positive_product": "", "top_negative_product": "",
                "most_discussed_product": "",
                "brand_score": 0, "brand_label": "No Data",
                "product_sentiment": {}, "platform_sentiment": {},
                "brand_reputation": _empty_brand_reputation,
                "platform_comments": {"Google": [], "Twitter": [], "Instagram": [], "YouTube": [], "Reddit": [], "Website": []},
                "comments":         [],
                "insight_tabs":     {"top_positive": [], "top_negative": [], "by_platform": {}, "sample": []},
                "positive": 0, "negative": 0, "neutral": 0, "total": 0,
                "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
                "platform_counts": {"Google": 0, "Twitter": 0, "Instagram": 0, "YouTube": 0},
                "chart_payload": {"labels": ["Positive", "Negative", "Neutral"], "values": [0, 0, 0]},
                "summary":     _empty_summary,
                "product_intelligence": [],
                "aspect_intelligence": {},
                "confidence_score": {"score": 0, "label": "No Data", "css_class": "no-data"},
                "buying_recommendation": {
                    "label": "No Data", "css_class": "no-data",
                    "explanation": "Analysis unavailable due to an error.",
                },
                "product_recommendations": {},
                "pdf_path": "",
                "download_dir": "downloads",
                "report_generated": datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC"),
            },
        )

    finally:
        _log_total_and_breakdown()

@app.get("/download/{path:path}")
async def download_file(path: str):
    resolved = Path("downloads") / path
    if resolved.exists() and resolved.is_file():
        return FileResponse(resolved)
    return JSONResponse(status_code=404, content={"detail": "File not found"})

@app.get("/discover")
async def discover(website: str):
    if not is_url(website):
        return JSONResponse(
            status_code=400,
            content={"detail": f"'{website}' doesn't look like a website URL."},
        )
    normalized_website = normalize_url(website)
    if not normalized_website:
        return JSONResponse(
            status_code=400,
            content={"detail": f"'{website}' looks like a URL but isn't valid."},
        )
    return await extract_website_metadata(normalized_website)

def _compute_brand_score(positive: int, negative: int, neutral: int, total: int) -> tuple:
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

def _aggregate_by_key(comment_rows: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    buckets: Dict[str, Dict[str, int]] = {}
    for row in comment_rows:
        label = row.get(key) or "General"
        bucket = buckets.setdefault(label, {"positive": 0, "negative": 0, "neutral": 0, "total": 0})
        bucket[row["sentiment"]] = bucket.get(row["sentiment"], 0) + 1
        bucket["total"] += 1

    aggregated: Dict[str, Dict[str, Any]] = {}
    for label, counts in buckets.items():
        total = counts["total"]
        positive, negative, neutral = counts["positive"], counts["negative"], counts["neutral"]
        score, score_label = _compute_brand_score(positive, negative, neutral, total)
        aggregated[label] = {
            "positive": positive,
            "negative": negative,
            "neutral":  neutral,
            "total":    total,
            "positive_pct": round((positive / total) * 100, 1) if total else 0.0,
            "negative_pct": round((negative / total) * 100, 1) if total else 0.0,
            "neutral_pct":  round((neutral  / total) * 100, 1) if total else 0.0,
            "score":       score,
            "score_label": score_label,
        }
    return aggregated


def _split_product_and_brand_rows(
    comment_rows: List[Dict[str, Any]],
) -> "tuple[List[Dict[str, Any]], List[Dict[str, Any]]]":
    product_rows = [r for r in comment_rows if r.get("product") and r["product"] != "General"]
    brand_rows = [r for r in comment_rows if not r.get("product") or r["product"] == "General"]
    return product_rows, brand_rows

def _compute_overall_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    positive = sum(1 for r in rows if r.get("sentiment") == "positive")
    negative = sum(1 for r in rows if r.get("sentiment") == "negative")
    neutral  = sum(1 for r in rows if r.get("sentiment") == "neutral")
    total    = len(rows)
    positive_pct = round((positive / total) * 100, 1) if total else 0.0
    negative_pct = round((negative / total) * 100, 1) if total else 0.0
    neutral_pct  = round((neutral  / total) * 100, 1) if total else 0.0
    score, label = _compute_brand_score(positive, negative, neutral, total)
    return {
        "positive": positive, "negative": negative, "neutral": neutral, "total": total,
        "positive_pct": positive_pct, "negative_pct": negative_pct, "neutral_pct": neutral_pct,
        "score": score, "score_label": label,
    }

def _aggregate_brand_reputation(brand_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    overall = _compute_overall_stats(brand_rows)
    by_platform = _aggregate_by_key(brand_rows, "platform")
    google_maps_rows = [r for r in brand_rows if r.get("platform") == "Google"]
    google_maps = _compute_overall_stats(google_maps_rows)
    return {
        "positive": overall["positive"], "negative": overall["negative"],
        "neutral": overall["neutral"], "total": overall["total"],
        "positive_pct": overall["positive_pct"], "negative_pct": overall["negative_pct"],
        "neutral_pct": overall["neutral_pct"],
        "score": overall["score"], "score_label": overall["score_label"],
        "by_platform": by_platform,
        "google_maps": google_maps,
    }

def _most_mentioned_feature(comments: List[str]) -> str:
    counts: Dict[str, int] = {}
    for comment in comments:
        lower = comment.lower()
        for keyword in config.FEATURE_KEYWORDS:
            if keyword in lower:
                counts[keyword] = counts.get(keyword, 0) + 1
    if not counts:
        return "Not enough data"
    return max(counts.items(), key=lambda kv: kv[1])[0].title()

def _top_snippets(comment_rows: List[Dict[str, Any]], sentiment: str, limit: int = 3) -> List[str]:
    candidates = [row["comment"] for row in comment_rows if row["sentiment"] == sentiment]
    candidates.sort(key=lambda text: abs(len(text.split()) - 15))
    return candidates[:limit]

def _build_insight_tabs(
    comment_rows: List[Dict[str, Any]],
    platform_comments: Dict[str, List[str]],
    per_tab_limit: int = 10,
) -> Dict[str, Any]:
    def _representative(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        ranked = sorted(rows, key=lambda r: abs(len(r["comment"].split()) - 15))
        return ranked[:limit]

    def _diverse_sample(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        buckets: Dict[str, List[Dict[str, Any]]] = {"positive": [], "negative": [], "neutral": []}
        for r in rows:
            buckets.setdefault(r["sentiment"], []).append(r)
        order = ["positive", "negative", "neutral"]
        picked: List[Dict[str, Any]] = []
        while len(picked) < limit and any(buckets[s] for s in order):
            for s in order:
                if buckets[s]:
                    picked.append(buckets[s].pop(0))
                    if len(picked) >= limit:
                        break
        return picked

    positive_rows = [r for r in comment_rows if r["sentiment"] == "positive"]
    negative_rows = [r for r in comment_rows if r["sentiment"] == "negative"]

    by_platform: Dict[str, List[Dict[str, Any]]] = {}
    for platform in platform_comments:
        rows = [r for r in comment_rows if r["platform"] == platform]
        if rows:
            by_platform[platform] = _diverse_sample(rows, per_tab_limit)

    return {
        "top_positive": _representative(positive_rows, per_tab_limit),
        "top_negative": _representative(negative_rows, per_tab_limit),
        "by_platform":  by_platform,
    }


def _score_spread_to_consistency(scores: List[float]) -> float:
    if len(scores) < 2:
        return 100.0 if scores else 0.0
    mean = sum(scores) / len(scores)
    variance = sum((s - mean) ** 2 for s in scores) / len(scores)
    std_dev = variance ** 0.5
    return round(max(0.0, 100.0 - (std_dev * 2)), 1)


def _aspect_consistency(aspects: "List[Dict[str, Any]] | None") -> float:
    scores = [a.get("score", 0) for a in (aspects or []) if a.get("total")]
    return _score_spread_to_consistency(scores)


def _platform_agreement(platform_sentiment: "Dict[str, Dict[str, Any]] | None") -> float:
    scores = [v.get("score", 0) for v in (platform_sentiment or {}).values() if v.get("total")]
    return _score_spread_to_consistency(scores)


def _compute_confidence_score(
    review_count: int,
    platforms_covered: int,
    platform_agreement: float,
    aspect_consistency: float,
) -> Dict[str, Any]:
    volume_score = min(100.0, (review_count / 50.0) * 100.0) if review_count else 0.0
    platform_score = min(100.0, (platforms_covered / 4.0) * 100.0)

    confidence = (
        volume_score * 0.30
        + platform_score * 0.20
        + platform_agreement * 0.25
        + aspect_consistency * 0.25
    )
    confidence = round(max(0.0, min(100.0, confidence)))

    if confidence >= 80:
        label, css_class = "Very High", "very-positive"
    elif confidence >= 60:
        label, css_class = "High", "positive"
    elif confidence >= 40:
        label, css_class = "Moderate", "mixed"
    elif confidence >= 20:
        label, css_class = "Low", "negative"
    else:
        label, css_class = "Very Low", "very-negative"

    return {
        "score": confidence,
        "label": label,
        "css_class": css_class,
        "review_count": review_count,
        "platforms_covered": platforms_covered,
        "volume_score": round(volume_score, 1),
        "platform_score": round(platform_score, 1),
        "platform_agreement": platform_agreement,
        "aspect_consistency": aspect_consistency,
    }


_RECOMMENDATION_CSS_CLASS = {
    "Highly Recommended": "very-positive",
    "Recommended":        "positive",
    "Mixed":              "mixed",
    "Buy with Caution":   "negative",
    "Not Recommended":    "very-negative",
    "Not Enough Data":    "no-data",
}


def _compute_buying_recommendation(
    product_score: float,
    aspects: "List[Dict[str, Any]] | None",
    review_count: int,
    confidence: Dict[str, Any],
) -> Dict[str, Any]:
    aspects = aspects or []
    scored_aspects = [a for a in aspects if a.get("total")]
    aspect_scores = [a["score"] for a in scored_aspects]
    avg_aspect_score = round(sum(aspect_scores) / len(aspect_scores), 1) if aspect_scores else product_score

    blended_score = round((product_score * 0.5) + (avg_aspect_score * 0.5), 1)
    confidence_score = confidence.get("score", 0)

    best_aspect = max(scored_aspects, key=lambda a: a["score"]) if scored_aspects else None
    worst_aspect = min(scored_aspects, key=lambda a: a["score"]) if scored_aspects else None

    if review_count < 5 or confidence_score < 30:
        label = "Not Enough Data"
        explanation = (
            f"Only {review_count} review(s) collected with {confidence_score}/100 confidence "
            "- not enough evidence yet for a reliable recommendation."
        )
    elif blended_score >= 80 and confidence_score >= 60:
        label = "Highly Recommended"
        explanation = (
            f"Strong sentiment ({blended_score}/100 blended score) backed by "
            f"{confidence_score}/100 confidence across {review_count} review(s)."
        )
    elif blended_score >= 65 and confidence_score >= 40:
        label = "Recommended"
        explanation = (
            f"Generally positive feedback ({blended_score}/100 blended score) with "
            f"{confidence_score}/100 confidence across {review_count} review(s)."
        )
    elif blended_score >= 45:
        label = "Mixed"
        explanation = (
            f"Feedback is mixed ({blended_score}/100 blended score) - some aspects "
            "perform well, others don't."
        )
    elif blended_score >= 30:
        label = "Buy with Caution"
        explanation = (
            f"Below-average feedback ({blended_score}/100 blended score) - review the "
            "specific complaints below before purchasing."
        )
    else:
        label = "Not Recommended"
        explanation = (
            f"Weak feedback ({blended_score}/100 blended score) across "
            f"{review_count} review(s)."
        )

    if label != "Not Enough Data":
        if worst_aspect:
            explanation += f" Weakest area: {worst_aspect['aspect']} ({worst_aspect['score']}/100)."
        if best_aspect:
            explanation += f" Strongest area: {best_aspect['aspect']} ({best_aspect['score']}/100)."

    return {
        "label": label,
        "css_class": _RECOMMENDATION_CSS_CLASS.get(label, "no-data"),
        "blended_score": blended_score,
        "product_score": product_score,
        "avg_aspect_score": avg_aspect_score,
        "explanation": explanation,
        "best_aspect": best_aspect.get("aspect") if best_aspect else None,
        "worst_aspect": worst_aspect.get("aspect") if worst_aspect else None,
        "confidence_score": confidence_score,
        "confidence_label": confidence.get("label", "No Data"),
        "review_count": review_count,
    }


def generate_summary(
    company_name: str,
    comment_rows: List[Dict[str, Any]],
    brand_score: float,
    brand_label: str,
    products_scraped: int,
    platform_comments: Dict[str, List[str]],
    most_discussed_product: str,
    top_positive_product: str,
    top_negative_product: str,
    selected_products: "List[str] | None" = None,
    product_sentiment: "Dict[str, Dict[str, Any]] | None" = None,
    brand_reputation: "Dict[str, Any] | None" = None,
    aspect_intelligence: "Dict[str, List[Dict[str, Any]]] | None" = None,
    confidence_score: "Dict[str, Any] | None" = None,
    buying_recommendation: "Dict[str, Any] | None" = None,
    product_recommendations: "Dict[str, Dict[str, Any]] | None" = None,
) -> Dict[str, Any]:
    sentiments = [item["sentiment"] for item in comment_rows]
    positive = sentiments.count("positive")
    negative = sentiments.count("negative")
    neutral  = sentiments.count("neutral")
    total    = len(comment_rows)

    platforms_covered = [name for name, comments in platform_comments.items() if comments]
    all_comment_text = [row["comment"] for row in comment_rows]
    most_mentioned_feature = _most_mentioned_feature(all_comment_text)

    selected_products = selected_products or []
    product_sentiment = product_sentiment or {}

    aspect_intelligence = aspect_intelligence or {}
    confidence_score = confidence_score or {
        "score": 0, "label": "No Data", "css_class": "no-data",
    }
    buying_recommendation = buying_recommendation or {
        "label": "No Data", "css_class": "no-data",
        "explanation": "Not enough data to generate a recommendation.",
    }
    product_recommendations = product_recommendations or {}

    product_summaries: List[Dict[str, Any]] = []
    for product_name in selected_products:
        stats = product_sentiment.get(product_name, {
            "positive": 0, "negative": 0, "neutral": 0, "total": 0,
            "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
            "score": 0, "score_label": "No Data",
        })
        if stats["total"]:
            narrative = (
                f"{product_name} collected {stats['total']} review(s) with a product "
                f"score of {stats['score']}/100 ({stats['score_label']}) — "
                f"{stats['positive_pct']}% positive, {stats['negative_pct']}% negative, "
                f"{stats['neutral_pct']}% neutral."
            )
        else:
            narrative = f"{product_name} had no attributable reviews collected for this run."
        product_summaries.append({
            "product": product_name,
            "positive": stats["positive"], "negative": stats["negative"],
            "neutral": stats["neutral"], "total": stats["total"],
            "positive_pct": stats["positive_pct"], "negative_pct": stats["negative_pct"],
            "neutral_pct": stats["neutral_pct"],
            "score": stats["score"], "score_label": stats["score_label"],
            "summary": narrative,
            "recommendation": product_recommendations.get(product_name),
        })

    all_scored_aspects: List[Dict[str, Any]] = []
    for product_name, aspects in aspect_intelligence.items():
        for a in aspects:
            if a.get("total"):
                all_scored_aspects.append({**a, "product": product_name})
    best_aspects = sorted(all_scored_aspects, key=lambda a: a["score"], reverse=True)[:3]
    worst_aspects = sorted(all_scored_aspects, key=lambda a: a["score"])[:3]

    brand_reputation = brand_reputation or {
        "positive": 0, "negative": 0, "neutral": 0, "total": 0,
        "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
        "score": 0, "score_label": "No Data",
        "google_maps": {"total": 0, "score": 0, "score_label": "No Data"},
    }
    if brand_reputation["total"]:
        brand_reputation_summary = (
            f"Outside the selected products, brand-wide reputation signals "
            f"(Google Maps reviews plus general social mentions) reflect "
            f"{brand_reputation['total']} comment(s) with a brand reputation "
            f"score of {brand_reputation['score']}/100 "
            f"({brand_reputation['score_label']}). Google Maps reviews alone "
            f"account for {brand_reputation['google_maps']['total']} of those, "
            f"scoring {brand_reputation['google_maps']['score']}/100."
        )
    else:
        brand_reputation_summary = (
            "No brand-wide (non-product) reviews were collected for this run."
        )

    key_positive_insights = _top_snippets(comment_rows, "positive", limit=3) or [
        "Not enough positive feedback was collected to summarize."
    ]
    key_complaints = _top_snippets(comment_rows, "negative", limit=3) or [
        "Not enough negative feedback was collected to summarize."
    ]

    recommendations: List[str] = []
    if negative and total and (negative / total) >= 0.25:
        feature_note = f" around {most_mentioned_feature.lower()}" if most_mentioned_feature != "Not enough data" else ""
        recommendations.append(
            f"Investigate recurring complaints{feature_note} — negative feedback makes up "
            f"{round((negative / total) * 100)}% of collected reviews."
        )
    if top_negative_product:
        recommendations.append(f"Prioritize a quality/support review for '{top_negative_product}', the lowest-scoring product.")
    if top_positive_product:
        recommendations.append(f"Lean into what's working with '{top_positive_product}' in marketing and testimonials.")
    if not recommendations:
        recommendations.append("Continue monitoring feedback trends across all platforms.")

    key_insights = [
        f"Comments were collected from {', '.join(platforms_covered) if platforms_covered else 'no platforms (none returned data)'}.",
        f"Sentiment breakdown: {positive} positive, {negative} negative, {neutral} neutral out of {total} total.",
    ]
    if total and (positive / total) >= 0.6:
        key_insights.append("Overall brand perception is predominantly positive.")
    elif total and (negative / total) >= 0.4:
        key_insights.append("A significant portion of feedback is negative — attention needed.")

    products_label = ", ".join(selected_products) if selected_products else "the selected product(s)"

    if best_aspects or worst_aspects:
        best_names = ", ".join(a["aspect"] for a in best_aspects) if best_aspects else "not enough data"
        worst_names = ", ".join(a["aspect"] for a in worst_aspects) if worst_aspects else "not enough data"
        aspect_clause = (
            f" Best-performing aspect(s): {best_names}. Areas needing attention: {worst_names}."
        )
    else:
        aspect_clause = ""
    recommendation_clause = (
        f" Overall buying recommendation: {buying_recommendation['label']} "
        f"(confidence: {confidence_score['score']}/100, {confidence_score['label']})."
    )

    return {
        "executive_summary": (
            f"Product Intelligence for {products_label}: {total} product-attributed review(s) "
            f"collected across {len(platforms_covered)} platform(s) covering {products_scraped} "
            f"product(s), with an overall product score of {brand_score}/100 ({brand_label})."
            f"{aspect_clause}{recommendation_clause}"
        ),
        "overall_sentiment":      brand_label,
        "platforms_covered":      len(platforms_covered),
        "platforms_covered_list": platforms_covered,
        "most_discussed_product": most_discussed_product,
        "most_mentioned_feature": most_mentioned_feature,
        "key_insights":            key_insights,
        "key_positive_insights":   key_positive_insights,
        "key_complaints":          key_complaints,

        "top_complaints":         key_complaints,
        "top_positive_topics":    key_positive_insights,
        "recommendations":        recommendations,

        "product_summaries":         product_summaries,
        "brand_reputation_summary":  brand_reputation_summary,

        "best_aspects":              best_aspects,
        "worst_aspects":             worst_aspects,
        "confidence_score":          confidence_score,
        "buying_recommendation":     buying_recommendation,
        "product_recommendations":  product_recommendations,
    }


_PDF_PRIMARY   = colors.HexColor("#5A4FCF")
_PDF_BLUE      = colors.HexColor("#3B5BDB")
_PDF_DARK      = colors.HexColor("#1B1B1B")
_PDF_MUTED     = colors.HexColor("#7B746C")
_PDF_BORDER    = colors.HexColor("#E3DEEF")
_PDF_LIGHT_BG  = colors.HexColor("#F7F5FC")
_PDF_POSITIVE  = colors.HexColor("#2E7D32")
_PDF_NEGATIVE  = colors.HexColor("#B3264A")
_PDF_NEUTRAL   = colors.HexColor("#8A6A1F")
_PDF_WHITE     = colors.HexColor("#FFFFFF")


def _pdf_styles() -> Dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    styles: Dict[str, ParagraphStyle] = {}

    styles["cover_title"] = ParagraphStyle(
        "cover_title", parent=base["Title"],
        fontName="Helvetica-Bold", fontSize=30, leading=34,
        textColor=_PDF_WHITE, alignment=TA_CENTER, spaceAfter=6,
    )
    styles["cover_subtitle"] = ParagraphStyle(
        "cover_subtitle", parent=base["Normal"],
        fontName="Helvetica", fontSize=13, leading=17,
        textColor=_PDF_WHITE, alignment=TA_CENTER,
    )
    styles["cover_field_label"] = ParagraphStyle(
        "cover_field_label", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=9, leading=12,
        textColor=_PDF_MUTED, alignment=TA_CENTER,
    )
    styles["cover_field_value"] = ParagraphStyle(
        "cover_field_value", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=14, leading=18,
        textColor=_PDF_DARK, alignment=TA_CENTER, spaceAfter=14,
    )
    styles["h1"] = ParagraphStyle(
        "h1", parent=base["Heading1"],
        fontName="Helvetica-Bold", fontSize=16, leading=20,
        textColor=_PDF_PRIMARY, spaceBefore=4, spaceAfter=6,
    )
    styles["body"] = ParagraphStyle(
        "body", parent=base["Normal"],
        fontName="Helvetica", fontSize=10.5, leading=15,
        textColor=_PDF_DARK, alignment=TA_JUSTIFY,
    )
    styles["bullet"] = ParagraphStyle(
        "bullet", parent=base["Normal"],
        fontName="Helvetica", fontSize=10, leading=14.5,
        textColor=_PDF_DARK, leftIndent=14, spaceAfter=5,
    )
    styles["table_header"] = ParagraphStyle(
        "table_header", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=9.5, leading=12,
        textColor=_PDF_WHITE, alignment=TA_CENTER,
    )
    styles["table_cell"] = ParagraphStyle(
        "table_cell", parent=base["Normal"],
        fontName="Helvetica", fontSize=9.5, leading=12.5,
        textColor=_PDF_DARK, alignment=TA_CENTER,
    )
    styles["table_cell_left"] = ParagraphStyle(
        "table_cell_left", parent=base["Normal"],
        fontName="Helvetica", fontSize=9.5, leading=12.5,
        textColor=_PDF_DARK, alignment=TA_JUSTIFY,
    )
    styles["footer_brand"] = ParagraphStyle(
        "footer_brand", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=8, textColor=_PDF_MUTED,
    )
    return styles


def _pdf_section_heading(title: str, styles: Dict[str, ParagraphStyle]) -> List[Any]:
    return [
        Paragraph(title, styles["h1"]),
        HRFlowable(width="100%", thickness=1.4, color=_PDF_PRIMARY,
                    spaceBefore=0, spaceAfter=10),
    ]


def _pdf_bullet_list(items: List[str], styles: Dict[str, ParagraphStyle],
                      empty_text: str, bullet_color=None) -> List[Any]:
    flow: List[Any] = []
    color_hex = (bullet_color or _PDF_PRIMARY).hexval() if hasattr(bullet_color or _PDF_PRIMARY, "hexval") else "#5A4FCF"
    if not items:
        flow.append(Paragraph(empty_text, styles["body"]))
        return flow
    for item in items:
        flow.append(Paragraph(
            f'<font color="{color_hex}">&#8226;</font>&nbsp;&nbsp;{item}',
            styles["bullet"],
        ))
    return flow


def _pdf_styled_table(header: List[str], rows: List[List[str]],
                       col_widths: List[float], styles: Dict[str, ParagraphStyle],
                       header_bg=None) -> Table:
    header_bg = header_bg or _PDF_PRIMARY
    data = [[Paragraph(h, styles["table_header"]) for h in header]]
    for row in rows:
        data.append([Paragraph(str(cell), styles["table_cell"]) for cell in row])

    table = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR", (0, 0), (-1, 0), _PDF_WHITE),
        ("GRID", (0, 0), (-1, -1), 0.6, _PDF_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), _PDF_LIGHT_BG))
    table.setStyle(TableStyle(style_cmds))
    return table


def _pdf_footer(canvas_obj, doc) -> None:
    canvas_obj.saveState()
    page_width, _ = A4
    canvas_obj.setStrokeColor(_PDF_PRIMARY)
    canvas_obj.setLineWidth(1.2)
    canvas_obj.line(2 * cm, 1.5 * cm, page_width - 2 * cm, 1.5 * cm)
    canvas_obj.setFont("Helvetica-Bold", 8)
    canvas_obj.setFillColor(_PDF_MUTED)
    canvas_obj.drawString(2 * cm, 1.0 * cm, "ManobhavaAI — Social Media Product Intelligence Report")
    canvas_obj.setFont("Helvetica", 8)
    canvas_obj.drawRightString(page_width - 2 * cm, 1.0 * cm, f"Page {doc.page}")
    canvas_obj.restoreState()


def generate_pdf_report(
    company_data: Dict[str, Any],
    summary: Dict[str, Any],
    brand_score: float,
    brand_label: str,
    positive_pct: float,
    negative_pct: float,
    neutral_pct: float,
    total: int,
    platform_counts: Dict[str, int],
    product_sentiment: Dict[str, Dict[str, Any]],
    selected_products: List[str],
    products_scraped: int,
    report_generated: str,
    pdf_path: Path,
    brand_reputation: "Dict[str, Any] | None" = None,
    product_title: str = "",
) -> Path:
    styles = _pdf_styles()
    story: List[Any] = []

    company_name = company_data.get("company_name") or "Unknown Company"
    website = company_data.get("website") or "—"
    product_title = product_title or (", ".join(selected_products) if selected_products else company_name)
    brand_reputation = brand_reputation or {
        "positive": 0, "negative": 0, "neutral": 0, "total": 0,
        "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
        "score": 0, "score_label": "No Data",
        "by_platform": {},
        "google_maps": {"total": 0, "score": 0, "score_label": "No Data"},
    }

    story.append(Spacer(1, 2.6 * cm))
    cover_band = Table(
        [[Paragraph("ManobhavaAI", styles["cover_title"])],
         [Paragraph("Social Media Product Intelligence Report", styles["cover_subtitle"])]],
        colWidths=[17 * cm],
    )
    cover_band.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _PDF_PRIMARY),
        ("TOPPADDING", (0, 0), (-1, 0), 26),
        ("BOTTOMPADDING", (0, -1), (-1, -1), 26),
        ("TOPPADDING", (0, 1), (-1, 1), 4),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    story.append(cover_band)
    story.append(Spacer(1, 1.6 * cm))

    detail_rows = [
        [Paragraph("PRODUCT(S) ANALYSED", styles["cover_field_label"])],
        [Paragraph(product_title, styles["cover_field_value"])],
        [Paragraph("COMPANY", styles["cover_field_label"])],
        [Paragraph(company_name, styles["cover_field_value"])],
        [Paragraph("WEBSITE", styles["cover_field_label"])],
        [Paragraph(website, styles["cover_field_value"])],
        [Paragraph("REPORT GENERATED", styles["cover_field_label"])],
        [Paragraph(report_generated, styles["cover_field_value"])],
    ]
    detail_card = Table(detail_rows, colWidths=[17 * cm])
    detail_card.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _PDF_LIGHT_BG),
        ("BOX", (0, 0), (-1, -1), 1, _PDF_BORDER),
        ("TOPPADDING", (0, 0), (-1, 0), 18),
        ("BOTTOMPADDING", (0, -1), (-1, -1), 18),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    story.append(detail_card)
    story.append(PageBreak())

    story.extend(_pdf_section_heading("Executive Summary", styles))
    story.append(Paragraph(
        summary.get("executive_summary", "No executive summary available."),
        styles["body"],
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Overall Product Statistics", styles))
    stats_rows = [
        ["Product Score", f"{brand_score} / 100"],
        ["Product Sentiment Label", brand_label],
        ["Positive %", f"{positive_pct}%"],
        ["Negative %", f"{negative_pct}%"],
        ["Neutral %", f"{neutral_pct}%"],
        ["Total Product Reviews", str(total)],
        ["Platforms Covered", str(summary.get("platforms_covered", 0))],
        ["Products Analysed", str(products_scraped)],
    ]
    story.append(_pdf_styled_table(
        ["Metric", "Value"], stats_rows, [8.5 * cm, 8.5 * cm], styles,
        header_bg=_PDF_PRIMARY,
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Selected Products", styles))
    story.extend(_pdf_bullet_list(
        selected_products or [], styles,
        empty_text="No products were selected for this analysis.",
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Product-by-Product Summary", styles))
    product_summaries = summary.get("product_summaries") or []
    if product_summaries:
        for item in product_summaries:
            story.append(Paragraph(f"<b>{item.get('product', '')}</b>", styles["body"]))
            story.append(Paragraph(item.get("summary", ""), styles["body"]))
            story.append(Spacer(1, 8))
    else:
        story.append(Paragraph(
            "No individual product summaries are available for this run.",
            styles["body"],
        ))
    story.append(Spacer(1, 8))

    story.extend(_pdf_section_heading("Platform Summary", styles))
    platform_order = ["Google", "Twitter", "Instagram", "YouTube"]
    platform_rows = [[p, str(platform_counts.get(p, 0))] for p in platform_order]
    story.append(_pdf_styled_table(
        ["Platform", "Review Count"], platform_rows, [8.5 * cm, 8.5 * cm], styles,
        header_bg=_PDF_BLUE,
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Product Sentiment Table", styles))
    real_products = {k: v for k, v in (product_sentiment or {}).items() if k != "General"}
    if real_products:
        product_rows = [
            [name, str(vals.get("positive", 0)), str(vals.get("negative", 0)),
             str(vals.get("neutral", 0)), str(vals.get("score", 0))]
            for name, vals in real_products.items()
        ]
        story.append(_pdf_styled_table(
            ["Product", "Positive", "Negative", "Neutral", "Score"],
            product_rows, [5.5 * cm, 3 * cm, 3 * cm, 3 * cm, 2.5 * cm], styles,
            header_bg=_PDF_BLUE,
        ))
    else:
        story.append(Paragraph("No product-level sentiment data is available.", styles["body"]))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Brand Reputation (Google Maps & Social)", styles))
    story.append(Paragraph(summary.get("brand_reputation_summary", ""), styles["body"]))
    story.append(Spacer(1, 10))
    brand_stats_rows = [
        ["Brand Reputation Score", f"{brand_reputation.get('score', 0)} / 100"],
        ["Brand Reputation Label", brand_reputation.get("score_label", "No Data")],
        ["Total Brand Mentions", str(brand_reputation.get("total", 0))],
        ["Google Maps Reviews", str(brand_reputation.get("google_maps", {}).get("total", 0))],
        ["Google Maps Score", f"{brand_reputation.get('google_maps', {}).get('score', 0)} / 100"],
    ]
    story.append(_pdf_styled_table(
        ["Metric", "Value"], brand_stats_rows, [8.5 * cm, 8.5 * cm], styles,
        header_bg=_PDF_BLUE,
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Top Positive Insights", styles))
    story.extend(_pdf_bullet_list(
        summary.get("key_positive_insights", []), styles,
        empty_text="No standout positive insights were identified.",
        bullet_color=_PDF_POSITIVE,
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Top Complaints", styles))
    story.extend(_pdf_bullet_list(
        summary.get("key_complaints", []), styles,
        empty_text="No significant complaints were identified.",
        bullet_color=_PDF_NEGATIVE,
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Recommendations", styles))
    story.extend(_pdf_bullet_list(
        summary.get("recommendations", []), styles,
        empty_text="No specific recommendations were generated.",
    ))
    story.append(Spacer(1, 16))

    story.extend(_pdf_section_heading("Final Conclusion", styles))
    platforms_covered = summary.get("platforms_covered", 0)
    conclusion = (
        f"Based on {total} product-attributed review(s) across {platforms_covered} platform(s) "
        f"and {products_scraped} selected product(s) — {product_title} — the overall product "
        f"score is {brand_score} out of 100, placing product sentiment in the '{brand_label}' "
        f"category. Of the product-attributed feedback collected, {positive_pct}% was positive, "
        f"{negative_pct}% was negative, and {neutral_pct}% was neutral. Brand-wide reputation "
        f"signals (Google Maps and general social mentions), covered separately above, are not "
        f"included in these product figures. These results, together with the insights and "
        f"recommendations above, offer a data-driven view of current product health and the "
        f"areas most likely to benefit from continued attention."
    )
    story.append(Paragraph(conclusion, styles["body"]))

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=2 * cm, bottomMargin=2.2 * cm,
        title=f"{product_title} — ManobhavaAI Product Intelligence Report",
    )
    doc.build(story, onFirstPage=_pdf_footer, onLaterPages=_pdf_footer)
    return pdf_path


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8001, reload=True)
