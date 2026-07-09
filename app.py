import asyncio
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

if not hasattr(ssl, "PROTOCOL_SSLv23"):
    ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLS

import collections
try:
    import collections.abc as _abc
    for _name in ("Callable", "MutableMapping", "Mapping", "Iterable",
                  "MutableSequence", "MappingView"):
        if not hasattr(collections, _name) and hasattr(_abc, _name):
            setattr(collections, _name, getattr(_abc, _name))
except Exception:
    pass

import pandas as pd
from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from transformers import pipeline

# --- PDF report generation (reportlab only) --------------------------------
# Purely additive: used only by generate_pdf_report() below. Does not touch
# any existing scraping, sentiment, discovery, or dashboard-calculation code.
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

import config
from company_discovery import extract_website_metadata
from product_discovery import discover_products
from sentiment import analyze_sentiment, analyze_sentiment_batch
from url_utils import derive_company_name, is_url, normalize_url
from utils import clean_comment, normalize_text, remove_links, unique_comments
from scrapers.google_scraper import scrape_google_reviews
from scrapers.twitter_scraper import scrape_twitter_comments
from scrapers.instagram_scraper import scrape_instagram_comments
from scrapers.youtube_scraper import scrape_youtube_comments

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DOWNLOADS_DIR = BASE_DIR / "downloads"
TEMPLATES_DIR = BASE_DIR / "templates"

DOWNLOADS_DIR.mkdir(exist_ok=True)

app = FastAPI(title="ManobhavaAI — Social Media Analyzer")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/downloads", StaticFiles(directory=str(DOWNLOADS_DIR)), name="downloads")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

sentiment_pipeline = None

def get_sentiment_pipeline():
    global sentiment_pipeline
    if sentiment_pipeline is None:
        try:
            sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                tokenizer="cardiffnlp/twitter-roberta-base-sentiment-latest",
                truncation=True,
                max_length=512,
            )
            logger.info("HuggingFace sentiment pipeline loaded successfully.")
        except Exception as exc:
            logger.warning(
                "HuggingFace pipeline unavailable (%s). "
                "Keyword-based sentiment will be used instead.", exc
            )
            sentiment_pipeline = None
    return sentiment_pipeline

@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={})

@app.post("/analyze")
async def analyze(request: Request, company_name: str = Form(...)):
    # === Performance profiling instrumentation ===================
    # Timing-only additions. No business logic, algorithms, or
    # function signatures below are modified by this instrumentation.
    pipeline_start = time.perf_counter()
    stage_times: Dict[str, float] = {}

    def _log_stage(name: str, elapsed: float) -> None:
        stage_times[name] = stage_times.get(name, 0.0) + elapsed
        logger.info(
            "\n%s\nStage: %s\nTime: %.2f sec\n%s",
            "=" * 50, name, elapsed, "=" * 50,
        )

    async def _timed(name: str, coro, timeout: float = None):
        # Hard per-scraper cutoff (requirement: no platform may block the
        # whole pipeline). Each scraper already carries its own internal
        # time budget and returns whatever it collected before this fires,
        # so this asyncio.wait_for is a safety-net backstop, not the
        # primary mechanism for getting partial results.
        _start = time.perf_counter()
        try:
            if timeout is not None:
                try:
                    return await asyncio.wait_for(coro, timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning(
                        "%s exceeded its %.0fs hard timeout — returning "
                        "whatever was collected instead of blocking the pipeline.",
                        name, timeout,
                    )
                    return []
            return await coro
        finally:
            _log_stage(name, time.perf_counter() - _start)

    def _log_total_and_breakdown() -> None:
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
    # === end instrumentation setup =================================

    company_name = company_name.strip()
    if not company_name:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={"error": "Please enter a website URL (e.g. https://www.example.com)."}
        )

    try:
        _stage_start = time.perf_counter()

        # --- Website URL is now the single source of truth. -------------
        # Company Discovery (name -> guessed website via search) has been
        # removed. If the input isn't already a URL, we ask for one rather
        # than trying to guess a website from a name.
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

        # From here on, `company_name` mirrors the resolved/derived name so
        # every downstream step (Product Discovery, scraping, Sentiment,
        # Dashboard, exports) behaves exactly as it did before — unchanged.
        company_name = company_data["company_name"]

        _stage_start = time.perf_counter()
        product_data = await discover_products(company_data)
        _log_stage("Product Discovery", time.perf_counter() - _stage_start)
        products = product_data.get("products", [])

        if len(products) > 10:
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

        google_semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)

        # Hard per-platform ceilings. Each scraper's own internal time
        # budget is set a few seconds below these so it returns naturally
        # with partial results; these are the outer safety-net cutoffs
        # that guarantee no single platform can block the whole request.
        GOOGLE_JOB_TIMEOUT_SECONDS = 30
        TWITTER_TIMEOUT_SECONDS = 20
        INSTAGRAM_TIMEOUT_SECONDS = 20
        YOUTUBE_TIMEOUT_SECONDS = 20

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
                        "timeout — returning whatever was collected instead "
                        "of blocking the pipeline.",
                        job.get("label"), GOOGLE_JOB_TIMEOUT_SECONDS,
                    )
                    return []
                except Exception:
                    logger.exception("Google Reviews job for %r failed", job.get("label"))
                    return []

        async def _scrape_google_jobs_group() -> List[Any]:
            # Wraps the whole Google job set (General + per-product, still
            # bounded by the same semaphore) so we get one wall-clock timing
            # for the "Google Review Collection" stage without altering how
            # the individual jobs are scheduled or their results.
            _group_start = time.perf_counter()
            try:
                return await asyncio.gather(
                    *(_scrape_google_job(job) for job in google_jobs),
                    return_exceptions=True,
                )
            finally:
                _log_stage("Google Review Collection", time.perf_counter() - _group_start)

        results = await asyncio.gather(
            _scrape_google_jobs_group(),
            _timed("Twitter Scraper", scrape_twitter_comments(company_data), timeout=TWITTER_TIMEOUT_SECONDS),
            _timed("Instagram Scraper", scrape_instagram_comments(company_data), timeout=INSTAGRAM_TIMEOUT_SECONDS),
            _timed("YouTube Scraper", scrape_youtube_comments(company_data), timeout=YOUTUBE_TIMEOUT_SECONDS),
            return_exceptions=True,
        )

        google_results = results[0]
        twitter_comments, instagram_comments, youtube_comments = results[1], results[2], results[3]

        google_by_product: List[Dict[str, Any]] = []
        google_all_comments: List[str] = []
        for job, outcome in zip(google_jobs, google_results):
            comments = list(outcome) if not isinstance(outcome, Exception) else []

            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            google_by_product.append({"product": job["label"], "comments": comments})
            google_all_comments.extend(comments)

        platform_comments = {
            "Google":    google_all_comments,
            "Twitter":   list(twitter_comments)   if not isinstance(twitter_comments,   Exception) else [],
            "Instagram": list(instagram_comments) if not isinstance(instagram_comments, Exception) else [],
            "YouTube":   list(youtube_comments)   if not isinstance(youtube_comments,   Exception) else [],
        }

        comment_product_lookup: Dict[str, str] = {}
        for entry in google_by_product:
            for comment in entry["comments"]:
                comment_product_lookup.setdefault(comment, entry["product"])

        combined = []
        for platform, comments in platform_comments.items():
            for comment in comments:
                product_label = comment_product_lookup.get(comment, "General") if platform == "Google" else "General"
                cleaned = normalize_text(remove_links(clean_comment(comment)))
                if cleaned:
                    combined.append((platform, cleaned, product_label))

        unique = unique_comments([(p, c) for p, c, _ in combined])

        product_lookup_by_key = {(p, c.lower()): prod for p, c, prod in combined}

        _stage_start = time.perf_counter()
        pipe = get_sentiment_pipeline()

        # Batched sentiment call: one (chunked) pass over all unique comments
        # instead of one pipeline call per comment. analyze_sentiment_batch()
        # returns sentiments in the same order as `unique`, with identical
        # label mapping / keyword fallback semantics to analyze_sentiment().
        comment_texts = [comment for _, comment in unique]
        sentiments = analyze_sentiment_batch(pipe, comment_texts)

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

        positive = int((df["sentiment"] == "positive").sum()) if "sentiment" in df.columns else 0
        negative = int((df["sentiment"] == "negative").sum()) if "sentiment" in df.columns else 0
        neutral  = int((df["sentiment"] == "neutral").sum())  if "sentiment" in df.columns else 0
        total    = len(df)

        positive_pct = round((positive / total) * 100, 1) if total else 0.0
        negative_pct = round((negative / total) * 100, 1) if total else 0.0
        neutral_pct  = round((neutral  / total) * 100, 1) if total else 0.0

        brand_score, brand_label = _compute_brand_score(positive, negative, neutral, total)

        product_sentiment = _aggregate_by_key(comment_rows, "product")
        platform_sentiment = _aggregate_by_key(comment_rows, "platform")

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
            comment_rows=comment_rows,
            brand_score=brand_score,
            brand_label=brand_label,
            products_scraped=len(scrape_targets),
            platform_comments=platform_comments,
            most_discussed_product=most_discussed_product,
            top_positive_product=top_positive_product,
            top_negative_product=top_negative_product,
        )

        insight_tabs = _build_insight_tabs(comment_rows, platform_comments)

        export_base = Path("downloads") / f"{re.sub(r'[^a-zA-Z0-9]+', '_', company_name).strip('_').lower()}"
        export_base.mkdir(parents=True, exist_ok=True)
        _log_stage("Report Generation", time.perf_counter() - _stage_start)

        # Human-readable "Report Generated" stamp for the dashboard header.
        # Purely additive — does not alter any existing field or API shape.
        report_generated = datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC")

        platform_counts = {
            "Google":    len(platform_comments["Google"]),
            "Twitter":   len(platform_comments["Twitter"]),
            "Instagram": len(platform_comments["Instagram"]),
            "YouTube":   len(platform_comments["YouTube"]),
        }

        # --- Professional PDF Report (additive; failure never breaks the
        # dashboard — CSV/Excel/JSON exports above are unaffected either way).
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

    except Exception as exc:
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
        }
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          {"company_name": company_name},
                "products": [], "services": [],
                "products_found": 0, "services_found": 0, "products_scraped": 0,
                "reviews_collected": 0,
                "top_positive_product": "", "top_negative_product": "",
                "most_discussed_product": "",
                "brand_score": 0, "brand_label": "No Data",
                "product_sentiment": {}, "platform_sentiment": {},
                "platform_comments": {"Google": [], "Twitter": [], "Instagram": [], "YouTube": []},
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
    # Mirrors the URL-validation prologue of /analyze exactly, then stops
    # right after Product Discovery so the user can hand-pick products —
    # regardless of how many were found — instead of only being routed
    # here automatically when the count exceeds 10.
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
        products = product_data.get("products", [])

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
    selected_products: List[str] = Form(...),
):
    # === Performance profiling instrumentation (mirrors /analyze) =========
    pipeline_start = time.perf_counter()
    stage_times: Dict[str, float] = {}

    def _log_stage(name: str, elapsed: float) -> None:
        stage_times[name] = stage_times.get(name, 0.0) + elapsed
        logger.info(
            "\n%s\nStage: %s\nTime: %.2f sec\n%s",
            "=" * 50, name, elapsed, "=" * 50,
        )

    async def _timed(name: str, coro, timeout: float = None):
        _start = time.perf_counter()
        try:
            if timeout is not None:
                try:
                    return await asyncio.wait_for(coro, timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning(
                        "%s exceeded its %.0fs hard timeout — returning "
                        "whatever was collected instead of blocking the pipeline.",
                        name, timeout,
                    )
                    return []
            return await coro
        finally:
            _log_stage(name, time.perf_counter() - _start)

    def _log_total_and_breakdown() -> None:
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
    # === end instrumentation setup =========================================

    website = website.strip()
    company_name = website  # fallback label until metadata resolves below

    # De-duplicate / clean the checkbox selections posted from select_products.html
    scrape_targets: List[str] = []
    seen = set()
    for product in selected_products:
        product = (product or "").strip()
        if product and product not in seen:
            seen.add(product)
            scrape_targets.append(product)

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

        # `website` arrives already normalized from select_products.html, but
        # we defend against a raw/unnormalized value just in case.
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

        # --- From here on this is the identical scrape -> sentiment ->
        # dashboard pipeline used by /analyze, just driven by the user's
        # selected_products instead of an auto-discovered scrape_targets
        # list. No pipeline logic, function, or output shape is altered. ---
        google_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product in scrape_targets:
            product_company_data = dict(company_data)
            product_company_data["company_name"] = f"{company_data['company_name']} {product}"
            google_jobs.append({"label": product, "data": product_company_data})

        google_semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)

        GOOGLE_JOB_TIMEOUT_SECONDS = 30
        TWITTER_TIMEOUT_SECONDS = 20
        INSTAGRAM_TIMEOUT_SECONDS = 20
        YOUTUBE_TIMEOUT_SECONDS = 20

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
                        "timeout — returning whatever was collected instead "
                        "of blocking the pipeline.",
                        job.get("label"), GOOGLE_JOB_TIMEOUT_SECONDS,
                    )
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

        results = await asyncio.gather(
            _scrape_google_jobs_group(),
            _timed("Twitter Scraper", scrape_twitter_comments(company_data), timeout=TWITTER_TIMEOUT_SECONDS),
            _timed("Instagram Scraper", scrape_instagram_comments(company_data), timeout=INSTAGRAM_TIMEOUT_SECONDS),
            _timed("YouTube Scraper", scrape_youtube_comments(company_data), timeout=YOUTUBE_TIMEOUT_SECONDS),
            return_exceptions=True,
        )

        google_results = results[0]
        twitter_comments, instagram_comments, youtube_comments = results[1], results[2], results[3]

        google_by_product: List[Dict[str, Any]] = []
        google_all_comments: List[str] = []
        for job, outcome in zip(google_jobs, google_results):
            comments = list(outcome) if not isinstance(outcome, Exception) else []
            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            google_by_product.append({"product": job["label"], "comments": comments})
            google_all_comments.extend(comments)

        platform_comments = {
            "Google":    google_all_comments,
            "Twitter":   list(twitter_comments)   if not isinstance(twitter_comments,   Exception) else [],
            "Instagram": list(instagram_comments) if not isinstance(instagram_comments, Exception) else [],
            "YouTube":   list(youtube_comments)   if not isinstance(youtube_comments,   Exception) else [],
        }

        comment_product_lookup: Dict[str, str] = {}
        for entry in google_by_product:
            for comment in entry["comments"]:
                comment_product_lookup.setdefault(comment, entry["product"])

        combined = []
        for platform, comments in platform_comments.items():
            for comment in comments:
                product_label = comment_product_lookup.get(comment, "General") if platform == "Google" else "General"
                cleaned = normalize_text(remove_links(clean_comment(comment)))
                if cleaned:
                    combined.append((platform, cleaned, product_label))

        unique = unique_comments([(p, c) for p, c, _ in combined])
        product_lookup_by_key = {(p, c.lower()): prod for p, c, prod in combined}

        _stage_start = time.perf_counter()
        pipe = get_sentiment_pipeline()

        comment_texts = [comment for _, comment in unique]
        sentiments = analyze_sentiment_batch(pipe, comment_texts)

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

        positive = int((df["sentiment"] == "positive").sum()) if "sentiment" in df.columns else 0
        negative = int((df["sentiment"] == "negative").sum()) if "sentiment" in df.columns else 0
        neutral  = int((df["sentiment"] == "neutral").sum())  if "sentiment" in df.columns else 0
        total    = len(df)

        positive_pct = round((positive / total) * 100, 1) if total else 0.0
        negative_pct = round((negative / total) * 100, 1) if total else 0.0
        neutral_pct  = round((neutral  / total) * 100, 1) if total else 0.0

        brand_score, brand_label = _compute_brand_score(positive, negative, neutral, total)

        product_sentiment = _aggregate_by_key(comment_rows, "product")
        platform_sentiment = _aggregate_by_key(comment_rows, "platform")

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
            comment_rows=comment_rows,
            brand_score=brand_score,
            brand_label=brand_label,
            products_scraped=len(scrape_targets),
            platform_comments=platform_comments,
            most_discussed_product=most_discussed_product,
            top_positive_product=top_positive_product,
            top_negative_product=top_negative_product,
        )

        insight_tabs = _build_insight_tabs(comment_rows, platform_comments)

        export_base = Path("downloads") / f"{re.sub(r'[^a-zA-Z0-9]+', '_', company_name).strip('_').lower()}"
        export_base.mkdir(parents=True, exist_ok=True)
        _log_stage("Report Generation", time.perf_counter() - _stage_start)

        report_generated = datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC")

        platform_counts = {
            "Google":    len(platform_comments["Google"]),
            "Twitter":   len(platform_comments["Twitter"]),
            "Instagram": len(platform_comments["Instagram"]),
            "YouTube":   len(platform_comments["YouTube"]),
        }

        # --- Professional PDF Report (additive; failure never breaks the
        # dashboard — CSV/Excel/JSON exports above are unaffected either way).
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
        }
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          {"company_name": company_name},
                "products": scrape_targets, "services": [],
                "products_found": len(scrape_targets), "services_found": 0,
                "products_scraped": 0,
                "reviews_collected": 0,
                "top_positive_product": "", "top_negative_product": "",
                "most_discussed_product": "",
                "brand_score": 0, "brand_label": "No Data",
                "product_sentiment": {}, "platform_sentiment": {},
                "platform_comments": {"Google": [], "Twitter": [], "Instagram": [], "YouTube": []},
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
) -> Dict[str, Any]:
    sentiments = [item["sentiment"] for item in comment_rows]
    positive = sentiments.count("positive")
    negative = sentiments.count("negative")
    neutral  = sentiments.count("neutral")
    total    = len(comment_rows)

    platforms_covered = [name for name, comments in platform_comments.items() if comments]
    all_comment_text = [row["comment"] for row in comment_rows]
    most_mentioned_feature = _most_mentioned_feature(all_comment_text)

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

    return {
        "executive_summary": (
            f"{company_name} received {total} reviews across {len(platforms_covered)} platform(s) "
            f"covering {products_scraped} product(s), with a brand score of {brand_score}/100 "
            f"({brand_label})."
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
    }

# =============================================================================
# Professional PDF Report (reportlab only) — ADDITIVE FEATURE
# =============================================================================
# generate_pdf_report() is a pure, self-contained helper. It only *reads*
# values that were already computed by the existing pipeline (brand score,
# summary, sentiment aggregates, etc.) and lays them out as a PDF. It does
# not recompute, alter, or invent any statistic, and it is not called from
# anywhere except once per analysis run, after the CSV/Excel/JSON export.

_PDF_PRIMARY   = colors.HexColor("#5A4FCF")   # purple — main headings / bands
_PDF_BLUE      = colors.HexColor("#3B5BDB")   # blue — secondary headings
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
) -> Path:
    """
    Builds a professional multi-page PDF report from data the existing
    pipeline has already computed (summary, brand score, sentiment
    aggregates, platform counts, product sentiment, selected products).
    No new statistics are calculated here — every figure below is read
    directly from the arguments passed in.
    """
    styles = _pdf_styles()
    story: List[Any] = []

    company_name = company_data.get("company_name") or "Unknown Company"
    website = company_data.get("website") or "—"

    # ---------------------------------------------------------------- Cover
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
        [Paragraph("COMPANY NAME", styles["cover_field_label"])],
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

    # ------------------------------------------------------- Executive Summary
    story.extend(_pdf_section_heading("Executive Summary", styles))
    story.append(Paragraph(
        summary.get("executive_summary", "No executive summary available."),
        styles["body"],
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Overall Statistics
    story.extend(_pdf_section_heading("Overall Statistics", styles))
    stats_rows = [
        ["Brand Score", f"{brand_score} / 100"],
        ["Brand Label", brand_label],
        ["Positive %", f"{positive_pct}%"],
        ["Negative %", f"{negative_pct}%"],
        ["Neutral %", f"{neutral_pct}%"],
        ["Total Reviews", str(total)],
        ["Platforms Covered", str(summary.get("platforms_covered", 0))],
        ["Products Analysed", str(products_scraped)],
    ]
    story.append(_pdf_styled_table(
        ["Metric", "Value"], stats_rows, [8.5 * cm, 8.5 * cm], styles,
        header_bg=_PDF_PRIMARY,
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Selected Products
    story.extend(_pdf_section_heading("Selected Products", styles))
    story.extend(_pdf_bullet_list(
        selected_products or [], styles,
        empty_text="No products were selected for this analysis.",
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Platform Summary
    story.extend(_pdf_section_heading("Platform Summary", styles))
    platform_order = ["Google", "Twitter", "Instagram", "YouTube"]
    platform_rows = [[p, str(platform_counts.get(p, 0))] for p in platform_order]
    story.append(_pdf_styled_table(
        ["Platform", "Review Count"], platform_rows, [8.5 * cm, 8.5 * cm], styles,
        header_bg=_PDF_BLUE,
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Product Sentiment
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

    # ------------------------------------------------------- Top Positive Insights
    story.extend(_pdf_section_heading("Top Positive Insights", styles))
    story.extend(_pdf_bullet_list(
        summary.get("key_positive_insights", []), styles,
        empty_text="No standout positive insights were identified.",
        bullet_color=_PDF_POSITIVE,
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Top Complaints
    story.extend(_pdf_section_heading("Top Complaints", styles))
    story.extend(_pdf_bullet_list(
        summary.get("key_complaints", []), styles,
        empty_text="No significant complaints were identified.",
        bullet_color=_PDF_NEGATIVE,
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Recommendations
    story.extend(_pdf_section_heading("Recommendations", styles))
    story.extend(_pdf_bullet_list(
        summary.get("recommendations", []), styles,
        empty_text="No specific recommendations were generated.",
    ))
    story.append(Spacer(1, 16))

    # ------------------------------------------------------- Final Conclusion
    story.extend(_pdf_section_heading("Final Conclusion", styles))
    platforms_covered = summary.get("platforms_covered", 0)
    conclusion = (
        f"Based on {total} analysed review(s) across {platforms_covered} platform(s) and "
        f"{products_scraped} product(s), {company_name} currently holds a brand score of "
        f"{brand_score} out of 100, placing its overall sentiment in the '{brand_label}' "
        f"category. Of the feedback collected, {positive_pct}% was positive, {negative_pct}% "
        f"was negative, and {neutral_pct}% was neutral. These results, together with the "
        f"insights and recommendations above, offer a data-driven view of current brand "
        f"health and the areas most likely to benefit from continued attention."
    )
    story.append(Paragraph(conclusion, styles["body"]))

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=2 * cm, bottomMargin=2.2 * cm,
        title=f"{company_name} — ManobhavaAI Report",
    )
    doc.build(story, onFirstPage=_pdf_footer, onLaterPages=_pdf_footer)
    return pdf_path


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
