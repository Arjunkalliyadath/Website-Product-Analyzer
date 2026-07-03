import asyncio
import logging
import re
import ssl
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# ── Windows asyncio policy (must be first) ──────────────────────────────────
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# ── Python 3.12 removed ssl.PROTOCOL_SSLv23; patch it back so older libs work
if not hasattr(ssl, "PROTOCOL_SSLv23"):
    ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLS  # type: ignore[attr-defined]

# ── collections shim for old libraries ─────────────────────────────────────
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

<<<<<<< HEAD
import config
from company_discovery import discover_company
from product_discovery import discover_products
=======
from company_discovery import discover_company
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
from sentiment import analyze_sentiment
from utils import clean_comment, normalize_text, remove_links, unique_comments
from scrapers.google_scraper import scrape_google_reviews
from scrapers.twitter_scraper import scrape_twitter_comments
from scrapers.instagram_scraper import scrape_instagram_comments
from scrapers.youtube_scraper import scrape_youtube_comments

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ManobhavaAI — Social Media Analyzer")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/downloads", StaticFiles(directory="downloads"), name="downloads")


templates = Jinja2Templates(directory="templates")

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
            sentiment_pipeline = None  # analyze_sentiment handles None gracefully
    return sentiment_pipeline


@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={})


@app.post("/analyze")
async def analyze(request: Request, company_name: str = Form(...)):
    company_name = company_name.strip()
    if not company_name:
        return templates.TemplateResponse(
            request=request, name="index.html",
            context={"error": "Company name is required."}
        )

    try:
        discovered = await discover_company(company_name)
        company_data = {
            "company_name": discovered.get("company_name") or company_name,
            "website":      discovered.get("website", ""),
            "twitter":      discovered.get("twitter", ""),
            "instagram":    discovered.get("instagram", ""),
            "youtube":      discovered.get("youtube", ""),
            "twitter_url":  discovered.get("twitter_url", ""),
            "instagram_url": discovered.get("instagram_url", ""),
            "youtube_url":  discovered.get("youtube_url", ""),
            "google_business": discovered.get("google_business", ""),
            "discovery_version": discovered.get("discovery_version", ""),
        }
        logger.info("Discovered company data: %s", company_data)

<<<<<<< HEAD
        product_data = await discover_products(company_data)
        scrape_targets: List[str] = product_data.get("scrape_targets", [])
        logger.info(
            "Product discovery: %d products, %d services, scrape_targets=%s (method=%s)",
            product_data.get("products_found", 0),
            product_data.get("services_found", 0),
            scrape_targets,
            product_data.get("discovery_method"),
        )

        # ------------------------------------------------------------------
        # Build the scraping job list.
        #
        # Twitter/Instagram/YouTube scrape the brand's own social profiles,
        # which don't change per product, so those still run once at brand
        # level exactly as before (untouched scrapers, untouched calls).
        #
        # Google Reviews genuinely differs per search query, so it runs once
        # for the brand overall (tagged "General" — preserves the original
        # behaviour) plus once per discovered product (tagged with the
        # product name). `scrape_targets` is already capped at
        # config.MAX_PRODUCTS by product_discovery.py (Change 1), and the
        # concurrency of these per-product scrapes is capped separately by
        # config.MAX_PARALLEL_TASKS below (Change 4). No changes to
        # google_scraper.py were needed for this — it already builds its
        # query from company_data["company_name"].
        # ------------------------------------------------------------------
        google_jobs: List[Dict[str, Any]] = [{"label": "General", "data": company_data}]
        for product in scrape_targets:
            product_company_data = dict(company_data)
            product_company_data["company_name"] = f"{company_data['company_name']} {product}"
            google_jobs.append({"label": product, "data": product_company_data})

        # CHANGE 4 — parallel product scraping, bounded by a semaphore.
        # Per-product Google review scraping now runs through
        # asyncio.gather() gated by asyncio.Semaphore(config.MAX_PARALLEL_TASKS)
        # (5 by default) instead of one unbounded gather. With up to
        # MAX_PRODUCTS (10) products, this caps us at 5 concurrent scraping
        # tasks at a time, keeping things fast without hammering the target
        # site with 10+ simultaneous browser sessions. The brand-level
        # Twitter/Instagram/YouTube scrapes each run exactly once regardless,
        # so they aren't gated by the same semaphore.
        google_semaphore = asyncio.Semaphore(config.MAX_PARALLEL_TASKS)

        async def _scrape_google_job(job: Dict[str, Any]) -> List[str]:
            async with google_semaphore:
                return await scrape_google_reviews(job["data"])

        results = await asyncio.gather(
            *(_scrape_google_job(job) for job in google_jobs),
            scrape_twitter_comments(company_data),
            scrape_instagram_comments(company_data),
            scrape_youtube_comments(company_data),
            return_exceptions=True,
        )

        google_results = results[:len(google_jobs)]
        twitter_comments, instagram_comments, youtube_comments = results[len(google_jobs):]

        google_by_product: List[Dict[str, Any]] = []
        google_all_comments: List[str] = []
        for job, outcome in zip(google_jobs, google_results):
            comments = list(outcome) if not isinstance(outcome, Exception) else []
            # CHANGE 5 — cap how many reviews a single product can
            # contribute, so one very "talkative" product/query doesn't
            # dominate the executive summary or the exported files.
            comments = comments[:config.MAX_COMMENTS_PER_PRODUCT]
            google_by_product.append({"product": job["label"], "comments": comments})
            google_all_comments.extend(comments)

        platform_comments = {
            "Google":    google_all_comments,
=======
        google_comments, twitter_comments, instagram_comments, youtube_comments = (
            await asyncio.gather(
                scrape_google_reviews(company_data),
                scrape_twitter_comments(company_data),
                scrape_instagram_comments(company_data),
                scrape_youtube_comments(company_data),
                return_exceptions=True,
            )
        )

        platform_comments = {
            "Google":    list(google_comments)    if not isinstance(google_comments,    Exception) else [],
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
            "Twitter":   list(twitter_comments)   if not isinstance(twitter_comments,   Exception) else [],
            "Instagram": list(instagram_comments) if not isinstance(instagram_comments, Exception) else [],
            "YouTube":   list(youtube_comments)   if not isinstance(youtube_comments,   Exception) else [],
        }

<<<<<<< HEAD
        # Map each raw comment text back to the product it was scraped for
        # (only meaningful for Google; other platforms are brand-level/"General").
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
        # Re-attach product labels after dedup (unique_comments dedupes on
        # platform+comment, so a lookup by the same key is safe here).
        product_lookup_by_key = {(p, c.lower()): prod for p, c, prod in combined}

=======
        combined = []
        for platform, comments in platform_comments.items():
            for comment in comments:
                cleaned = normalize_text(remove_links(clean_comment(comment)))
                if cleaned:
                    combined.append((platform, cleaned))

        unique = unique_comments(combined)
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
        pipe = get_sentiment_pipeline()

        comment_rows: List[Dict[str, Any]] = []
        for platform, comment in unique:
            sentiment = analyze_sentiment(pipe, comment)
<<<<<<< HEAD
            product_label = product_lookup_by_key.get((platform, comment.lower()), "General")
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
            comment_rows.append({
                "comment":   comment,
                "platform":  platform,
                "sentiment": sentiment,
<<<<<<< HEAD
                "product":   product_label,
=======
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
                "timestamp": datetime.utcnow().isoformat(),
            })

        df = pd.DataFrame(comment_rows)
        if df.empty:
<<<<<<< HEAD
            df = pd.DataFrame(columns=["comment", "platform", "sentiment", "product", "timestamp"])
=======
            df = pd.DataFrame(columns=["comment", "platform", "sentiment", "timestamp"])
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52

        positive = int((df["sentiment"] == "positive").sum()) if "sentiment" in df.columns else 0
        negative = int((df["sentiment"] == "negative").sum()) if "sentiment" in df.columns else 0
        neutral  = int((df["sentiment"] == "neutral").sum())  if "sentiment" in df.columns else 0
        total    = len(df)

        positive_pct = round((positive / total) * 100, 1) if total else 0.0
        negative_pct = round((negative / total) * 100, 1) if total else 0.0
        neutral_pct  = round((neutral  / total) * 100, 1) if total else 0.0

<<<<<<< HEAD
        brand_score, brand_label = _compute_brand_score(positive, negative, neutral, total)

        product_sentiment = _aggregate_by_key(comment_rows, "product")
        platform_sentiment = _aggregate_by_key(comment_rows, "platform")

        # Top positive/negative product — only among real products (exclude
        # the "General" bucket, which is brand-level, not product-level).
        real_products = {k: v for k, v in product_sentiment.items() if k != "General"}
        top_positive_product = (
            max(real_products.items(), key=lambda kv: kv[1]["positive_pct"])[0]
            if real_products else ""
        )
        top_negative_product = (
            max(real_products.items(), key=lambda kv: kv[1]["negative_pct"])[0]
            if real_products else ""
        )
        # Most Discussed Product = highest raw comment volume, not sentiment.
        most_discussed_product = (
            max(real_products.items(), key=lambda kv: kv[1]["total"])[0]
            if real_products else ""
        )

        # CHANGE 6/7 — executive summary replaces the raw "All Comments"
        # dump. Aggregates already computed above are passed straight in so
        # nothing gets recomputed twice.
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

        # CHANGE 6 — "Top Insights" replaces the old "All Comments (N)" wall.
        # Tabbed view (Top Positive / Top Negative / By Platform / Sample),
        # each capped at 10-20 items so the dashboard stays readable while
        # still letting you drill into any platform or sentiment.
        insight_tabs = _build_insight_tabs(comment_rows, platform_comments)
=======
        summary = generate_summary(company_name, comment_rows)
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52

        export_base = Path("downloads") / f"{re.sub(r'[^a-zA-Z0-9]+', '_', company_name).strip('_').lower()}"
        export_base.mkdir(parents=True, exist_ok=True)

        csv_path   = export_base / "comments.csv"
        excel_path = export_base / "comments.xlsx"
        json_path  = export_base / "comments.json"

        df.to_csv(csv_path, index=False)
        df.to_excel(excel_path, index=False)
        df.to_json(json_path, orient="records", indent=2)

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          company_data,
<<<<<<< HEAD
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
=======
                "platform_comments": platform_comments,
                "comments":         comment_rows,
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
                "positive":         positive,
                "negative":         negative,
                "neutral":          neutral,
                "total":            total,
                "positive_pct":     positive_pct,
                "negative_pct":     negative_pct,
                "neutral_pct":      neutral_pct,
                "platform_counts": {
                    "Google":    len(platform_comments["Google"]),
                    "Twitter":   len(platform_comments["Twitter"]),
                    "Instagram": len(platform_comments["Instagram"]),
                    "YouTube":   len(platform_comments["YouTube"]),
                },
                "chart_payload": {
                    "labels": ["Positive", "Negative", "Neutral"],
                    "values": [positive, negative, neutral],
                },
                "summary":        summary,
                "csv_path":       csv_path.as_posix(),
                "excel_path":     excel_path.as_posix(),
                "json_path":      json_path.as_posix(),
                "download_dir":   str(export_base).replace("\\", "/"),
            },
        )

    except Exception as exc:
        logger.exception("Analysis failed")
        _empty_summary = {
            "executive_summary": "Analysis unavailable due to an error.",
<<<<<<< HEAD
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
=======
            "key_insights":          [],
            "top_complaints":        [],
            "top_positive_topics":   [],
            "recommendations":       [],
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
        }
        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "company":          {"company_name": company_name},
<<<<<<< HEAD
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
=======
                "platform_comments": {"Google": [], "Twitter": [], "Instagram": [], "YouTube": []},
                "comments":         [],
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
                "positive": 0, "negative": 0, "neutral": 0, "total": 0,
                "positive_pct": 0.0, "negative_pct": 0.0, "neutral_pct": 0.0,
                "platform_counts": {"Google": 0, "Twitter": 0, "Instagram": 0, "YouTube": 0},
                "chart_payload": {"labels": ["Positive", "Negative", "Neutral"], "values": [0, 0, 0]},
                "summary":     _empty_summary,
                "csv_path": "", "excel_path": "", "json_path": "",
                "download_dir": "downloads",
            },
        )


@app.get("/download/{path:path}")
async def download_file(path: str):
    resolved = Path("downloads") / path
    if resolved.exists() and resolved.is_file():
        return FileResponse(resolved)
    return JSONResponse(status_code=404, content={"detail": "File not found"})


@app.get("/discover")
async def discover(company_name: str):
    return await discover_company(company_name)


<<<<<<< HEAD
def _compute_brand_score(positive: int, negative: int, neutral: int, total: int) -> tuple:
    """
    Weighted 0-100 brand/emotion score: positive comments count fully,
    neutral comments count half, negative comments count zero.
    No ML needed — same simple weighting the spec asked for.
    """
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
    """
    Group comment_rows by `key` (e.g. "product" or "platform") and compute
    positive/negative/neutral counts + percentages + a brand-score style
    weighted score for each group.
    """
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
    """
    CHANGE 7 — lightweight keyword-frequency feature extraction. No new ML
    dependency: same philosophy as sentiment.py's keyword fallback — scan a
    small, readable lexicon (config.FEATURE_KEYWORDS) and report whichever
    term shows up most across the collected review text.
    """
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
    """
    Pick a handful of representative, medium-length comments for a given
    sentiment — used for both "Key Positive Insights" / "Key Customer
    Complaints" in the executive summary, and for the Top Insights cards.
    Medium-length comments tend to be more informative than one-word ones
    or overly long rambling ones, so we sort toward ~15 words.
    """
    candidates = [row["comment"] for row in comment_rows if row["sentiment"] == sentiment]
    candidates.sort(key=lambda text: abs(len(text.split()) - 15))
    return candidates[:limit]


def _build_insight_tabs(
    comment_rows: List[Dict[str, Any]],
    platform_comments: Dict[str, List[str]],
    per_tab_limit: int = 10,
) -> Dict[str, Any]:
    """
    Tabbed comment view: rather than one giant "All Comments" wall, give a
    few focused tabs, each capped so the dashboard stays readable:

      - Top Positive — up to 10 representative positive reviews
      - Top Negative — up to 10 representative negative reviews
      - By Platform  — one sub-tab per platform that actually returned
                        data (never a fixed Google/Twitter/Instagram/
                        YouTube list — only what was actually scraped),
                        each showing up to 10 comments sampled across
                        positive/negative/neutral so one sentiment doesn't
                        dominate the view.
    """
    def _representative(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        ranked = sorted(rows, key=lambda r: abs(len(r["comment"].split()) - 15))
        return ranked[:limit]

    def _diverse_sample(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        """Round-robin across sentiment so a platform's tab isn't just its
        most common sentiment. Deterministic (not truly random) so a demo
        looks the same on every refresh."""
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

    # Only platforms that actually returned comments get a sub-tab — if
    # only Google + Instagram + YouTube were scraped, only those three
    # show up, never a fixed 4-platform list with empty tabs.
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
    """
    CHANGE 7 — Executive Summary generator.

    Produces every field the brief asked for: Overall Brand Score / Overall
    Sentiment, Products Analysed, Platforms Covered, Total Reviews, Top
    Positive/Negative Product, Most Discussed Product, Most Mentioned
    Feature, Key Positive Insights, Key Customer Complaints, and Suggested
    Improvements — all derived from data already collected, no extra
    scraping needed.
    """
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
=======
def generate_summary(company_name: str, comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    sentiments = [item["sentiment"] for item in comments]
    positive = sentiments.count("positive")
    negative = sentiments.count("negative")
    neutral  = sentiments.count("neutral")
    total    = len(comments)

    # Build dynamic insights
    key_insights = [
        f"Comments were collected from Google, X/Twitter, Instagram, and YouTube.",
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
        f"Sentiment breakdown: {positive} positive, {negative} negative, {neutral} neutral out of {total} total.",
    ]
    if total and (positive / total) >= 0.6:
        key_insights.append("Overall brand perception is predominantly positive.")
    elif total and (negative / total) >= 0.4:
        key_insights.append("A significant portion of feedback is negative — attention needed.")

    return {
        "executive_summary": (
<<<<<<< HEAD
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
        # Kept for backward compatibility with anything still reading the
        # old field names.
        "top_complaints":         key_complaints,
        "top_positive_topics":    key_positive_insights,
        "recommendations":        recommendations,
=======
            f"{company_name} received {total} comments across social platforms "
            f"with {positive} positive, {negative} negative, and {neutral} neutral signals."
        ),
        "key_insights":        key_insights,
        "top_complaints":      ["Service and delivery concerns were mentioned in the feedback."],
        "top_positive_topics": ["Product quality and variety were common positive themes."],
        "recommendations":     ["Monitor feedback trends and respond to recurring complaints quickly."],
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
