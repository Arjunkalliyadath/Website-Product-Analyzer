import asyncio
import logging
import re
import ssl
import sys
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

import config
from company_discovery import discover_company
from product_discovery import discover_products
from sentiment import analyze_sentiment
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

        product_data = await discover_products(company_data)
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

        pipe = get_sentiment_pipeline()

        comment_rows: List[Dict[str, Any]] = []
        for platform, comment in unique:
            sentiment = analyze_sentiment(pipe, comment)
            product_label = product_lookup_by_key.get((platform, comment.lower()), "General")
            comment_rows.append({
                "comment":   comment,
                "platform":  platform,
                "sentiment": sentiment,
                "product":   product_label,
                "timestamp": datetime.utcnow().isoformat(),
            })

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
