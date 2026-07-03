<<<<<<< HEAD
# Social Media Analyzer

A production-style FastAPI application that discovers a company across web platforms, scrapes public comments from Google, X/Twitter, Instagram, and YouTube, analyzes sentiment, and presents a dashboard with charts and downloadable reports.

## Features
- Company discovery using search engines and Playwright
- Asynchronous scraping with asyncio.gather
- Comment collection from Google, Twitter, Instagram, and YouTube
- Sentiment analysis with Hugging Face transformers
- FastAPI + Jinja2 dashboard with pure CSS
- CSV/Excel/JSON downloads

## Installation
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

## Run
=======
# 🚀 Social Media Analyzer

A FastAPI-based web application that automatically collects public opinions about a company from multiple social media platforms and performs AI-powered sentiment analysis.

## 📌 Features

- 🔍 Company discovery using company name
- 🌐 Automatic social media link extraction
- ⭐ Google Reviews scraping
- 🐦 Twitter/X reply scraping
- 📺 YouTube comment scraping
- 📷 Instagram comment scraping
- 🤖 AI-powered sentiment analysis using Hugging Face Transformers
- 📊 Interactive dashboard with sentiment statistics
- 📁 Export results as CSV

---

## 🛠️ Tech Stack

- **Python**
- **FastAPI**
- **Playwright**
- **Jinja2**
- **HTML & CSS**
- **Pandas**
- **Hugging Face Transformers**
- **Asyncio (Performance Optimization)**

---

## 📂 Project Structure

```
Social-Media-Analyzer/
│
├── app.py
├── company_discovery.py
├── sentiment.py
├── utils.py
├── requirements.txt
│
├── scrapers/
│   ├── google_scraper.py
│   ├── twitter_scraper.py
│   ├── youtube_scraper.py
│   └── instagram_scraper.py
│
├── templates/
│   └── index.html
│
├── static/
│   ├── style.css
│   └── downloads/
│
└── README.md
```

---

## ⚙️ How It Works

1. Enter a company name.
2. Discover the company's official website.
3. Identify available social media platforms.
4. Extract reviews/comments using Playwright.
5. Merge all collected comments.
6. Perform sentiment analysis.
7. Display the overall sentiment in a dashboard.

---

## ▶️ Installation

```bash
git clone https://github.com/Arjunkalliyadath/Social-Media-Analyzer.git

cd Social-Media-Analyzer

pip install -r requirements.txt
```

---

## ▶️ Run the Project

>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
```bash
uvicorn app:app --reload
```

<<<<<<< HEAD
Then visit http://127.0.0.1:8000/
=======
Open your browser:

```
http://127.0.0.1:8000
```

---

## 📈 Future Improvements

- ✅ LinkedIn comment scraping
- Better company discovery
- Improved asynchronous scraping
- Enhanced dashboard visualizations
- More accurate social media detection

---

## 👨‍💻 Author

**Arjun K**

AI/ML Intern | Data Science Enthusiast
>>>>>>> 5b4009c04f14eaf1ec23d9aa8e7e56bc4049ef52
