# Website Product Analyzer

A FastAPI-based application for discovering company information, product opportunities, and social media sentiment from public sources.

## Features
- Discover company websites and social profiles
- Identify products and services from the discovered website
- Scrape comments from Google, Twitter, Instagram, and YouTube
- Analyze sentiment and summarize results
- Render a simple dashboard in the browser

## Tech Stack
- Python
- FastAPI
- Jinja2
- Playwright
- httpx
- BeautifulSoup
- pandas

## Setup
1. Create and activate a virtual environment
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the app:
   ```bash
   uvicorn app:app --reload
   ```

## Usage
Open the app in your browser and enter a company name to begin analysis.