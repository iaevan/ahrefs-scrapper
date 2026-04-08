"""
Ahrefs Blog Scraper -> Obsidian Markdown
----------------------------------------
Scrapes all articles from https://ahrefs.com/blog
and saves them as interlinked .md files ready for Obsidian.
"""

import os
import re
import time
import random
import logging
import requests
import html2text
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---

BASE_URL = "https://ahrefs.com/blog/archive/"
OUTPUT_DIR = "ahrefs-blog"
MAX_WORKERS = 4  # Start with 4; lower to 2 if you get blocked

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
]

# Polite delay range between requests per thread (seconds)
DELAY_MIN = 2.0
DELAY_MAX = 4.5

# --- Logging ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# --- html2text config ---

converter = html2text.HTML2Text()
converter.ignore_links   = False
converter.ignore_images  = False
converter.body_width     = 0
converter.protect_links  = True
converter.unicode_snob   = True
converter.mark_code      = True

# --- Helpers ---

def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://ahrefs.com/blog/"
    }

def polite_sleep(label=""):
    secs = random.uniform(DELAY_MIN, DELAY_MAX)
    log.info(f"  Sleeping {secs:.1f}s {label}")
    time.sleep(secs)

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text[:80].strip("-")

def fetch(url: str, session: requests.Session) -> requests.Response | None:
    try:
        res = session.get(url, headers=get_random_headers(), timeout=20)
        if res.status_code == 200:
            return res
        log.warning(f"  HTTP {res.status_code} -> {url}")
        return None
    except requests.RequestException as e:
        log.error(f"  Request failed: {e}")
        return None

# --- Step 1: Collect all article URLs ---

def get_all_article_urls() -> list[str]:
    all_urls = []
    page = 1
    archive_session = requests.Session()

    log.info("Collecting article URLs from archive pages...")

    while True:
        page_url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"
        log.info(f"  Archive page {page}: {page_url}")

        res = fetch(page_url, archive_session)
        if not res:
            log.info(f"  Stopping at page {page} (no response).")
            break

        soup = BeautifulSoup(res.text, "html.parser")

        article_links = (
            soup.select("h2 a[href*='/blog/']") +
            soup.select("h3 a[href*='/blog/']") +
            soup.select(".post-title a") +
            soup.select("article a[href*='/blog/']")
        )

        page_urls = list({
            a["href"] for a in article_links
            if a.get("href", "").startswith("https://ahrefs.com/blog/")
            and a["href"] != BASE_URL
            and "/page/" not in a["href"]
        })

        if not page_urls:
            log.info(f"  No articles found on page {page}. Archive complete.")
            break

        log.info(f"  Found {len(page_urls)} articles on page {page}.")
        all_urls.extend(page_urls)
        page += 1

        polite_sleep(f"(after archive page {page - 1})")

    unique = list(dict.fromkeys(all_urls))
    log.info(f"\nTotal unique article URLs collected: {len(unique)}\n")
    return unique

# --- Step 2: Scrape a single article (Worker) ---

def scrape_article_worker(url: str) -> dict:
    # Each thread uses a distinct session to isolate cookies
    worker_session = requests.Session()
    
    # Jitter to prevent concurrent immediate hits
    time.sleep(random.uniform(0.5, 2.0))
    
    res = fetch(url, worker_session)
    if not res:
        return {"url": url, "error": True}

    soup = BeautifulSoup(res.text, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else "Untitled"

    date_el = soup.find("time")
    if date_el:
        date = date_el.get("datetime", date_el.get_text(strip=True))[:10]
    else:
        meta_date = soup.find("meta", {"property": "article:published_time"})
        date = meta_date["content"][:10] if meta_date else "unknown"

    author_el = soup.select_one(".author-name, [rel='author'], .byline a")
    author = author_el.get_text(strip=True) if author_el else "Ahrefs"

    tag_els = soup.select(".post-tags a, .tags a, .category a")
    tags = [t.get_text(strip=True) for t in tag_els]
    tags = list(dict.fromkeys(["ahrefs", "seo"] + tags))

    content = (
        soup.select_one("article .entry-content") or
        soup.select_one("article .post-content") or
        soup.select_one(".entry-content") or
        soup.select_one(".post-content") or
        soup.select_one("article") or
        soup.select_one("main")
    )

    if not content:
        log.warning(f"  No content found for: {url}")
        return {"url": url, "error": True}

    for noise in content.select("nav, aside, footer, .sidebar, .related-posts, .comments"):
        noise.decompose()

    md_body = converter.handle(str(content))
    filename = f"{slugify(title)}.md"

    # Polite delay per thread
    polite_sleep(f"after {url}")

    return {
        "title":    title,
        "date":     date,
        "author":   author,
        "tags":     tags,
        "url":      url,
        "md_body":  md_body,
        "filename": filename,
        "error":    False
    }

# --- Step 3: Build Obsidian frontmatter ---

def build_frontmatter(article: dict) -> str:
    tags_yaml = "\n".join(f'  - {t}' for t in article["tags"])
    return f"""---
title: "{article['title'].replace('"', "'")}"
date: {article['date']}
author: {article['author']}
source: {article['url']}
tags:
{tags_yaml}
---

# {article['title']}

"""

# --- Step 4: Save article to disk ---

def save_article(article: dict, output_dir: str) -> str:
    filepath = os.path.join(output_dir, article["filename"])
    content = build_frontmatter(article) + article["md_body"]

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath

# --- Step 5: Convert internal links to Obsidian wiki-links ---

def convert_internal_links(output_dir: str, url_to_filename: dict[str, str]):
    log.info("\nConverting internal links to Obsidian wiki-links...")

    md_files = [
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.endswith(".md") and f != "000-INDEX.md"
    ]

    pattern = re.compile(r'\[([^\]]+)\]\((https://ahrefs\.com/blog/[^\)]+)\)')
    changed = 0

    for filepath in md_files:
        with open(filepath, "r", encoding="utf-8") as f:
            original = f.read()

        def replace_link(match):
            text = match.group(1)
            url  = match.group(2).rstrip("/") + "/"
            if url in url_to_filename:
                note = url_to_filename[url][:-3]
                return f"[[{note}|{text}]]"
            return match.group(0)

        updated = pattern.sub(replace_link, original)

        if updated != original:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(updated)
            changed += 1

    log.info(f"  Wiki-links applied to {changed} files.")

# --- Step 6: Build master index ---

def build_index(articles: list[dict], output_dir: str):
    log.info("\nBuilding 000-INDEX.md...")

    sorted_articles = sorted(articles, key=lambda a: a["title"].lower())

    lines = [
        "# Ahrefs Blog - Master Index",
        "",
        f"> {len(articles)} articles scraped for personal learning.",
        "",
        "## Articles",
        "",
    ]

    for a in sorted_articles:
        note = a["filename"][:-3]
        lines.append(f"- [[{note}|{a['title']}]] - {a['date']}")

    index_path = os.path.join(output_dir, "000-INDEX.md")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"  Index saved: {index_path}")

# --- Main ---

def main():
    log.info("Starting Accelerated Ahrefs Scraper...\n")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_urls = get_all_article_urls()
    if not all_urls:
        log.error("No article URLs found. Check your selectors or network.")
        return

    log.info(f"Scraping {len(all_urls)} articles using {MAX_WORKERS} threads...\n")

    scraped_articles = []
    url_to_filename  = {}
    failed_urls      = []

    # Execute scraping across multiple threads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(scrape_article_worker, url): url for url in all_urls}
        
        for count, future in enumerate(as_completed(future_to_url), 1):
            article = future.result()
            
            if article.get("error"):
                failed_urls.append(article["url"])
            else:
                # Handle duplicate filenames centrally to prevent race conditions
                base = article["filename"]
                candidate = base
                counter = 2
                existing_files = {a["filename"] for a in scraped_articles}
                
                while candidate in existing_files:
                    stem = base[:-3]
                    candidate = f"{stem}-{counter}.md"
                    counter += 1
                
                article["filename"] = candidate
                save_article(article, OUTPUT_DIR)
                scraped_articles.append(article)

                norm_url = article["url"].rstrip("/") + "/"
                url_to_filename[norm_url] = article["filename"]

                log.info(f"[{count}/{len(all_urls)}] Saved: {article['filename']}")

    # Post-process
    convert_internal_links(OUTPUT_DIR, url_to_filename)
    build_index(scraped_articles, OUTPUT_DIR)

    log.info("\n" + "=" * 50)
    log.info(f"  Successfully scraped : {len(scraped_articles)} articles")
    log.info(f"  Failed               : {len(failed_urls)} articles")
    log.info(f"  Output directory     : ./{OUTPUT_DIR}/")
    log.info("=" * 50)

    if failed_urls:
        log.info("\nFailed URLs (you can retry these manually):")
        for u in failed_urls:
            log.info(f"  {u}")

    log.info("\nDone! Open the ahrefs-blog/ folder as an Obsidian vault.")

if __name__ == "__main__":
    main()