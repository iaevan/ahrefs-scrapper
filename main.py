"""
Ahrefs Blog Scraper → Obsidian Markdown
----------------------------------------
Scrapes all articles from https://ahrefs.com/blog
and saves them as interlinked .md files ready for Obsidian.

Requirements:
    pip install requests beautifulsoup4 html2text

Usage:
    python ahrefs_scraper.py

Output:
    ./ahrefs-blog/
        000-INDEX.md        ← master index with wiki-links
        slug-title.md       ← one file per article
"""

import os
import re
import time
import random
import logging
import requests
import html2text
from bs4 import BeautifulSoup

# ─── Configuration ────────────────────────────────────────────────────────────

BASE_URL    = "https://ahrefs.com/blog/"
OUTPUT_DIR  = "ahrefs-blog"
HEADERS     = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

# Polite delay range between requests (seconds)
DELAY_MIN = 2.0
DELAY_MAX = 4.5

# Extra longer pause every N articles (to avoid rate-limiting)
LONG_PAUSE_EVERY = 15       # articles
LONG_PAUSE_SECONDS = 30     # seconds

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── html2text config ─────────────────────────────────────────────────────────

converter = html2text.HTML2Text()
converter.ignore_links   = False   # keep hyperlinks
converter.ignore_images  = False   # keep image references
converter.body_width     = 0       # no line-wrapping (important for Obsidian)
converter.protect_links  = True
converter.unicode_snob   = True
converter.mark_code      = True    # wrap code blocks in ```

# ─── Helpers ──────────────────────────────────────────────────────────────────

def polite_sleep(label=""):
    """Random sleep between DELAY_MIN and DELAY_MAX seconds."""
    secs = random.uniform(DELAY_MIN, DELAY_MAX)
    log.info(f"  ⏳ Sleeping {secs:.1f}s {label}")
    time.sleep(secs)


def slugify(text: str) -> str:
    """Convert a title to a clean filename slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)        # remove special chars
    text = re.sub(r"[\s_]+", "-", text)          # spaces → hyphens
    text = re.sub(r"-+", "-", text)              # collapse multiple hyphens
    return text[:80].strip("-")                  # max 80 chars


def fetch(url: str) -> requests.Response | None:
    """GET a URL with error handling."""
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        if res.status_code == 200:
            return res
        log.warning(f"  HTTP {res.status_code} → {url}")
        return None
    except requests.RequestException as e:
        log.error(f"  Request failed: {e}")
        return None

# ─── Step 1: Collect all article URLs ────────────────────────────────────────

def get_all_article_urls() -> list[str]:
    """
    Walk through paginated blog archive and collect every article URL.
    Stops when a page returns no articles or a non-200 status.
    """
    all_urls = []
    page = 1

    log.info("📋 Collecting article URLs from archive pages…")

    while True:
        page_url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"
        log.info(f"  Archive page {page}: {page_url}")

        res = fetch(page_url)
        if not res:
            log.info(f"  Stopping at page {page} (no response).")
            break

        soup = BeautifulSoup(res.text, "html.parser")

        # Ahrefs blog uses <h2> / <h3> wrapping <a> tags for article titles.
        # We also try common post-title class patterns as fallbacks.
        article_links = (
            soup.select("h2 a[href*='/blog/']") +
            soup.select("h3 a[href*='/blog/']") +
            soup.select(".post-title a") +
            soup.select("article a[href*='/blog/']")
        )

        # De-dupe within this page
        page_urls = list({
            a["href"] for a in article_links
            if a.get("href", "").startswith("https://ahrefs.com/blog/")
            and a["href"] != BASE_URL             # exclude archive root
            and "/page/" not in a["href"]         # exclude pagination links
        })

        if not page_urls:
            log.info(f"  No articles found on page {page}. Archive complete.")
            break

        log.info(f"  Found {len(page_urls)} articles on page {page}.")
        all_urls.extend(page_urls)
        page += 1

        polite_sleep(f"(after archive page {page - 1})")

    # Final de-dupe across all pages
    unique = list(dict.fromkeys(all_urls))
    log.info(f"\n✅ Total unique article URLs collected: {len(unique)}\n")
    return unique

# ─── Step 2: Scrape a single article ─────────────────────────────────────────

def scrape_article(url: str) -> dict | None:
    """
    Fetch one article and return a dict with:
        title, date, author, tags, md_body, filename
    Returns None if scraping fails.
    """
    res = fetch(url)
    if not res:
        return None

    soup = BeautifulSoup(res.text, "html.parser")

    # ── Title ──
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else "Untitled"

    # ── Date ── (stored in frontmatter only, not in filename)
    date_el = soup.find("time")
    if date_el:
        date = date_el.get("datetime", date_el.get_text(strip=True))[:10]
    else:
        # Try common meta patterns
        meta_date = soup.find("meta", {"property": "article:published_time"})
        date = meta_date["content"][:10] if meta_date else "unknown"

    # ── Author ──
    author_el = soup.select_one(".author-name, [rel='author'], .byline a")
    author = author_el.get_text(strip=True) if author_el else "Ahrefs"

    # ── Tags / Categories ──
    tag_els = soup.select(".post-tags a, .tags a, .category a")
    tags = [t.get_text(strip=True) for t in tag_els]
    tags = list(dict.fromkeys(["ahrefs", "seo"] + tags))   # ensure base tags

    # ── Main content ──
    # Try progressively broader selectors
    content = (
        soup.select_one("article .entry-content") or
        soup.select_one("article .post-content") or
        soup.select_one(".entry-content") or
        soup.select_one(".post-content") or
        soup.select_one("article") or
        soup.select_one("main")
    )

    if not content:
        log.warning(f"  ⚠️  No content found for: {url}")
        return None

    # Remove nav / sidebar / footer noise inside the article tag
    for noise in content.select("nav, aside, footer, .sidebar, .related-posts, .comments"):
        noise.decompose()

    md_body = converter.handle(str(content))

    # ── Filename ──
    filename = f"{slugify(title)}.md"

    return {
        "title":    title,
        "date":     date,
        "author":   author,
        "tags":     tags,
        "url":      url,
        "md_body":  md_body,
        "filename": filename,
    }

# ─── Step 3: Build Obsidian frontmatter ──────────────────────────────────────

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

# ─── Step 4: Save article to disk ────────────────────────────────────────────

def save_article(article: dict, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, article["filename"])

    content = build_frontmatter(article) + article["md_body"]

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath

# ─── Step 5: Convert internal links to Obsidian wiki-links ───────────────────

def convert_internal_links(output_dir: str, url_to_filename: dict[str, str]):
    """
    Post-processing pass: replace Markdown links that point to other
    scraped Ahrefs blog articles with Obsidian [[wiki-links]].

    Before: [Anchor Text Guide](https://ahrefs.com/blog/anchor-text/)
    After:  [[anchor-text-guide|Anchor Text Guide]]
    """
    log.info("\n🔗 Converting internal links to Obsidian wiki-links…")

    md_files = [
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.endswith(".md") and f != "000-INDEX.md"
    ]

    pattern = re.compile(
        r'\[([^\]]+)\]\((https://ahrefs\.com/blog/[^\)]+)\)'
    )

    changed = 0
    for filepath in md_files:
        with open(filepath, "r", encoding="utf-8") as f:
            original = f.read()

        def replace_link(match):
            text = match.group(1)
            url  = match.group(2).rstrip("/") + "/"   # normalise trailing slash
            if url in url_to_filename:
                note = url_to_filename[url][:-3]       # strip .md
                return f"[[{note}|{text}]]"
            return match.group(0)                      # keep external links

        updated = pattern.sub(replace_link, original)

        if updated != original:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(updated)
            changed += 1

    log.info(f"  ✅ Wiki-links applied to {changed} files.")

# ─── Step 6: Build master index ──────────────────────────────────────────────

def build_index(articles: list[dict], output_dir: str):
    """
    Creates 000-INDEX.md — a master list of all articles as Obsidian wiki-links,
    sorted alphabetically by title.
    """
    log.info("\n📑 Building 000-INDEX.md…")

    sorted_articles = sorted(articles, key=lambda a: a["title"].lower())

    lines = [
        "# Ahrefs Blog — Master Index",
        "",
        f"> {len(articles)} articles scraped for personal learning.",
        "",
        "## Articles",
        "",
    ]

    for a in sorted_articles:
        note = a["filename"][:-3]   # strip .md for wiki-link
        lines.append(f"- [[{note}|{a['title']}]] — {a['date']}")

    index_path = os.path.join(output_dir, "000-INDEX.md")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"  ✅ Index saved: {index_path}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("🚀 Ahrefs Blog Scraper starting…\n")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── 1. Collect all article URLs ──
    all_urls = get_all_article_urls()

    if not all_urls:
        log.error("No article URLs found. Check your selectors or network.")
        return

    # ── 2. Scrape each article ──
    log.info(f"📰 Scraping {len(all_urls)} articles…\n")

    scraped_articles = []
    url_to_filename  = {}   # for wiki-link conversion later
    failed_urls      = []

    for i, url in enumerate(all_urls, start=1):
        log.info(f"[{i}/{len(all_urls)}] {url}")

        article = scrape_article(url)

        if article:
            # Handle duplicate filenames (append -2, -3, etc.)
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

            # Normalise URL for lookup (ensure trailing slash)
            norm_url = url.rstrip("/") + "/"
            url_to_filename[norm_url] = article["filename"]

            log.info(f"  ✅ Saved: {article['filename']}")
        else:
            log.warning(f"  ❌ Failed: {url}")
            failed_urls.append(url)

        # Polite sleep between articles
        polite_sleep()

        # Extra long pause every N articles
        if i % LONG_PAUSE_EVERY == 0 and i < len(all_urls):
            log.info(f"\n⏸️  Long pause ({LONG_PAUSE_SECONDS}s) after {i} articles…\n")
            time.sleep(LONG_PAUSE_SECONDS)

    # ── 3. Post-process: convert internal links → wiki-links ──
    convert_internal_links(OUTPUT_DIR, url_to_filename)

    # ── 4. Build master index ──
    build_index(scraped_articles, OUTPUT_DIR)

    # ── 5. Summary ──
    log.info("\n" + "═" * 50)
    log.info(f"  ✅ Successfully scraped : {len(scraped_articles)} articles")
    log.info(f"  ❌ Failed               : {len(failed_urls)} articles")
    log.info(f"  📁 Output directory     : ./{OUTPUT_DIR}/")
    log.info("═" * 50)

    if failed_urls:
        log.info("\nFailed URLs (you can retry these manually):")
        for u in failed_urls:
            log.info(f"  {u}")

    log.info("\n🎉 Done! Open the ahrefs-blog/ folder as an Obsidian vault.")


if __name__ == "__main__":
    main()