#!/usr/bin/env python3
# coding: utf-8

"""
MehrNews single-site scraper
Usage:
    python main.py output.xlsx

Produces an Excel file with columns:
url, title, first_paragraph, datetime
"""

from __future__ import annotations

import sys
import time
import logging
from typing import Dict, Optional, List, Tuple
from urllib.parse import urljoin, urlparse
import re

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import pytz
import pandas as pd

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse, urlunparse, quote, unquote

# Configuration
BASE_DOMAIN = "mehrnews.com"
BASE_URL = "https://www.mehrnews.com/"
USER_AGENT = "MyNewsScraper/1.0 (+https://example.com/contact)"
REQUEST_TIMEOUT = 15  # seconds
POLITE_DELAY = 2.0  # seconds between requests
RETRY = 2
LOG_FILE = "errors.log"

# Logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _sleep():
    time.sleep(POLITE_DELAY)


def _get_headers():
    return {"User-Agent": USER_AGENT}


def fetch_with_requests(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        _sleep()
        return resp.text
    except Exception as e:
        logger.info(f"requests fetch failed for {url}: {e}")
        return None


def fetch_with_playwright(url: str) -> Optional[str]:
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()
            page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
            try:
                page.wait_for_load_state("networkidle", timeout=REQUEST_TIMEOUT * 1000)
            except PlaywrightTimeoutError:
                # proceed anyway
                pass
            content = page.content()
            browser.close()
            _sleep()
            return content
    except Exception as e:
        logger.info(f"playwright fetch failed for {url}: {e}")
        return None


# ---------- Extraction helpers ----------
def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    # priority: h1 -> title tag -> meta og:title
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og.get("content").strip()
    return None


def _extract_first_paragraph(soup: BeautifulSoup) -> Optional[str]:
    # 1) first <p> after <h1>
    h1 = soup.find("h1")
    if h1:
        # next siblings
        for sib in h1.find_next_siblings():
            if sib.name == "p" and sib.get_text(strip=True):
                return sib.get_text(strip=True)
            # container with <p>
            p = sib.find("p") if hasattr(sib, "find") else None
            if p and p.get_text(strip=True):
                return p.get_text(strip=True)
        # fallback: first <p> after h1 in document
        p_after = h1.find_next("p")
        if p_after and p_after.get_text(strip=True):
            return p_after.get_text(strip=True)

    # 2) first <p> in the page
    p = soup.find("p")
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)

    # 3) meta description or og:description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta.get("content").strip()
    ogd = soup.find("meta", property="og:description")
    if ogd and ogd.get("content"):
        return ogd.get("content").strip()

    return None


def _parse_datetime_string(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if dt is None:
            return None
        # ensure timezone-aware: if naive, assume UTC
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        tehran = pytz.timezone("Asia/Tehran")
        dt_tehran = dt.astimezone(tehran)
        return dt_tehran.isoformat()

    except Exception:
        return None


DATE_REGEXES = [
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:?\d{2})?",
    r"\d{4}-\d{2}-\d{2}",
    r"\b\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b",
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
]


def _find_datetime_in_text(text: str) -> Optional[str]:
    if not text:
        return None
    for rx in DATE_REGEXES:
        m = re.search(rx, text, flags=re.IGNORECASE)
        if m:
            candidate = m.group(0)
            parsed = _parse_datetime_string(candidate)
            if parsed:
                return parsed
    # try fuzzy parsing on longer snippets
    try:
        parsed_dt = dateparser.parse(text, fuzzy=True)
        if parsed_dt:
            if parsed_dt.tzinfo is None:
                parsed_dt = pytz.UTC.localize(parsed_dt)
            return parsed_dt.astimezone(pytz.timezone("Asia/Tehran")).isoformat()
    except Exception:
        pass
    return None


def _extract_datetime(soup: BeautifulSoup, full_text: str) -> Optional[str]:
    # 1) <time datetime="...">
    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text(strip=True)
        parsed = _parse_datetime_string(dt)
        if parsed:
            return parsed
    # 2) meta article:published_time
    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        parsed = _parse_datetime_string(meta.get("content"))
        if parsed:
            return parsed
    # Some sites use meta[name="pubdate"] or meta[name="PublishDate"]
    meta2 = soup.find("meta", attrs={"name": "pubdate"}) or soup.find("meta", attrs={"name": "PublishDate"})
    if meta2 and meta2.get("content"):
        parsed = _parse_datetime_string(meta2.get("content"))
        if parsed:
            return parsed
    # 3) regex fallback on visible text
    parsed = _find_datetime_in_text(full_text)
    if parsed:
        return parsed
    return None


def _parse_html(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    title = _extract_title(soup)
    first_paragraph = _extract_first_paragraph(soup)
    full_text = soup.get_text(separator="\n", strip=True)
    datetime_iso = _extract_datetime(soup, full_text)
    return title, first_paragraph, datetime_iso


# ---------- Public extract function ----------
def extract(url: str) -> Dict[str, Optional[str]]:
    """
    Extracts fields from a single article URL.
    Returns dict: {'url','title','first_paragraph','datetime'}
    """
    result = {"url": url, "title": None, "first_paragraph": None, "datetime": None}

    last_exc = None
    for attempt in range(RETRY + 1):
        try:
            html = fetch_with_requests(url)
            if html:
                title, first_para, dt_iso = _parse_html(html)
                # If missing essential fields, fallback to Playwright
                if not (title and first_para and dt_iso):
                    html2 = fetch_with_playwright(url)
                    if html2:
                        title2, first_para2, dt_iso2 = _parse_html(html2)
                        title = title or title2
                        first_para = first_para or first_para2
                        dt_iso = dt_iso or dt_iso2
                result.update({"title": title, "first_paragraph": first_para, "datetime": dt_iso})
                return result
            else:
                # requests failed -> try playwright immediately
                html2 = fetch_with_playwright(url)
                if html2:
                    title2, first_para2, dt_iso2 = _parse_html(html2)
                    result.update({"title": title2, "first_paragraph": first_para2, "datetime": dt_iso2})
                    return result
        except Exception as e:
            last_exc = e
            logger.exception(f"extract error for {url} attempt {attempt}: {e}")
            _sleep()
            continue

    if last_exc:
        logger.error(f"Failed to extract {url} after {RETRY+1} attempts: {last_exc}")
    return result


# ---------- List page crawler ----------
def crawl_list_page(list_url: str = BASE_URL, max_links: int = 100) -> List[str]:
    """
    Crawl the MehrNews homepage and return ONLY real article URLs.
    A real article always contains `/news/` followed by digits.
    Example:
       https://www.mehrnews.com/news/1234567/
    """
    html = fetch_with_requests(list_url) or fetch_with_playwright(list_url) or ""
    soup = BeautifulSoup(html, "lxml")

    links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue

        # --- Only accept real article links ---
        # Pattern: /news/NUMBER
        if re.search(r"/news/\d+", href) is None:
            continue

        # Convert relative → absolute
        abs_url = href if href.startswith("http") else urljoin(list_url, href)
        # Normalize URL (remove trailing slash, query params)
        abs_url = abs_url.split("?")[0].rstrip("/")
        abs_url = normalize_url(abs_url)

        # Domain filter
        if BASE_DOMAIN not in abs_url:
            continue

        if abs_url in seen:
            continue

        seen.add(abs_url)
        links.append(abs_url)

        if len(links) >= max_links:
            break

    return links




def normalize_url(url: str) -> str:
    # Step 1 — remove query params
    url = url.split("?")[0]

    # Step 2 — remove trailing slash
    url = url.rstrip("/")

    # Step 3 — normalize unicode/encoded URLs
    parsed = urlparse(url)
    decoded_path = unquote(parsed.path)        # /news/6674867/پیگیری...
    encoded_path = quote(decoded_path)         # URL-encoded form

    normalized = urlunparse((
        parsed.scheme,
        parsed.netloc,
        encoded_path,
        "", "", ""
    ))
    return normalized

# ---------- Save ----------
def save_to_excel(rows: List[Dict[str, Optional[str]]], output_path: str) -> None:
    df = pd.DataFrame(rows, columns=["url", "title", "first_paragraph", "datetime"])
    df.to_excel(output_path, index=False, engine="openpyxl")


# ---------- CLI ----------
def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) != 1:
        print("Usage: python main.py output.xlsx")
        sys.exit(2)
    output_file = argv[0]

    try:
        urls = crawl_list_page(BASE_URL, max_links=100)
    except Exception as e:
        logger.exception(f"Failed to crawl list page: {e}")
        urls = []

    results = []
    for url in urls:
        try:
            data = extract(url)
            results.append(data)
        except Exception as e:
            logger.exception(f"Unhandled exception for {url}: {e}")

    save_to_excel(results, output_file)
    print(f"Saved {len(results)} rows to {output_file}")


if __name__ == "__main__":
    main()
