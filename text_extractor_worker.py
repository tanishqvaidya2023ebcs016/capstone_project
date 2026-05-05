#/usr/bin/env python3
"""
Text Extractor Worker
Reads links.txt AND grpc_links.txt, fetches URLs, extracts clean text,
and stores results in SQLite.
 
FIX: Previously only read links.txt (Tanishq's ranked crawler output).
     Shivam's links arrive via gRPC into grpc_links.txt in the format:
       [timestamp] [crawler_id] url
     These were silently ignored. Now we read both files.
"""
 
import os
import re
import time
import sqlite3
import logging
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
 
# Configuration
LINKS_FILE      = os.environ.get('LINKS_FILE',      './output/links.txt')
GRPC_LINKS_FILE = os.environ.get('GRPC_LINKS_FILE', './output/grpc_links.txt')
DB_FILE         = os.environ.get('DB_FILE',         './output/extracted.db')
POLL_INTERVAL   = int(os.environ.get('POLL_INTERVAL',   '60'))
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '15'))
USER_AGENT = 'Mozilla/5.0 (compatible; TextExtractorBot/1.0)'
 
# Default score assigned to links that came in via gRPC (no scoring data)
GRPC_DEFAULT_SCORE = 5.0
 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger('TextExtractor')
 
 
class TextExtractorWorker:
    def __init__(self):
        self.session = self._create_session()
        self.init_db()
 
    def _create_session(self):
        session = requests.Session()
        retries = Retry(total=2, backoff_factor=1,
                        status_forcelist=[500, 502, 503, 504])
        session.mount('http://',  HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        session.headers.update({'User-Agent': USER_AGENT})
        return session
 
    def init_db(self):
        """Create SQLite table if not exists."""
        db_path = Path(DB_FILE)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS extracted_content (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    score REAL,
                    title TEXT,
                    text_content TEXT,
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_url ON extracted_content(url)'
            )
        logger.info(f"Database initialized at {DB_FILE}")
 
    def is_url_processed(self, url):
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.execute(
                'SELECT 1 FROM extracted_content WHERE url = ?', (url,)
            )
            return cur.fetchone() is not None
 
    def save_content(self, url, score, title, text):
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    'INSERT INTO extracted_content '
                    '(url, score, title, text_content) VALUES (?, ?, ?, ?)',
                    (url, score, title, text)
                )
                logger.info(f"✅ Saved to DB: {url}")
                return True
            except sqlite3.IntegrityError:
                logger.debug(f"URL already in DB: {url}")
                return False
 
    def extract_text(self, html):
        """Extract clean text from HTML, also return title."""
        soup = BeautifulSoup(html, 'html.parser')
        title = ''
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
        for tag in soup.find_all(['script', 'style', 'nav', 'footer',
                                   'header', 'aside', 'noscript', 'iframe']):
            tag.decompose()
        text = soup.get_text(separator=' ')
        text = re.sub(r'\s+', ' ', text).strip()
        return title, text
 
    def parse_ranked_links_file(self):
        """
        Read links.txt (Tanishq's ranked crawler output).
        Format:  [rank] [SCORE: X.XX] [crawler_id] url
        Yields (url, score) for unprocessed URLs.
        """
        if not os.path.exists(LINKS_FILE):
            logger.warning(f"Ranked links file not found: {LINKS_FILE}")
            return
 
        logger.info(f"Reading ranked links from {LINKS_FILE}...")
        seen_urls  = set()
        line_count = 0
        match_count = 0
 
        try:
            with open(LINKS_FILE, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line_count += 1
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue
 
                    score_match = re.search(r'\[SCORE:\s*([\d.]+)\]', line)
                    if not score_match:
                        continue
 
                    score = float(score_match.group(1))
                    url   = None
                    for part in line.split():
                        if part.startswith('http://') or part.startswith('https://'):
                            url = part.rstrip('.,;)')
                            break
 
                    if url:
                        match_count += 1
                        if url not in seen_urls and not self.is_url_processed(url):
                            seen_urls.add(url)
                            yield url, score
        except Exception as e:
            logger.error(f"Error reading ranked links file: {e}")
 
        logger.info(
            f"Ranked file: scanned {line_count} lines, found {match_count} URLs"
        )
 
    def parse_grpc_links_file(self):
        """
        FIX: Read grpc_links.txt (Shivam's crawler output via gRPC file server).
        Format:  [ISO-timestamp] [crawler_id] url
        These have no score, so we assign GRPC_DEFAULT_SCORE.
        Yields (url, score) for unprocessed URLs.
        """
        if not os.path.exists(GRPC_LINKS_FILE):
            logger.debug(f"gRPC links file not found yet: {GRPC_LINKS_FILE}")
            return
 
        logger.info(f"Reading gRPC links from {GRPC_LINKS_FILE}...")
        seen_urls  = set()
        line_count = 0
        match_count = 0
 
        try:
            with open(GRPC_LINKS_FILE, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line_count += 1
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue
 
                    # Format: [2026-04-12T03:05:31] [crawler-windows] https://...
                    url = None
                    for part in line.split():
                        if part.startswith('http://') or part.startswith('https://'):
                            url = part.rstrip('.,;)')
                            break
 
                    if url:
                        match_count += 1
                        if url not in seen_urls and not self.is_url_processed(url):
                            seen_urls.add(url)
                            yield url, GRPC_DEFAULT_SCORE
        except Exception as e:
            logger.error(f"Error reading gRPC links file: {e}")
 
        logger.info(
            f"gRPC file: scanned {line_count} lines, found {match_count} URLs"
        )
 
    def process_url(self, url, score):
        """Fetch URL, extract content, and save. If fetch fails, save placeholder."""
        try:
            logger.info(f"📄 Processing: {url}")
            resp = self.session.get(
                url, timeout=REQUEST_TIMEOUT, allow_redirects=True
            )
            resp.raise_for_status()
 
            ct = resp.headers.get('content-type', '').lower()
            if 'text/html' not in ct and 'application/xhtml' not in ct:
                logger.info(f"⏭️ Skipping non-HTML: {url} ({ct})")
                self.save_content(url, score, '', f"[Non-HTML content: {ct}]")
                return
 
            if len(resp.content) > 10 * 1024 * 1024:
                logger.warning(f"⏭️ Skipping large content: {url}")
                self.save_content(url, score, '', "[Content too large]")
                return
 
            title, text = self.extract_text(resp.text)
 
            if not text or len(text) < 50:
                logger.info(f"⏭️ Insufficient content: {url}")
                self.save_content(url, score, title, "[Insufficient text content]")
                return
 
            self.save_content(url, score, title, text)
            logger.info(
                f"📝 Extracted: {url} (score={score:.2f}, {len(text)} chars)"
            )
 
        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout: {url}")
            self.save_content(url, score, '', "[Timeout]")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {url} - {e}")
            self.save_content(url, score, '', f"[Request error: {e}]")
        except Exception as e:
            logger.error(f"❌ Failed: {url} - {e}")
            self.save_content(url, score, '', f"[Error: {e}]")
 
    def run_once(self):
        """Process all new URLs from both links files."""
        count = 0
 
        # 1. Tanishq's ranked links (links.txt)
        for url, score in self.parse_ranked_links_file():
            self.process_url(url, score)
            count += 1
            time.sleep(1)
 
        # 2. Shivam's gRPC links (grpc_links.txt)  ← FIX
        for url, score in self.parse_grpc_links_file():
            self.process_url(url, score)
            count += 1
            time.sleep(1)
 
        if count:
            logger.info(f"Processed {count} new URLs total")
        else:
            logger.info("No new URLs to process")
        return count
 
    def run_forever(self):
        logger.info(
            f"🔄 Worker started. "
            f"Watching {LINKS_FILE} + {GRPC_LINKS_FILE} every {POLL_INTERVAL}s"
        )
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Error in run loop: {e}")
            time.sleep(POLL_INTERVAL)
 
    def cleanup(self):
        try:
            self.session.close()
        except Exception:
            pass
 
 
def main():
    worker = TextExtractorWorker()
    try:
        if os.environ.get('RUN_ONCE', 'false').lower() == 'true':
            worker.run_once()
        else:
            worker.run_forever()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    finally:
        worker.cleanup()
 
 
if __name__ == '__main__':
    main()