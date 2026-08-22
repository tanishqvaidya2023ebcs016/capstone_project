#!/usr/bin/env python3
"""
Text Extractor Worker – Fixed version with real browser headers, parallel fetching,
FORCE_REEXTRACT support, and strict binary file detection.
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
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================== CONFIGURATION ==========================
LINKS_FILE         = os.environ.get('LINKS_FILE',      './output/links.txt')
GRPC_LINKS_FILE    = os.environ.get('GRPC_LINKS_FILE', './output/grpc_links.txt')
DB_FILE            = os.environ.get('DB_FILE',         './output/extracted.db')
POLL_INTERVAL      = int(os.environ.get('POLL_INTERVAL',   '60'))
REQUEST_TIMEOUT    = int(os.environ.get('REQUEST_TIMEOUT', '20'))
MAX_WORKERS        = int(os.environ.get('MAX_WORKERS',     '8'))
FORCE_REEXTRACT    = os.environ.get('FORCE_REEXTRACT', 'false').lower() == 'true'
GRPC_DEFAULT_SCORE = 5.0

# Real Chrome browser headers
BROWSER_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/125.0.0.0 Safari/537.36'
    ),
    'Accept': (
        'text/html,application/xhtml+xml,application/xml;q=0.9,'
        'image/avif,image/webp,*/*;q=0.8'
    ),
    'Accept-Language':           'en-US,en;q=0.9',
    'Accept-Encoding':           'gzip, deflate, br',
    'Cache-Control':             'max-age=0',
    'DNT':                       '1',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest':            'document',
    'Sec-Fetch-Mode':            'navigate',
    'Sec-Fetch-Site':            'none',
    'Sec-Fetch-User':            '?1',
}

PAYWALL_MARKERS = [
    'sign in to continue', 'log in to continue', 'subscribe to read',
    'create a free account', 'join to access', 'please log in',
    'start your free trial', 'unlock this content', 'gated content',
    'premium content', 'members only', 'sign up for free',
    'create an account', 'register to read', 'login required',
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [TextExtractor] %(levelname)s: %(message)s'
)
logger = logging.getLogger('TextExtractor')


class TextExtractorWorker:
    def __init__(self):
        self.session = self._create_session()
        self.init_db()

    def _create_session(self):
        session = requests.Session()
        retries = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        session.mount('http://',  HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        session.headers.update(BROWSER_HEADERS)
        return session

    def init_db(self):
        Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS extracted_content (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    url           TEXT UNIQUE NOT NULL,
                    score         REAL,
                    title         TEXT,
                    text_content  TEXT,
                    extracted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_paywall    INTEGER DEFAULT 0,
                    char_count    INTEGER DEFAULT 0
                )
            ''')
            # Migrate existing DBs
            for col, defn in [
                ('is_paywall', 'INTEGER DEFAULT 0'),
                ('char_count', 'INTEGER DEFAULT 0'),
            ]:
                try:
                    conn.execute(f'ALTER TABLE extracted_content ADD COLUMN {col} {defn}')
                except Exception:
                    pass
            conn.execute('CREATE INDEX IF NOT EXISTS idx_url   ON extracted_content(url)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_score ON extracted_content(score DESC)')
        logger.info(f"Database ready: {DB_FILE}")

    def is_url_processed(self, url):
        if FORCE_REEXTRACT:
            return False
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(
                'SELECT 1 FROM extracted_content WHERE url = ?', (url,)
            ).fetchone()
            return row is not None

    def save_content(self, url, score, title, text, is_paywall=False):
        with sqlite3.connect(DB_FILE) as conn:
            try:
                conn.execute(
                    '''INSERT INTO extracted_content
                       (url, score, title, text_content, is_paywall, char_count)
                       VALUES (?, ?, ?, ?, ?, ?)''',
                    (url, score, title, text, int(is_paywall), len(text or ''))
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def extract_text(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        title = ''
        tag = soup.find('title')
        if tag:
            title = tag.get_text(strip=True)
        for t in soup.find_all(['script', 'style', 'nav', 'footer',
                                 'header', 'aside', 'noscript', 'iframe',
                                 'form', 'button']):
            t.decompose()
        # Prefer article/main content
        main = (soup.find('article') or soup.find('main') or
                soup.find(id=re.compile(r'content|article|post|body', re.I)) or
                soup)
        text = re.sub(r'\s+', ' ', main.get_text(separator=' ')).strip()
        return title, text

    def is_login_wall(self, text):
        t = text.lower()
        return any(marker in t for marker in PAYWALL_MARKERS)

    def fetch_url(self, url, score):
        result = {'url': url, 'score': score, 'title': '', 'text': '',
                  'status': 'error', 'error': ''}
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)

            if resp.status_code in (401, 403):
                result['status'] = 'paywall'
                result['error']  = f'HTTP {resp.status_code}'
                return result

            resp.raise_for_status()

            # --- FIX: Strict binary file detection ---
            ct = resp.headers.get('content-type', '').lower()
            # If the content-type is missing or indicates binary data, skip it
            if not ct or any(bad in ct for bad in ['image/', 'application/pdf', 'application/zip', 'audio/', 'video/', 'application/octet-stream', 'binary']):
                result['status'] = 'skip'
                result['error']  = f'Binary/Non-text content-type: {ct}'
                return result

            if 'text/html' not in ct and 'application/xhtml' not in ct:
                result['status'] = 'skip'
                result['error']  = f'Non-HTML: {ct}'
                return result

            if len(resp.content) > 10 * 1024 * 1024:
                result['status'] = 'skip'
                result['error']  = 'Content too large'
                return result

            # Attempt to decode as UTF-8; if it fails, it's binary garbage
            try:
                decoded_text = resp.content.decode('utf-8')
            except UnicodeDecodeError:
                result['status'] = 'skip'
                result['error']  = 'Binary content (UTF-8 decode failed)'
                return result

            title, text = self.extract_text(decoded_text)

            if self.is_login_wall(text):
                result['status'] = 'paywall'
                result['title']  = title
                result['error']  = 'Login wall detected (200 + login page)'
                return result

            if not text or len(text) < 100:
                result['status'] = 'skip'
                result['error']  = f'Too little text ({len(text)} chars)'
                return result

            result['status'] = 'ok'
            result['title']  = title
            result['text']   = text
            return result

        except requests.exceptions.Timeout:
            result['error'] = 'Timeout'
        except requests.exceptions.ConnectionError as e:
            result['error'] = f'Connection error: {e}'
        except requests.exceptions.RequestException as e:
            result['error'] = str(e)
        except Exception as e:
            result['error'] = f'Unexpected: {e}'
        return result

    def _iter_links_file(self, filepath, source):
        if not os.path.exists(filepath):
            logger.debug(f"{source}: not found at {filepath}")
            return
        seen = set()
        matched = 0
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                for raw in f:
                    line = raw.strip()
                    if (not line or line.startswith('#')
                            or line.startswith('=')
                            or 'Domain=' in line):
                        continue
                    url = next(
                        (p.rstrip('.,;)') for p in line.split()
                         if p.startswith('http://') or p.startswith('https://')),
                        None
                    )
                    if not url:
                        continue
                    m = re.search(r'\[SCORE:\s*([\d.]+)\]', line)
                    score = float(m.group(1)) if m else GRPC_DEFAULT_SCORE
                    if url not in seen and not self.is_url_processed(url):
                        seen.add(url)
                        matched += 1
                        yield url, score
        except Exception as e:
            logger.error(f"{source}: read error — {e}")
        logger.info(f"{source}: {matched} new URLs to process")

    def run_once(self):
        # Collect all new URLs from both files, deduplicating
        seen_in_batch = set()
        all_urls = []
        for url, score in self._iter_links_file(LINKS_FILE, 'links.txt'):
            if url not in seen_in_batch:
                seen_in_batch.add(url)
                all_urls.append((url, score))
        for url, score in self._iter_links_file(GRPC_LINKS_FILE, 'grpc_links.txt'):
            if url not in seen_in_batch:
                seen_in_batch.add(url)
                all_urls.append((url, score))

        if not all_urls:
            logger.info("No new URLs to process.")
            return

        logger.info(f"Processing {len(all_urls)} URLs ({MAX_WORKERS} workers)…")
        ok = paywall = skipped = errors = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(self.fetch_url, url, score): (url, score)
                for url, score in all_urls
            }
            for future in as_completed(futures):
                res = future.result()
                url   = res['url']
                score = res['score']

                if res['status'] == 'ok':
                    if self.save_content(url, score, res['title'], res['text']):
                        ok += 1
                        logger.info(f"✅ {len(res['text'])} chars | score={score:.2f} | {url}")

                elif res['status'] == 'paywall':
                    # Permanent placeholder so we skip this URL in future runs
                    self.save_content(
                        url, score, res.get('title', ''),
                        f"[Paywall: {res['error']}]", is_paywall=True
                    )
                    paywall += 1
                    logger.info(f"🔒 Paywall | {url} ({res['error']})")

                elif res['status'] == 'skip':
                    # Don't save — will retry next poll cycle
                    skipped += 1
                    logger.debug(f"⏭️ Skip | {url} ({res['error']})")

                else:
                    # Transient error — don't save, retry next cycle
                    errors += 1
                    logger.warning(f"❌ Error | {url} ({res['error']})")

                time.sleep(0.2)

        logger.info(
            f"Done — ✅ {ok} extracted  🔒 {paywall} paywall  "
            f"⏭️ {skipped} skipped  ❌ {errors} errors  "
            f"(of {len(all_urls)} total)"
        )

    def run_forever(self):
        logger.info(
            f"Worker started — {LINKS_FILE} + {GRPC_LINKS_FILE} "
            f"every {POLL_INTERVAL}s, {MAX_WORKERS} workers"
        )
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Error in run loop: {e}", exc_info=True)
            logger.info(f"Sleeping {POLL_INTERVAL}s…")
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
        logger.info("Worker stopped.")
    finally:
        worker.cleanup()


if __name__ == '__main__':
    main()