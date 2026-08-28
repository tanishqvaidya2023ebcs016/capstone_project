"""
gRPC File Server — centralised ranked-link writer for ALL crawlers.

Runs on localhost:50052 (or FILE_SERVER host/port via env).
"""

import sys
import os
import re
import time
import logging
import threading
from concurrent import futures
from datetime import datetime

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [FileServer] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Centralised ranked writer
# ---------------------------------------------------------------------------

class CentralizedRankedWriter:
    """Thread-safe writer that maintains a single sorted links.txt from all
    crawlers.  Format matches what text_extractor_worker.parse_ranked_links_file
    expects:
        [rank] [SCORE: X.XX] [crawler_id] url
    """

    def __init__(self, output_file: str):
        self.output_file = output_file
        self.lock = threading.Lock()
        # url -> {'score': float, 'crawler_id': str, 'domain': str, 'ts': str}
        self.links: dict = {}
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        self._load_existing()

    def _load_existing(self):
        """Reload from disk on server start so data survives restarts."""
        if not os.path.exists(self.output_file):
            return
        loaded = 0
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue
                    score_m = re.search(r'\[SCORE:\s*([\d.]+)\]', line)
                    crawler_m = re.search(r'\[SCORE:[^\]]+\]\s+\[([^\]]+)\]', line)
                    url = next(
                        (p for p in line.split() if p.startswith('http')), None
                    )
                    if score_m and url:
                        self.links[url] = {
                            'score': float(score_m.group(1)),
                            'crawler_id': crawler_m.group(1).strip() if crawler_m else 'unknown',
                            'domain': '',
                            'ts': datetime.now().isoformat(),
                        }
                        loaded += 1
        except Exception as e:
            logger.error(f"Error loading existing ranked file: {e}")
        if loaded:
            logger.info(f"Reloaded {loaded} existing ranked links from {self.output_file}")

    def add(self, url: str, score: float, crawler_id: str, domain: str, ts: str):
        with self.lock:
            existing = self.links.get(url)
            if existing and existing['score'] >= score:
                return          # keep higher score
            self.links[url] = {
                'score': score,
                'crawler_id': crawler_id,
                'domain': domain or '',
                'ts': ts,
            }
            self._write_locked()

    def _write_locked(self):
        """Must be called with self.lock held."""
        sorted_links = sorted(
            self.links.items(), key=lambda kv: kv[1]['score'], reverse=True
        )
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write('# RANKED SYSTEM DESIGN LINKS — all crawlers\n')
                f.write(f'# Last Updated: {datetime.now().isoformat()}\n')
                f.write(f'# Total Links: {len(sorted_links)}\n')
                f.write('=' * 100 + '\n\n')
                for rank, (url, meta) in enumerate(sorted_links, 1):
                    f.write(
                        f"[{rank:>4d}] "
                        f"[SCORE: {meta['score']:>7.2f}] "
                        f"[{meta['crawler_id']:>14s}] "
                        f"{url}\n"
                    )
                    if meta['domain']:
                        f.write(f"       Domain={meta['domain']}\n")
                    f.write('\n')
                f.write('=' * 100 + '\n')
                scores = [m['score'] for m in self.links.values()]
                f.write(f'# Highest: {max(scores):.2f}  '
                        f'Lowest: {min(scores):.2f}  '
                        f'Average: {sum(scores)/len(scores):.2f}\n')
                # Per-crawler breakdown
                breakdown: dict = {}
                for m in self.links.values():
                    breakdown[m['crawler_id']] = breakdown.get(m['crawler_id'], 0) + 1
                for cid, cnt in sorted(breakdown.items(), key=lambda x: -x[1]):
                    f.write(f'#   {cid}: {cnt} links\n')
        except Exception as e:
            logger.error(f"Error writing ranked file: {e}")

    @property
    def total(self):
        with self.lock:
            return len(self.links)


# ---------------------------------------------------------------------------
# gRPC servicer
# ---------------------------------------------------------------------------

class FileServiceServicer(crawler_pb2_grpc.FileServiceServicer):

    def __init__(self, output_file: str, raw_log_file: str):
        self.ranked_writer = CentralizedRankedWriter(output_file)
        self.raw_log_file = raw_log_file
        self.log_lock = threading.Lock()
        self.link_count = 0

        # Initialise raw log
        os.makedirs(os.path.dirname(raw_log_file) or '.', exist_ok=True)
        if not os.path.exists(raw_log_file):
            with open(raw_log_file, 'w', encoding='utf-8') as f:
                f.write('# Distributed Crawler — raw gRPC link log\n')
                f.write(f'# Started: {datetime.now().isoformat()}\n')
                f.write('# Format: [timestamp] [crawler_id] [score] url\n')
                f.write('=' * 80 + '\n\n')

        logger.info(f"Ranked output : {output_file}")
        logger.info(f"Raw gRPC log  : {raw_log_file}")

    def _write_raw_log(self, url: str, crawler_id: str, score: float, ts_str: str):
        """Append one line to the chronological raw log (kept for debugging)."""
        try:
            line = f"[{ts_str}] [{crawler_id}] [SCORE: {score:.2f}] {url}\n"
            with self.log_lock:
                with open(self.raw_log_file, 'a', encoding='utf-8') as f:
                    f.write(line)
                self.link_count += 1
        except Exception as e:
            logger.error(f"Raw log write error: {e}")

    # ------------------------------------------------------------------
    def StoreLink(self, request, context):
        if request.url in ('__rtt_probe__', '__probe__'):
            return crawler_pb2.StoreLinkResponse(success=True)

        ts_str = (
            datetime.fromtimestamp(request.timestamp).isoformat()
            if request.timestamp
            else datetime.now().isoformat()
        )

        # FIX: use score from proto (was always 0 / missing before)
        score = float(request.score) if request.score else 0.0
        domain = request.domain or ''

        try:
            self.ranked_writer.add(
                url=request.url,
                score=score,
                crawler_id=request.crawler_id,
                domain=domain,
                ts=ts_str,
            )
            self._write_raw_log(request.url, request.crawler_id, score, ts_str)

            logger.info(
                f"Stored #{self.link_count} score={score:.2f} "
                f"from [{request.crawler_id}]: {request.url}"
            )
            return crawler_pb2.StoreLinkResponse(success=True)

        except Exception as e:
            logger.error(f"StoreLink error: {e}")
            return crawler_pb2.StoreLinkResponse(success=False)

    # ------------------------------------------------------------------
    def StoreLinks(self, request, context):
        stored = 0
        try:
            for link in request.links:
                if link.url in ('__rtt_probe__', '__probe__'):
                    continue
                ts_str = (
                    datetime.fromtimestamp(link.timestamp).isoformat()
                    if link.timestamp
                    else datetime.now().isoformat()
                )
                score = float(link.score) if link.score else 0.0
                self.ranked_writer.add(
                    url=link.url,
                    score=score,
                    crawler_id=link.crawler_id,
                    domain=link.domain or '',
                    ts=ts_str,
                )
                self._write_raw_log(link.url, link.crawler_id, score, ts_str)
                stored += 1

            return crawler_pb2.StoreLinksResponse(stored_count=stored)
        except Exception as e:
            logger.error(f"StoreLinks error: {e}")
            return crawler_pb2.StoreLinksResponse(stored_count=stored)


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------

def serve():
    output_file  = os.environ.get('OUTPUT_FILE',  './output/links.txt')
    raw_log_file = os.environ.get('RAW_LOG_FILE', './output/grpc_links.txt')
    grpc_port    = os.environ.get('GRPC_PORT',    '50052')

    logger.info("=" * 64)
    logger.info("🔖 grpc_file_server.py CODE_VERSION=centralized-storage-v2")
    logger.info("   (single ranked links.txt, fed by ALL crawlers)")
    logger.info("=" * 64)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = FileServiceServicer(output_file, raw_log_file)
    crawler_pb2_grpc.add_FileServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f'0.0.0.0:{grpc_port}')
    server.start()
    logger.info(f"File server started on port {grpc_port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5)


if __name__ == '__main__':
    serve()