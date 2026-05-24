"""
gRPC File Server - Runs on Windows
Receives crawled links from both crawlers and writes to links.txt
"""

import sys
import os
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


class FileServiceServicer(crawler_pb2_grpc.FileServiceServicer):
    """gRPC service that writes crawled links to a text file."""

    def __init__(self, output_file: str):
        self.output_file = output_file
        self.lock = threading.Lock()
        self.link_count = 0

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

        # Write header if file doesn't exist
        if not os.path.exists(output_file):
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"# Distributed Crawler - System Design Links\n")
                f.write(f"# Started: {datetime.now().isoformat()}\n")
                f.write(f"# Format: [timestamp] [crawler_id] url\n")
                f.write(f"{'='*80}\n\n")

        logger.info(f"File server initialized. Output: {output_file}")

    def StoreLink(self, request, context):
        """Store a single link to the file."""
        try:
            timestamp = datetime.fromtimestamp(request.timestamp).isoformat() \
                if request.timestamp else datetime.now().isoformat()

            line = f"[{timestamp}] [{request.crawler_id}] {request.url}\n"

            with self.lock:
                with open(self.output_file, 'a', encoding='utf-8') as f:
                    f.write(line)
                self.link_count += 1

            logger.info(
                f"Stored link #{self.link_count} from {request.crawler_id}: "
                f"{request.url}"
            )
            return crawler_pb2.StoreLinkResponse(success=True)

        except Exception as e:
            logger.error(f"Error storing link: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return crawler_pb2.StoreLinkResponse(success=False)

    def StoreLinks(self, request, context):
        """Store multiple links to the file."""
        stored = 0
        try:
            with self.lock:
                with open(self.output_file, 'a', encoding='utf-8') as f:
                    for link_req in request.links:
                        timestamp = datetime.fromtimestamp(
                            link_req.timestamp
                        ).isoformat() if link_req.timestamp else datetime.now().isoformat()

                        line = f"[{timestamp}] [{link_req.crawler_id}] {link_req.url}\n"
                        f.write(line)
                        stored += 1
                        self.link_count += 1

            logger.info(f"Stored {stored} links (total: {self.link_count})")
            return crawler_pb2.StoreLinksResponse(stored_count=stored)

        except Exception as e:
            logger.error(f"Error storing links: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return crawler_pb2.StoreLinksResponse(stored_count=stored)


def serve():
    output_file = os.environ.get('OUTPUT_FILE', './output/links.txt')
    grpc_port = os.environ.get('GRPC_PORT', '50052')

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024),
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
        ]
    )

    servicer = FileServiceServicer(output_file)
    crawler_pb2_grpc.add_FileServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f'0.0.0.0:{grpc_port}')
    server.start()
    logger.info(f"gRPC File Server started on port {grpc_port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.stop(grace=5)


if __name__ == '__main__':
    serve()