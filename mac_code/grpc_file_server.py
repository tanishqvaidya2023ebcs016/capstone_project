"""
gRPC File Server — writes crawled links to output/links.txt
Runs on localhost:50052
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
 
    def __init__(self, output_file):
        self.output_file = output_file
        self.lock = threading.Lock()
        self.link_count = 0
 
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
 
        if not os.path.exists(output_file):
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"# Distributed Crawler — System Design Links\n")
                f.write(f"# Started: {datetime.now().isoformat()}\n")
                f.write(f"# Format: [timestamp] [crawler_id] url\n")
                f.write(f"{'=' * 80}\n\n")
 
        logger.info(f"Output file: {output_file}")
 
    def StoreLink(self, request, context):
        # Skip probe requests
        if request.url == "__rtt_probe__" or request.url == "__probe__":
            return crawler_pb2.StoreLinkResponse(success=True)
 
        try:
            timestamp = datetime.fromtimestamp(request.timestamp).isoformat() \
                if request.timestamp else datetime.now().isoformat()
 
            line = f"[{timestamp}] [{request.crawler_id}] {request.url}\n"
 
            with self.lock:
                with open(self.output_file, 'a', encoding='utf-8') as f:
                    f.write(line)
                self.link_count += 1
 
            logger.info(f"Stored #{self.link_count} from {request.crawler_id}: {request.url}")
            return crawler_pb2.StoreLinkResponse(success=True)
 
        except Exception as e:
            logger.error(f"Error: {e}")
            return crawler_pb2.StoreLinkResponse(success=False)
 
    def StoreLinks(self, request, context):
        stored = 0
        try:
            with self.lock:
                with open(self.output_file, 'a', encoding='utf-8') as f:
                    for link in request.links:
                        if link.url in ("__rtt_probe__", "__probe__"):
                            continue
                        ts = datetime.fromtimestamp(link.timestamp).isoformat() \
                            if link.timestamp else datetime.now().isoformat()
                        f.write(f"[{ts}] [{link.crawler_id}] {link.url}\n")
                        stored += 1
                        self.link_count += 1
            return crawler_pb2.StoreLinksResponse(stored_count=stored)
        except Exception as e:
            logger.error(f"Error: {e}")
            return crawler_pb2.StoreLinksResponse(stored_count=stored)
 
 
def serve():
    output_file = os.environ.get('OUTPUT_FILE', './output/links.txt')
    grpc_port = os.environ.get('GRPC_PORT', '50052')
 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = FileServiceServicer(output_file)
    crawler_pb2_grpc.add_FileServiceServicer_to_server(servicer, server)
 
    server.add_insecure_port(f'0.0.0.0:{grpc_port}')
    server.start()
    logger.info(f"File Server started on port {grpc_port}")
 
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5)
 
 
if __name__ == '__main__':
    serve()