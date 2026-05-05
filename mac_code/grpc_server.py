"""
gRPC Queue Server - Runs on Mac
Manages Redis queue and deduplication for distributed crawlers.
"""

import sys
import os
import time
import logging
from concurrent import futures

import grpc
import redis

# Add generated proto path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [QueueServer] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


class QueueServiceServicer(crawler_pb2_grpc.QueueServiceServicer):
    """gRPC service that wraps Redis for URL queue management."""

    QUEUE_KEY = "crawler:url_queue"
    VISITED_KEY = "crawler:visited"
    PROCESSING_KEY = "crawler:processing"

    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        # Verify connection
        self.redis_client.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to avoid duplicates."""
        url = url.strip().rstrip('/')
        # Remove fragments
        if '#' in url:
            url = url.split('#')[0]
        return url

    def GetNextURL(self, request, context):
        """Atomically get next URL from queue. Uses SPOP for O(1) random pick."""
        crawler_id = request.crawler_id
        response = crawler_pb2.GetNextURLResponse()

        # Use a Lua script for atomic dequeue + mark processing
        lua_script = """
        local url = redis.call('SPOP', KEYS[1])
        if url then
            redis.call('SADD', KEYS[2], url)
            return url
        end
        return nil
        """

        try:
            result = self.redis_client.eval(
                lua_script, 2, self.QUEUE_KEY, self.PROCESSING_KEY
            )

            if result:
                response.url = result
                response.queue_empty = False
                logger.info(f"[{crawler_id}] Dequeued: {result}")
            else:
                response.url = ""
                response.queue_empty = True
                logger.debug(f"[{crawler_id}] Queue empty")
        except redis.RedisError as e:
            logger.error(f"Redis error in GetNextURL: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))

        return response

    def AddURLs(self, request, context):
        """Add new URLs to queue if not already visited."""
        crawler_id = request.crawler_id
        added = 0

        # Lua script: only add if not in visited set
        lua_script = """
        local added = 0
        for i, url in ipairs(ARGV) do
            if redis.call('SISMEMBER', KEYS[1], url) == 0 then
                if redis.call('SISMEMBER', KEYS[2], url) == 0 then
                    redis.call('SADD', KEYS[2], url)
                    added = added + 1
                end
            end
        end
        return added
        """

        try:
            normalized_urls = [self._normalize_url(u) for u in request.urls if u.strip()]
            # Filter out empty strings
            normalized_urls = [u for u in normalized_urls if u]

            if normalized_urls:
                added = self.redis_client.eval(
                    lua_script, 2, self.VISITED_KEY, self.QUEUE_KEY,
                    *normalized_urls
                )
                logger.info(
                    f"[{crawler_id}] Added {added}/{len(normalized_urls)} URLs to queue"
                )
        except redis.RedisError as e:
            logger.error(f"Redis error in AddURLs: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))

        return crawler_pb2.AddURLsResponse(added_count=added)

    def MarkVisited(self, request, context):
        """Mark a URL as visited and remove from processing."""
        url = self._normalize_url(request.url)

        try:
            pipe = self.redis_client.pipeline()
            pipe.sadd(self.VISITED_KEY, url)
            pipe.srem(self.PROCESSING_KEY, url)
            pipe.srem(self.QUEUE_KEY, url)  # safety: remove from queue too
            pipe.execute()

            logger.debug(f"[{request.crawler_id}] Marked visited: {url}")
            return crawler_pb2.MarkVisitedResponse(success=True)
        except redis.RedisError as e:
            logger.error(f"Redis error in MarkVisited: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return crawler_pb2.MarkVisitedResponse(success=False)

    def IsVisited(self, request, context):
        """Check if URL has been visited."""
        url = self._normalize_url(request.url)

        try:
            visited = self.redis_client.sismember(self.VISITED_KEY, url)
            return crawler_pb2.IsVisitedResponse(visited=bool(visited))
        except redis.RedisError as e:
            logger.error(f"Redis error in IsVisited: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return crawler_pb2.IsVisitedResponse(visited=False)

    def SeedURLs(self, request, context):
        """Seed initial URLs into the queue."""
        seeded = 0
        try:
            for url in request.urls:
                normalized = self._normalize_url(url)
                if normalized:
                    # Only add if not visited
                    if not self.redis_client.sismember(self.VISITED_KEY, normalized):
                        self.redis_client.sadd(self.QUEUE_KEY, normalized)
                        seeded += 1

            logger.info(f"Seeded {seeded} URLs")
        except redis.RedisError as e:
            logger.error(f"Redis error in SeedURLs: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))

        return crawler_pb2.SeedURLsResponse(seeded_count=seeded)

    def GetStats(self, request, context):
        """Return queue and visited counts."""
        try:
            queue_size = self.redis_client.scard(self.QUEUE_KEY)
            visited_count = self.redis_client.scard(self.VISITED_KEY)
            return crawler_pb2.GetStatsResponse(
                queue_size=queue_size,
                visited_count=visited_count
            )
        except redis.RedisError as e:
            logger.error(f"Redis error in GetStats: {e}")
            return crawler_pb2.GetStatsResponse(queue_size=0, visited_count=0)


def serve():
    redis_host = os.environ.get('REDIS_HOST', 'redis')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    grpc_port = os.environ.get('GRPC_PORT', '50051')

    # Wait for Redis to be ready
    for attempt in range(30):
        try:
            r = redis.Redis(host=redis_host, port=redis_port)
            r.ping()
            logger.info("Redis is ready!")
            break
        except (redis.ConnectionError, redis.TimeoutError):
            logger.info(f"Waiting for Redis... (attempt {attempt + 1})")
            time.sleep(2)
    else:
        logger.error("Could not connect to Redis after 30 attempts")
        sys.exit(1)

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

    servicer = QueueServiceServicer(redis_host, redis_port)
    crawler_pb2_grpc.add_QueueServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f'0.0.0.0:{grpc_port}')
    server.start()
    logger.info(f"gRPC Queue Server started on port {grpc_port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.stop(grace=5)


if __name__ == '__main__':
    serve()