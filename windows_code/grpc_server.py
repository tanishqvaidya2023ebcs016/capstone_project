"""
gRPC Queue Server — Redis Sentinel edition
Manages the URL queue and deduplication for all crawlers.

HA CHANGE: previously connected to a single Redis host directly.  If Mac
went down, the Redis connection died and queue state was lost.  Now uses
redis.sentinel.Sentinel which:
  1. Discovers the current Redis master via the Sentinel cluster.
  2. Automatically reconnects to the promoted replica after a failover.
  3. Retries transient errors (connection resets during failover).

Normal operation: Mac's Redis is master → same performance as before.
Mac goes down:    Oracle's Redis promoted → Oracle's grpc_server.py
                  (running as standby) seamlessly takes over.
Mac comes back:   Mac's Redis joins as replica, syncs, sentinel manages.
"""

import sys
import os
import time
import logging
from concurrent import futures

import grpc
import redis
from redis.sentinel import Sentinel as RedisSentinel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [QueueServer] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def _parse_sentinel_hosts(raw: str) -> list:
    """Parse 'host1:port1,host2:port2' into [(host, port), ...]."""
    hosts = []
    for h in raw.split(','):
        h = h.strip()
        if not h:
            continue
        if ':' in h:
            host, port = h.rsplit(':', 1)
            hosts.append((host.strip(), int(port)))
        else:
            hosts.append((h, 26379))
    return hosts


def connect_redis(redis_host: str, redis_port: int,
                  sentinel_hosts_raw: str, master_name: str):
    """
    Connect to Redis via Sentinel if sentinel hosts are configured,
    otherwise fall back to a direct connection.

    The Sentinel client auto-reconnects to the new master after failover,
    so the rest of the code (QueueServiceServicer) needs no changes.
    """
    sentinel_hosts = _parse_sentinel_hosts(sentinel_hosts_raw)

    if sentinel_hosts:
        logger.info(f"Connecting via Redis Sentinel: {sentinel_hosts} (master={master_name})")
        sentinel = RedisSentinel(
            sentinel_hosts,
            socket_timeout=1.0,
            socket_connect_timeout=2.0,
        )
        # master_for() returns a client that auto-discovers the current master
        # and retries on the new master after failover.
        client = sentinel.master_for(
            master_name,
            socket_timeout=1.0,
            decode_responses=True,
            retry_on_timeout=True,
        )
        # Verify at least one sentinel is reachable
        client.ping()
        logger.info("Connected to Redis master via Sentinel ✓")
        return client
    else:
        logger.warning("No SENTINEL_HOSTS set — falling back to direct Redis connection (no HA)")
        client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        client.ping()
        logger.info(f"Connected directly to Redis at {redis_host}:{redis_port}")
        return client


class QueueServiceServicer(crawler_pb2_grpc.QueueServiceServicer):
    QUEUE_KEY      = "crawler:url_queue"
    VISITED_KEY    = "crawler:visited"
    PROCESSING_KEY = "crawler:processing"

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def _normalize_url(self, url: str) -> str:
        url = url.strip().rstrip('/')
        if '#' in url:
            url = url.split('#')[0]
        return url

    def GetNextURL(self, request, context):
        crawler_id = request.crawler_id
        response   = crawler_pb2.GetNextURLResponse()
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
                response.url         = result
                response.queue_empty = False
                logger.info(f"[{crawler_id}] Dequeued: {result}")
            else:
                response.url         = ""
                response.queue_empty = True
                logger.debug(f"[{crawler_id}] Queue empty")
        except redis.RedisError as e:
            logger.error(f"Redis error in GetNextURL: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
        return response

    def AddURLs(self, request, context):
        crawler_id = request.crawler_id
        added      = 0
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
            normalized = [self._normalize_url(u) for u in request.urls if u.strip()]
            normalized = [u for u in normalized if u]
            if normalized:
                added = self.redis_client.eval(
                    lua_script, 2, self.VISITED_KEY, self.QUEUE_KEY, *normalized
                )
                logger.info(f"[{crawler_id}] Added {added}/{len(normalized)} URLs")
        except redis.RedisError as e:
            logger.error(f"Redis error in AddURLs: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
        return crawler_pb2.AddURLsResponse(added_count=added)

    def MarkVisited(self, request, context):
        url = self._normalize_url(request.url)
        try:
            pipe = self.redis_client.pipeline()
            pipe.sadd(self.VISITED_KEY,    url)
            pipe.srem(self.PROCESSING_KEY, url)
            pipe.srem(self.QUEUE_KEY,      url)
            pipe.execute()
            logger.debug(f"[{request.crawler_id}] Marked visited: {url}")
            return crawler_pb2.MarkVisitedResponse(success=True)
        except redis.RedisError as e:
            logger.error(f"Redis error in MarkVisited: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return crawler_pb2.MarkVisitedResponse(success=False)

    def IsVisited(self, request, context):
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
        seeded = 0
        try:
            for url in request.urls:
                normalized = self._normalize_url(url)
                if normalized and not self.redis_client.sismember(self.VISITED_KEY, normalized):
                    self.redis_client.sadd(self.QUEUE_KEY, normalized)
                    seeded += 1
            logger.info(f"Seeded {seeded} URLs")
        except redis.RedisError as e:
            logger.error(f"Redis error in SeedURLs: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
        return crawler_pb2.SeedURLsResponse(seeded_count=seeded)

    def GetStats(self, request, context):
        try:
            queue_size    = self.redis_client.scard(self.QUEUE_KEY)
            visited_count = self.redis_client.scard(self.VISITED_KEY)
            return crawler_pb2.GetStatsResponse(
                queue_size=queue_size, visited_count=visited_count
            )
        except redis.RedisError as e:
            logger.error(f"Redis error in GetStats: {e}")
            return crawler_pb2.GetStatsResponse(queue_size=0, visited_count=0)


def serve():
    redis_host          = os.environ.get('REDIS_HOST',          'redis')
    redis_port          = int(os.environ.get('REDIS_PORT',      6379))
    grpc_port           = os.environ.get('GRPC_PORT',           '50051')
    sentinel_hosts_raw  = os.environ.get('SENTINEL_HOSTS',      '')
    master_name         = os.environ.get('SENTINEL_MASTER_NAME','mymaster')

    logger.info("=" * 60)
    logger.info("gRPC Queue Server — Redis Sentinel HA edition")
    logger.info(f"  Sentinel hosts : {sentinel_hosts_raw or '(none — direct mode)'}")
    logger.info(f"  Master name    : {master_name}")
    logger.info("=" * 60)

    # Wait for Redis / Sentinel to be ready
    for attempt in range(30):
        try:
            redis_client = connect_redis(
                redis_host, redis_port, sentinel_hosts_raw, master_name
            )
            break
        except Exception as e:
            logger.info(f"Waiting for Redis... ({attempt + 1}/30): {e}")
            time.sleep(2)
    else:
        logger.error("Could not connect to Redis after 30 attempts")
        sys.exit(1)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length',    50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024),
            ('grpc.keepalive_time_ms',          10000),
            ('grpc.keepalive_timeout_ms',        5000),
            ('grpc.keepalive_permit_without_calls', True),
        ]
    )

    servicer = QueueServiceServicer(redis_client)
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