"""
Ranked Crawler — Enhanced Algorithm (Complete Version)
========================================================
Includes all URL filtering, file writer, and seed functions.
"""

import sys
import os
import time
import logging
import re
import math
import threading
import statistics
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, parse_qs
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional better date parsing
try:
    import dateparser
    HAS_DATEPARSER = True
except ImportError:
    HAS_DATEPARSER = False
    dateparser = None

# Add generated protobuf path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

# ============================================================
# SCORING WEIGHTS & PARAMETERS
# ============================================================
WEIGHT_SOURCE = 3.0
WEIGHT_RELEVANCE = 2.5
WEIGHT_FRESHNESS = 2.0
RELEVANCE_ALPHA = 0.02
FRESHNESS_HALF_LIFE_DAYS = 730
LAMBDA = math.log(2) / FRESHNESS_HALF_LIFE_DAYS
MIN_CONTENT_LENGTH = 500
OPTIMAL_CONTENT_LENGTH = 5000
CONTENT_LENGTH_BONUS_MAX = 0.5
CONTENT_LENGTH_PENALTY_MAX = -0.5

# Minimum keyword hits a page must have to be stored (even tier1 pages)
MIN_KEYWORD_COUNT = 3

# ============================================================
# SOURCE AUTHORITY
# ============================================================
SOURCE_AUTHORITY = {
    'bytebytego.com': 1.0, 'blog.bytebytego.com': 1.0,
    'hellointerview.com': 1.0, 'www.hellointerview.com': 1.0,
    'systemdesign.one': 1.0, 'newsletter.systemdesign.one': 1.0,
    'designgurus.io': 0.95, 'www.designgurus.io': 0.95,
    'martinfowler.com': 0.95, 'highscalability.com': 0.95,
    'donnemartin.com': 0.95, 'architecturenotes.co': 0.95,
    'blog.pragmaticengineer.com': 0.95, 'newsletter.pragmaticengineer.com': 0.95,
    'interviewready.io': 0.90, 'systemdesignschool.io': 0.90,
    'netflixtechblog.com': 0.85, 'engineering.fb.com': 0.85,
    'eng.uber.com': 0.85, 'engineering.linkedin.com': 0.85,
    'instagram-engineering.com': 0.85, 'slack.engineering': 0.85,
    'engineering.shopify.com': 0.85, 'dropbox.tech': 0.85,
    'airbnb.io': 0.85, 'stripe.com': 0.80,
    'aws.amazon.com': 0.80, 'cloud.google.com': 0.80,
    'learn.microsoft.com': 0.80, 'docs.microsoft.com': 0.80,
    'research.google': 0.80, 'github.com': 0.75,
    'medium.com': 0.60, 'dev.to': 0.60, 'educative.io': 0.70,
    'geeksforgeeks.org': 0.55, 'leetcode.com': 0.55,
    'stackoverflow.com': 0.55, 'baeldung.com': 0.55,
    'freecodecamp.org': 0.55, 'wikipedia.org': 0.50,
    'en.wikipedia.org': 0.50,
}

GITHUB_REPO_AUTHORITY = {
    'donnemartin/system-design-primer': 1.0,
    'karanpratapsingh/system-design': 0.95,
    'ByteByteGoHq/system-design-101': 0.95,
    'ashishps1/awesome-system-design-resources': 0.90,
    'systemdesign42/system-design': 0.90,
    'checkcheckzz/system-design-interview': 0.85,
    'binhnguyennus/awesome-scalability': 0.85,
    'madd86/awesome-system-design': 0.85,
    'shashank88/system_design': 0.80,
    'kilimchoi/engineering-blogs': 0.80,
    'codersguild/System-Design': 0.80,
    'prasadgujar/low-level-design-primer': 0.75,
    'alex/what-happens-when': 0.75,
    'lei-hsia/grokking-system-design': 0.75,
    'InterviewReady/system-design-resources': 0.80,
}

# ============================================================
# WEIGHTED KEYWORDS
# ============================================================
KEYWORD_WEIGHTS = {
    'system design': 3, 'distributed system': 3, 'distributed systems': 3,
    'microservices': 3, 'microservice architecture': 3, 'software architecture': 3,
    'scalability': 4, 'horizontal scaling': 4, 'vertical scaling': 4,
    'auto scaling': 4, 'elastic scaling': 4, 'load balancer': 4,
    'load balancing': 4, 'round robin': 3, 'least connections': 3,
    'reverse proxy': 3, 'nginx': 3, 'haproxy': 3, 'database design': 5,
    'database sharding': 5, 'sharding': 5, 'database replication': 5,
    'replication': 4, 'sql vs nosql': 4, 'relational database': 3,
    'nosql': 4, 'mongodb': 4, 'cassandra': 5, 'dynamodb': 4,
    'database indexing': 4, 'b-tree': 4, 'lsm tree': 5,
    'data partitioning': 5, 'partitioning strategy': 5,
    'master slave': 4, 'leader follower': 4, 'data modeling': 4,
    'schema design': 4, 'cap theorem': 5, 'consistency': 4,
    'availability': 4, 'partition tolerance': 5, 'eventual consistency': 5,
    'strong consistency': 5, 'linearizability': 5, 'acid': 4,
    'base theorem': 5, 'caching': 5, 'cache invalidation': 5,
    'cache aside': 5, 'write through': 4, 'write back': 4,
    'read through': 4, 'cache eviction': 4, 'redis': 4, 'memcached': 4,
    'cdn': 3, 'content delivery network': 3, 'message queue': 5,
    'message broker': 5, 'kafka': 5, 'rabbitmq': 4, 'sqs': 4,
    'pub sub': 4, 'publish subscribe': 4, 'event driven': 5,
    'event sourcing': 5, 'cqrs': 5, 'saga pattern': 5, 'api design': 4,
    'api gateway': 4, 'rest api': 3, 'restful': 3, 'graphql': 4,
    'grpc': 4, 'websocket': 4, 'long polling': 4, 'server sent events': 4,
    'rate limiting': 5, 'rate limiter': 5, 'throttling': 4,
    'circuit breaker': 5, 'dns': 2, 'domain name system': 2,
    'url shortener': 5, 'tiny url': 5, 'chat system': 5,
    'chat application': 5, 'notification system': 5, 'push notification': 5,
    'news feed': 5, 'newsfeed': 5, 'timeline': 5, 'search engine': 5,
    'web crawler': 5, 'video streaming': 5, 'live streaming': 5,
    'file storage': 5, 'object storage': 5, 'blob storage': 5,
    'payment system': 5, 'payment gateway': 5, 'ride sharing': 5,
    'ride hailing': 5, 'hotel booking': 5, 'reservation system': 5,
    'e-commerce': 4, 'shopping cart': 4, 'social network': 4,
    'social media': 4, 'twitter design': 5, 'instagram design': 5,
    'whatsapp design': 5, 'facebook design': 5, 'youtube design': 5,
    'netflix design': 5, 'uber design': 5, 'dropbox design': 5,
    'google maps': 5, 'proximity service': 5, 'typeahead': 5,
    'autocomplete': 5, 'key value store': 5, 'unique id generator': 5,
    'snowflake': 5, 'consistent hashing': 5, 'bloom filter': 5,
    'leader election': 5, 'consensus algorithm': 5, 'raft consensus': 5,
    'paxos': 5, 'gossip protocol': 5, 'heartbeat': 4, 'quorum': 5,
    'vector clock': 5, 'merkle tree': 5, 'geohashing': 5,
    'high availability': 4, 'fault tolerance': 4, 'fault tolerant': 4,
    'disaster recovery': 4, 'failover': 4, 'redundancy': 4,
    'single point of failure': 5, 'spof': 5, 'sla': 3, 'slo': 4,
    'sli': 4, 'latency': 4, 'throughput': 4, 'bandwidth': 3,
    'back of the envelope': 5, 'capacity estimation': 5,
    'bottleneck': 4, 'performance optimization': 4, 'p99 latency': 5,
    'tail latency': 5, 'kubernetes': 3, 'docker': 3, 'containerization': 3,
    'service mesh': 4, 'istio': 4, 'envoy': 4, 'ci cd': 2,
    'deployment strategy': 3, 'blue green deployment': 4,
    'canary deployment': 4, 'rolling deployment': 3, 'observability': 3,
    'monitoring': 3, 'logging': 3, 'distributed tracing': 4,
    'prometheus': 3, 'grafana': 3, 'trade-off': 3, 'tradeoff': 3,
    'functional requirements': 3, 'non-functional requirements': 4,
    'back of the envelope estimation': 5, 'capacity planning': 4,
    'design principles': 3, 'single responsibility': 2,
    'separation of concerns': 2,
}

# ============================================================
# URL FILTERING CONSTANTS
# ============================================================
TIER1_DOMAINS = set(k for k, v in SOURCE_AUTHORITY.items() if v >= 0.90)
TIER2_DOMAINS = set(k for k, v in SOURCE_AUTHORITY.items() if 0.60 <= v < 0.90)
TIER3_DOMAINS = set(k for k, v in SOURCE_AUTHORITY.items() if v < 0.60)
ALLOWED_DOMAINS = set(SOURCE_AUTHORITY.keys())
GITHUB_QUALITY_REPOS = list(GITHUB_REPO_AUTHORITY.keys())

BLOCKED_PATTERNS = [
    r'/login', r'/signin', r'/signup', r'/register', r'/auth/', r'/oauth',
    r'/sso/', r'/password', r'/forgot', r'/reset-password', r'/account',
    r'/settings', r'/profile', r'/preferences', r'/notifications',
    r'/pulls$', r'/issues$', r'/issues\?', r'/commit/', r'/commits/',
    r'/compare/', r'/blame/', r'/raw/', r'/edit/', r'/delete/', r'/fork',
    r'/forks$', r'/stargazers', r'/watchers', r'/network', r'/graphs/',
    r'/pulse', r'/projects', r'/actions', r'/security', r'/packages',
    r'/releases/tag/', r'/archive/', r'/workflows/', r'\.git$', r'/sponsors',
    r'/marketplace', r'/codespaces', r'/copilot', r'/share\?',
    r'intent/tweet', r'facebook\.com/sharer', r'linkedin\.com/share',
    r'/pricing', r'/plans', r'/subscribe', r'/cart', r'/checkout', r'/buy',
    r'/premium', r'/gift', r'/gift-credits', r'/mock/gift',
    r'/terms', r'/privacy', r'/cookie', r'/legal', r'/contact', r'/support',
    r'/about', r'/careers', r'/jobs', r'/our-coaches', r'/coaches',
    r'/become-an-expert', r'/become-a-coach', r'/team', r'/press',
    r'/faq', r'/help', r'/docs$', r'/changelog',
    r'/blog/tag/', r'/blog/page/', r'/category/', r'/tags/',
    r'/community/', r'/questions\?', r'/questions$', r'/forum',
    r'/discuss', r'/answers',
    r'/mock/', r'/schedule',
    r'/practice$', r'/practice/overview$',
    r'/behavioral$', r'/code$',
    r'\.(png|jpg|jpeg|gif|svg|ico|webp|mp4|mp3|pdf|zip|tar|gz)$',
    r'/api/', r'/rss', r'/feed', r'/sitemap', r'\.json$', r'\.xml$',
    r'utm_', r'ref=', r'/ads/', r'/sponsor',
    r'/page/\d+', r'\?page=', r'\?p=\d+',
]
BLOCKED_COMPILED = [re.compile(p, re.IGNORECASE) for p in BLOCKED_PATTERNS]

DOMAIN_PATH_ALLOWLIST = {
    'hellointerview.com': ['/learn/', '/blog/'],
    'www.hellointerview.com': ['/learn/', '/blog/'],
    'bytebytego.com': ['/courses/', '/guides/', '/blog/'],
    'blog.bytebytego.com': ['/'],
    'designgurus.io': ['/course/', '/blog/', '/learn/'],
    'educative.io': ['/courses/', '/blog/', '/answers/'],
}

SD_URL_KEYWORDS = [
    'system-design', 'system_design', 'systemdesign', 'distributed-system',
    'distributed_system', 'microservices', 'architecture', 'software-architecture',
    'scalability', 'high-availability', 'fault-tolerance', 'load-balancer',
    'load-balancing', 'database-design', 'database-sharding', 'sharding',
    'replication', 'cap-theorem', 'caching', 'cache-invalidation',
    'message-queue', 'kafka', 'event-driven', 'api-design', 'api-gateway',
    'rate-limiting', 'consistent-hashing', 'bloom-filter', 'circuit-breaker',
    'leader-election', 'system-design-interview', 'design-interview',
    'grokking', 'bytebytego', 'donnemartin', 'hellointerview', 'designgurus',
    'highscalability', 'system-design-primer', 'low-level-design',
    'ml-system-design', 'vector-database', 'deep-dive',
    'problem-breakdown', 'core-concept', 'key-technolog',
]

# ============================================================
# URL HELPER FUNCTIONS
# ============================================================
def get_domain(url):
    try:
        return urlparse(url).netloc.lower()
    except:
        return ""

def get_domain_stripped(url):
    return get_domain(url).lstrip('www.')

def is_blocked(url):
    for p in BLOCKED_COMPILED:
        if p.search(url):
            return True
    return False

def is_allowed(url):
    domain = get_domain_stripped(url)
    return any(a in domain for a in ALLOWED_DOMAINS)

def is_tier1(url):
    domain = get_domain_stripped(url)
    return any(t in domain for t in TIER1_DOMAINS)

def has_sd_keyword(url):
    url_lower = url.lower()
    return any(kw in url_lower for kw in SD_URL_KEYWORDS)

def passes_domain_path_allowlist(url):
    domain = get_domain(url)
    allowed_paths = DOMAIN_PATH_ALLOWLIST.get(domain) or \
                    DOMAIN_PATH_ALLOWLIST.get(domain.lstrip('www.'))
    if allowed_paths is None:
        return True
    try:
        path = urlparse(url).path
        return any(path.startswith(prefix) for prefix in allowed_paths)
    except:
        return False

def is_quality_github(url):
    parsed = urlparse(url)
    if 'github.com' not in parsed.netloc:
        return True
    path = parsed.path.strip('/')
    parts = path.split('/')
    if path in ('', 'explore', 'trending', 'topics'):
        return False
    if len(parts) == 1:
        return False
    if len(parts) >= 2:
        repo = f"{parts[0]}/{parts[1]}"
        if repo.lower() in [r.lower() for r in GITHUB_QUALITY_REPOS]:
            if len(parts) == 2:
                return True
            if len(parts) >= 3 and parts[2] in ('blob', 'tree', 'wiki'):
                return True
            return False
    url_lower = url.lower()
    sd_kw = ['system-design', 'system_design', 'distributed', 'scalability',
             'architecture', 'awesome-system']
    if any(kw in url_lower for kw in sd_kw) and len(parts) == 2:
        return True
    return False

def is_quality_url(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ('http', 'https') or not parsed.netloc:
            return False, "invalid"
    except:
        return False, "invalid"

    if is_blocked(url):
        return False, "blocked"

    if not is_allowed(url):
        return False, "domain not allowed"

    if not passes_domain_path_allowlist(url):
        return False, "path not in allowlist"

    domain = get_domain_stripped(url)

    if 'github.com' in domain:
        return (True, "quality github") if is_quality_github(url) else (False, "junk github")

    if 'medium.com' in domain:
        return (True, "quality medium") if has_sd_keyword(url) else (False, "non-sd medium")

    if any(t in domain for t in TIER3_DOMAINS):
        return (True, "quality edu") if has_sd_keyword(url) else (False, "non-sd edu")

    if any(t in domain for t in TIER2_DOMAINS):
        return True, "tier2"

    if is_tier1(url):
        return True, "tier1"

    if has_sd_keyword(url):
        return True, "has keyword"

    return False, "no relevance"

def clean_url(url):
    try:
        parsed = urlparse(url)
        if parsed.query:
            params = parse_qs(parsed.query)
            clean = {k: v for k, v in params.items()
                     if not any(t in k.lower() for t in
                               ['utm_', 'ref', 'source', 'campaign', 'fbclid', 'gclid'])}
            if clean:
                qs = '&'.join(f"{k}={v[0]}" for k, v in clean.items())
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{qs}"
            else:
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if '#' in url:
            url = url.split('#')[0]
        return url.rstrip('/')
    except:
        return url

# ============================================================
# PER-DOMAIN RATE LIMITER
# Replaces the old global time.sleep(1.0) inside crawl_url.
#
# Why this is better:
#   - Old approach: every worker sleeps 1s after EVERY page,
#     regardless of domain. 3 workers = max ~180 URLs/hour total.
#   - New approach: each domain gets its own 0.5s cooldown.
#     Workers crawling DIFFERENT domains run fully in parallel
#     with zero sleep. Only workers hitting the SAME domain
#     throttle against each other. Result: N domains in flight
#     simultaneously, each politely paced.
# ============================================================
class DomainRateLimiter:
    """Thread-safe per-domain rate limiter using token bucket approach."""

    def __init__(self, delay_seconds: float = 0.5):
        self._delay   = delay_seconds
        self._last    = {}   # domain -> last-request timestamp
        self._locks   = defaultdict(threading.Lock)
        self._meta    = threading.Lock()

    def _get_lock(self, domain: str) -> threading.Lock:
        with self._meta:
            return self._locks[domain]

    def wait(self, url: str):
        """Block if needed to honour per-domain rate limit, then mark access."""
        domain = get_domain(url)
        lock   = self._get_lock(domain)
        with lock:
            now  = time.monotonic()
            last = self._last.get(domain, 0.0)
            gap  = now - last
            if gap < self._delay:
                time.sleep(self._delay - gap)
            self._last[domain] = time.monotonic()


# ============================================================
# ENHANCED SCORING ENGINE
# ============================================================
class ScoringEngine:
    def __init__(self):
        self.logger = logging.getLogger('ScoringEngine')

    def get_source_authority(self, url: str) -> float:
        domain = get_domain_stripped(url)
        if 'github.com' in domain:
            parsed = urlparse(url)
            parts = parsed.path.strip('/').split('/')
            if len(parts) >= 2:
                repo = f"{parts[0]}/{parts[1]}"
                for known_repo, score in GITHUB_REPO_AUTHORITY.items():
                    if repo.lower() == known_repo.lower():
                        return score
        for auth_domain, score in SOURCE_AUTHORITY.items():
            if auth_domain in domain:
                return score
        return 0.3

    def calculate_relevance(self, html: str) -> tuple:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for tag in soup.find_all(['script', 'style', 'nav', 'footer',
                                       'header', 'aside', 'noscript']):
                tag.decompose()
            text = soup.get_text(separator=' ').lower()
            text = re.sub(r'\s+', ' ', text)

            total_weight = 0
            found = []
            keyword_count = 0
            for kw, weight in KEYWORD_WEIGHTS.items():
                count = text.count(kw.lower())
                if count > 0:
                    total_weight += weight * count
                    keyword_count += count
                    found.append((kw, count, weight))

            normalized = 1 - math.exp(-RELEVANCE_ALPHA * total_weight)
            return total_weight, normalized, found, keyword_count
        except Exception as e:
            self.logger.error(f"Keyword analysis error: {e}")
            return 0, 0.0, [], 0

    def extract_publish_date(self, html: str, url: str) -> datetime:
        soup = BeautifulSoup(html, 'html.parser')
        date_str = None

        for name in ['article:published_time', 'datePublished', 'date',
                     'publish_date', 'pubdate', 'og:article:published_time']:
            tag = soup.find('meta', attrs={'property': name}) or \
                  soup.find('meta', attrs={'name': name})
            if tag and tag.get('content'):
                date_str = tag['content']
                break

        if not date_str:
            time_tag = soup.find('time', attrs={'datetime': True})
            if time_tag:
                date_str = time_tag['datetime']

        if not date_str:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        for key in ['datePublished', 'dateCreated', 'dateModified']:
                            if key in data:
                                date_str = data[key]
                                break
                except:
                    pass

        if not date_str:
            match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
            if match:
                date_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

        if date_str:
            for fmt in ['%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%SZ',
                        '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except:
                    pass
            if HAS_DATEPARSER and dateparser:
                dt = dateparser.parse(date_str)
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
        return None

    def freshness_factor(self, publish_date: datetime) -> float:
        if publish_date is None:
            days = 180
        else:
            now = datetime.now(timezone.utc)
            if publish_date.tzinfo is None:
                publish_date = publish_date.replace(tzinfo=timezone.utc)
            days = max(0, (now - publish_date).days)
        return math.exp(-LAMBDA * days)

    def content_length_bonus(self, html: str) -> float:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            length = len(text)
            if length < MIN_CONTENT_LENGTH:
                return CONTENT_LENGTH_PENALTY_MAX
            elif length >= OPTIMAL_CONTENT_LENGTH:
                return CONTENT_LENGTH_BONUS_MAX
            else:
                ratio = (length - MIN_CONTENT_LENGTH) / (OPTIMAL_CONTENT_LENGTH - MIN_CONTENT_LENGTH)
                return CONTENT_LENGTH_PENALTY_MAX + ratio * (CONTENT_LENGTH_BONUS_MAX - CONTENT_LENGTH_PENALTY_MAX)
        except:
            return 0.0

    def calculate_score(self, url: str, html: str) -> dict:
        sauth = self.get_source_authority(url)
        raw_rel, norm_rel, found_keywords, kw_hit_count = self.calculate_relevance(html)
        pub_date = self.extract_publish_date(html, url)
        fresh = self.freshness_factor(pub_date)
        len_bonus = self.content_length_bonus(html)

        source_score    = WEIGHT_SOURCE    * sauth
        relevance_score = WEIGHT_RELEVANCE * norm_rel
        freshness_score = WEIGHT_FRESHNESS * fresh
        final_score     = source_score + relevance_score + freshness_score + len_bonus
        final_score     = max(0.0, round(final_score, 2))

        days_old = (datetime.now(timezone.utc) - pub_date).days if pub_date else 180

        result = {
            'url': url, 'score': final_score,
            'source_authority': sauth,
            'source_score': round(source_score, 2),
            'raw_relevance': raw_rel,
            'kw_hit_count': kw_hit_count,
            'norm_relevance': round(norm_rel, 3),
            'relevance_score': round(relevance_score, 2),
            'freshness_factor': round(fresh, 3),
            'freshness_score': round(freshness_score, 2),
            'length_bonus': round(len_bonus, 2),
            'days_old': days_old,
            'publish_date': pub_date.isoformat() if pub_date else 'unknown',
            'top_keywords': [kw for kw, cnt, w in
                             sorted(found_keywords, key=lambda x: -x[2]*x[1])[:10]],
            'domain': get_domain_stripped(url),
        }
        self.logger.info(
            f"Score={final_score:.2f} | Src={source_score:.2f} "
            f"Rel={relevance_score:.2f} Fresh={freshness_score:.2f} "
            f"Len={len_bonus:+.2f} KwHits={kw_hit_count} | {url}"
        )
        return result


# ============================================================
# RANKED FILE WRITER
# ============================================================
class RankedFileWriter:
    def __init__(self, output_file: str):
        self.output_file  = output_file
        self.scored_links = []
        self._lock        = threading.Lock()   # protect list from concurrent writers
        self.logger       = logging.getLogger('RankedWriter')
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        self._load_existing()

    def _load_existing(self):
        if not os.path.exists(self.output_file):
            return
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue
                    match = re.match(
                        r'\[(\d+)\]\s+\[SCORE:\s*([\d.]+)\]\s+\[(\S+)\]\s+(\S+)', line)
                    if match:
                        rank, score, crawler_id, url = match.groups()
                        self.scored_links.append({
                            'url': url, 'score': float(score),
                            'crawler_id': crawler_id,
                        })
        except Exception as e:
            self.logger.error(f"Error loading existing file: {e}")

    def add_link(self, score_data: dict, crawler_id: str):
        entry = {
            'url':             score_data['url'],
            'score':           score_data['score'],
            'source_authority': score_data['source_authority'],
            'keyword_count':   score_data.get('raw_relevance', 0),
            'time_age_days':   score_data['days_old'],
            'publish_date':    score_data['publish_date'],
            'top_keywords':    score_data.get('top_keywords', []),
            'domain':          score_data['domain'],
            'crawler_id':      crawler_id,
            'crawled_at':      datetime.now().isoformat(),
        }
        with self._lock:
            for existing in self.scored_links:
                if existing['url'] == entry['url']:
                    if entry['score'] > existing['score']:
                        existing.update(entry)
                    self._write_file()
                    return
            self.scored_links.append(entry)
            self._write_file()

    def _write_file(self):
        # Called inside self._lock
        self.scored_links.sort(key=lambda x: x['score'], reverse=True)
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write(f"# RANKED SYSTEM DESIGN LINKS\n")
                f.write(f"# Score = ({WEIGHT_SOURCE}xSource) + ({WEIGHT_RELEVANCE}xKeywords) - (0.02xDaysOld)\n")
                f.write(f"# Last Updated: {datetime.now().isoformat()}\n")
                f.write(f"# Total Links: {len(self.scored_links)}\n")
                f.write(f"{'=' * 100}\n\n")
                for rank, entry in enumerate(self.scored_links, 1):
                    f.write(
                        f"[{rank:>4d}] "
                        f"[SCORE: {entry['score']:>7.2f}] "
                        f"[{entry.get('crawler_id', 'unknown'):>12s}] "
                        f"{entry['url']}\n"
                    )
                    keywords_str = ', '.join(entry.get('top_keywords', [])[:5])
                    f.write(
                        f"       "
                        f"Source={entry.get('source_authority', 0):.2f} "
                        f"Keywords={entry.get('keyword_count', 0)} "
                        f"Age={entry.get('time_age_days', 0)}d "
                        f"Published={entry.get('publish_date', 'unknown')} "
                        f"Domain={entry.get('domain', 'unknown')}\n"
                    )
                    if keywords_str:
                        f.write(f"       Top: {keywords_str}\n")
                    f.write(f"\n")
                f.write(f"{'=' * 100}\n")
                f.write(f"# SUMMARY\n")
                if self.scored_links:
                    scores = [l['score'] for l in self.scored_links]
                    f.write(f"# Total Links:  {len(self.scored_links)}\n")
                    f.write(f"# Highest:      {max(scores):.2f}\n")
                    f.write(f"# Lowest:       {min(scores):.2f}\n")
                    f.write(f"# Average:      {sum(scores)/len(scores):.2f}\n")
                    domains = {}
                    for l in self.scored_links:
                        d = l.get('domain', 'unknown')
                        domains[d] = domains.get(d, 0) + 1
                    f.write(f"#\n# DOMAINS:\n")
                    for d, count in sorted(domains.items(), key=lambda x: -x[1]):
                        f.write(f"#   {d}: {count} links\n")
        except Exception as e:
            self.logger.error(f"Error writing file: {e}")


# ============================================================
# CRAWLER WITH FULL CPU UTILIZATION
# ============================================================
class RankedCrawler:
    # ── Worker count strategy ──────────────────────────────────────────
    # Crawling is I/O-bound (network + HTML parsing). The GIL barely
    # matters here; threads spend nearly all their time waiting on
    # network I/O. Setting workers = cpu_count * 8 keeps every CPU
    # busy scheduling threads and parsing HTML even when many workers
    # are blocked on network.  The env var CRAWLER_WORKERS overrides
    # this so you can tune per machine:
    #   Mac (8-core M1):   default → 64 workers
    #   Windows (4-core):  default → 32 workers
    #   Oracle (16-core):  default → 128 workers
    # ──────────────────────────────────────────────────────────────────
    DEFAULT_WORKERS_PER_CPU = 8

    def __init__(self, crawler_id, queue_server, file_server, max_workers=None):
        self.crawler_id = crawler_id
        self.logger     = logging.getLogger(crawler_id)

        # Auto-scale if not explicitly set
        if max_workers is None:
            cpu_count   = os.cpu_count() or 4
            max_workers = cpu_count * self.DEFAULT_WORKERS_PER_CPU
            self.logger.info(
                f"Auto-scaled workers: {cpu_count} CPUs x {self.DEFAULT_WORKERS_PER_CPU}"
                f" = {max_workers} workers"
            )

        self.max_workers = max_workers
        self.logger.info(f"Worker pool size: {self.max_workers}")

        self.scoring       = ScoringEngine()
        self.ranked_writer = RankedFileWriter('./output/links.txt')
        self.rate_limiter  = DomainRateLimiter(delay_seconds=0.5)

        # Thread-safe visited set
        self._visited_lock     = threading.Lock()
        self.visited_in_session = set()

        # Build a connection-pooled session with enough connections to
        # saturate all workers without exhausting file descriptors.
        pool_size = max_workers + 10
        self.session = requests.Session()
        retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=pool_size,
            pool_maxsize=pool_size,
        )
        self.session.mount('http://',  adapter)
        self.session.mount('https://', adapter)
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36'
            )
        })

        self.queue_channel = grpc.insecure_channel(queue_server)
        self.queue_stub    = crawler_pb2_grpc.QueueServiceStub(self.queue_channel)
        self.file_channel  = grpc.insecure_channel(file_server)
        self.file_stub     = crawler_pb2_grpc.FileServiceStub(self.file_channel)

        self.stats      = defaultdict(int)
        self._stats_lock = threading.Lock()

    # ── visited helpers (thread-safe) ─────────────────────────────────
    def _is_visited(self, url: str) -> bool:
        with self._visited_lock:
            return url in self.visited_in_session

    def _set_visited(self, url: str):
        with self._visited_lock:
            self.visited_in_session.add(url)

    # ── gRPC helpers ──────────────────────────────────────────────────
    def _get_next_url(self):
        try:
            resp = self.queue_stub.GetNextURL(
                crawler_pb2.GetNextURLRequest(crawler_id=self.crawler_id), timeout=10)
            return resp.url, resp.queue_empty
        except:
            return "", True

    def _add_urls(self, urls):
        if not urls:
            return
        with self._visited_lock:
            new_urls = [u for u in urls if u not in self.visited_in_session]
        if not new_urls:
            return
        try:
            resp = self.queue_stub.AddURLs(
                crawler_pb2.AddURLsRequest(
                    urls=new_urls, crawler_id=self.crawler_id), timeout=10)
            self.logger.info(f"Added {resp.added_count} new URLs")
        except Exception as e:
            self.logger.error(f"AddURLs error: {e}")

    def _mark_visited(self, url):
        self._set_visited(url)
        try:
            self.queue_stub.MarkVisited(
                crawler_pb2.MarkVisitedRequest(
                    url=url, crawler_id=self.crawler_id), timeout=10)
        except:
            pass

    # ── core crawl task (runs inside thread pool) ──────────────────────
    def crawl_url(self, url):
        if self._is_visited(url):
            return

        ok, reason = is_quality_url(url)
        if not ok:
            self.logger.info(f"Skip ({reason}): {url}")
            self._mark_visited(url)
            with self._stats_lock:
                self.stats['filtered_url'] += 1
            return

        # Per-domain polite delay — blocks only threads hitting the
        # SAME domain; workers on other domains run unimpeded.
        self.rate_limiter.wait(url)

        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            ct = resp.headers.get('content-type', '')
            if 'text/html' not in ct:
                self._mark_visited(url)
                with self._stats_lock:
                    self.stats['filtered_url'] += 1
                return

            html       = resp.text
            score_data = self.scoring.calculate_score(url, html)

            kw_hits = score_data.get('kw_hit_count', 0)
            if kw_hits < MIN_KEYWORD_COUNT:
                self.logger.info(
                    f"Too few keywords ({kw_hits} < {MIN_KEYWORD_COUNT}): {url}")
                self._mark_visited(url)
                with self._stats_lock:
                    self.stats['filtered_content'] += 1
                return

            if score_data['score'] < 2.0 and not is_tier1(url):
                self.logger.info(
                    f"Low score ({score_data['score']:.2f}): {url}")
                self._mark_visited(url)
                with self._stats_lock:
                    self.stats['filtered_content'] += 1
                return

            self.ranked_writer.add_link(score_data, self.crawler_id)
            self._store_to_grpc(url, score_data['score'])
            with self._stats_lock:
                self.stats['stored']  += 1
                self.stats['crawled'] += 1

            self._mark_visited(url)

            links = self._extract_links(url, html)
            if links:
                self._add_urls(links)

        except Exception as e:
            self.logger.error(f"Error crawling {url}: {e}")
            self._mark_visited(url)
            with self._stats_lock:
                self.stats['errors'] += 1

    def _extract_links(self, url, html):
        links = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            for tag in soup.find_all(['nav', 'footer', 'aside', 'header',
                                       'menu', 'toolbar']):
                tag.decompose()
            for tag in soup.find_all(
                    attrs={'role': ['navigation', 'banner',
                                    'contentinfo', 'complementary']}):
                tag.decompose()
            for tag in soup.find_all(class_=re.compile(
                    r'(nav|navbar|sidebar|footer|header|menu|breadcrumb|'
                    r'related|social|share|ad-|advertisement)', re.I)):
                tag.decompose()

            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue
                absolute = clean_url(urljoin(url, href))
                ok, _ = is_quality_url(absolute)
                if ok:
                    links.append(absolute)
        except:
            pass
        return list(set(links))

    def _store_to_grpc(self, url, score):
        try:
            self.file_stub.StoreLink(
                crawler_pb2.StoreLinkRequest(
                    url=url, crawler_id=self.crawler_id,
                    timestamp=int(time.time())), timeout=10)
        except:
            pass

    # ── run loop: keep the thread pool 100% saturated ─────────────────
    #
    # Old approach:
    #   while crawled < max_urls:
    #       fetch max_workers URLs from queue  →  submit to pool
    #       if len(futures) >= max_workers*2:  →  wait for half to finish
    #
    # Problem: the "wait" stalls the producer, so workers sit idle
    # between batches. With 3 workers and 1s sleep, this was the
    # dominant bottleneck.
    #
    # New approach (producer-consumer with a bounded work queue):
    #   Producer thread:  drains the gRPC queue → work_queue (maxsize=pool)
    #   Consumer threads: ThreadPoolExecutor pulling from work_queue
    #
    # The pool is always full. The only idle time is genuine queue
    # exhaustion (nothing left to crawl), not scheduling gaps.
    # ──────────────────────────────────────────────────────────────────
    def run(self, max_urls=200):
        import queue as _queue

        self.logger.info(
            f"Crawler ({self.crawler_id}) starting with "
            f"{self.max_workers} workers | max_urls={max_urls}"
        )

        work_queue  = _queue.Queue(maxsize=self.max_workers * 4)
        crawled_cnt = [0]   # mutable int shared with producer
        done_event  = threading.Event()

        # ── producer: fills work_queue from gRPC queue ──────────────
        def producer():
            consecutive_empty = 0
            while not done_event.is_set():
                if crawled_cnt[0] >= max_urls:
                    break
                url, queue_empty = self._get_next_url()
                if queue_empty or not url:
                    consecutive_empty += 1
                    if consecutive_empty >= 20:
                        self.logger.info("Queue empty — producer stopping.")
                        break
                    time.sleep(0.5)
                    continue
                consecutive_empty = 0
                if not self._is_visited(url):
                    work_queue.put(url)   # blocks if pool backlog is full
            # Signal consumers that no more URLs are coming
            for _ in range(self.max_workers):
                work_queue.put(None)

        producer_thread = threading.Thread(target=producer, daemon=True,
                                           name='URLProducer')
        producer_thread.start()

        # ── consumers: ThreadPoolExecutor pulling from work_queue ────
        def consumer():
            while True:
                url = work_queue.get()
                if url is None:
                    # Poison pill — forward to other consumers, then exit
                    work_queue.put(None)
                    work_queue.task_done()
                    break
                try:
                    self.crawl_url(url)
                    with self._stats_lock:
                        crawled_cnt[0] += 1
                    if crawled_cnt[0] % 25 == 0:
                        self.print_stats()
                finally:
                    work_queue.task_done()

        with ThreadPoolExecutor(max_workers=self.max_workers,
                                thread_name_prefix='Crawler') as pool:
            futures = [pool.submit(consumer) for _ in range(self.max_workers)]
            producer_thread.join()
            done_event.set()
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    self.logger.error(f"Consumer exception: {e}")

        self.print_stats()

    def print_stats(self):
        with self._stats_lock:
            s = dict(self.stats)
        self.logger.info(
            f"Stats | Crawled={s.get('crawled',0)} Stored={s.get('stored',0)} "
            f"Filtered(URL)={s.get('filtered_url',0)} "
            f"Filtered(Content)={s.get('filtered_content',0)} "
            f"Errors={s.get('errors',0)} | Workers={self.max_workers}"
        )

    def close(self):
        self.queue_channel.close()
        self.file_channel.close()
        self.session.close()


# ============================================================
# METRICS REPORTER
# ============================================================
class MetricsReporter(threading.Thread):
    INTERVAL = 5

    def __init__(self, crawler_id, dashboard_url, queue_stub, file_stub):
        super().__init__(daemon=True, name='MetricsReporter')
        self.crawler_id    = crawler_id
        self.dashboard_url = dashboard_url.rstrip('/')
        self.queue_stub    = queue_stub
        self.file_stub     = file_stub
        self.start_time    = time.time()
        self.q_rtts        = deque(maxlen=60)
        self.f_rtts        = deque(maxlen=60)
        self.logger        = logging.getLogger('MetricsReporter')
        self._http         = requests.Session()
        self._http.headers.update({'Content-Type': 'application/json'})
        self._running      = True

    def _probe(self, fn, req):
        try:
            t0 = time.perf_counter()
            fn(req, timeout=5)
            return round((time.perf_counter() - t0) * 1000, 1)
        except Exception:
            return -1.0

    def _stats(self, rtts):
        v = [x for x in rtts if x >= 0]
        if not v:
            return {'current': -1, 'avg': -1, 'p95': -1, 'min': -1, 'max': -1}
        s = sorted(v)
        n = len(s)
        return {
            'current': rtts[-1] if rtts else -1,
            'avg':     round(statistics.mean(s), 1),
            'p95':     round(s[int(n * 0.95)], 1),
            'min':     round(s[0], 1),
            'max':     round(s[-1], 1),
        }

    def run(self):
        self.logger.info(f"Metrics reporter started -> {self.dashboard_url}")
        while self._running:
            q_rtt = self._probe(self.queue_stub.GetStats,
                                crawler_pb2.GetStatsRequest())
            f_rtt = self._probe(
                self.file_stub.StoreLink,
                crawler_pb2.StoreLinkRequest(
                    url='__rtt_probe__', crawler_id=self.crawler_id, timestamp=0))
            self.q_rtts.append(q_rtt)
            self.f_rtts.append(f_rtt)
            payload = {
                'crawler_id': self.crawler_id,
                'timestamp':  time.time(),
                'uptime_sec': time.time() - self.start_time,
                'queue_rtt':  self._stats(self.q_rtts),
                'file_rtt':   self._stats(self.f_rtts),
            }
            try:
                self._http.post(
                    f"{self.dashboard_url}/api/report-metrics",
                    json=payload, timeout=5)
            except Exception:
                pass
            self.logger.info(f"Q={q_rtt:>7.1f}ms  F={f_rtt:>7.1f}ms")
            time.sleep(self.INTERVAL)

    def stop(self):
        self._running = False


# ============================================================
# SEED URLS
# ============================================================
def seed_urls(queue_stub):
    seeds = [
        "https://bytebytego.com/courses/system-design-interview/scale-from-zero-to-millions-of-users",
        "https://www.hellointerview.com/learn/system-design/in-a-hurry/introduction",
        "https://www.hellointerview.com/learn/system-design/in-a-hurry/core-concepts",
        "https://www.hellointerview.com/learn/system-design/in-a-hurry/key-technologies",
        "https://www.hellointerview.com/learn/system-design/problem-breakdowns/uber",
        "https://www.hellointerview.com/learn/system-design/problem-breakdowns/instagram",
        "https://www.hellointerview.com/learn/system-design/problem-breakdowns/distributed-rate-limiter",
        "https://www.hellointerview.com/learn/ml-system-design/in-a-hurry/introduction",
        "https://www.hellointerview.com/learn/low-level-design/in-a-hurry/introduction",
        "https://www.hellointerview.com/blog/staff-level-system-design",
        "https://highscalability.com",
        "https://martinfowler.com/articles/patterns-of-distributed-systems",
        "https://architecturenotes.co",
        "https://github.com/donnemartin/system-design-primer",
        "https://github.com/karanpratapsingh/system-design",
        "https://github.com/ByteByteGoHq/system-design-101",
        "https://github.com/ashishps1/awesome-system-design-resources",
        "https://github.com/binhnguyennus/awesome-scalability",
        "https://netflixtechblog.com",
        "https://eng.uber.com",
        "https://engineering.fb.com",
        "https://slack.engineering",
        "https://dropbox.tech",
        "https://airbnb.io",
        "https://www.designgurus.io/course/grokking-the-system-design-interview",
        "https://www.educative.io/courses/grokking-modern-system-design-interview-for-engineers-managers",
    ]
    try:
        resp = queue_stub.SeedURLs(
            crawler_pb2.SeedURLsRequest(urls=seeds), timeout=10)
        logging.info(f"Seeded {resp.seeded_count} URLs")
    except grpc.RpcError as e:
        logging.error(f"Seed error: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    queue_server  = os.environ.get('QUEUE_SERVER',      'localhost:50051')
    file_server   = os.environ.get('FILE_SERVER',       'localhost:50052')
    crawler_id    = os.environ.get('CRAWLER_ID',        'crawler-1')
    max_urls      = int(os.environ.get('MAX_URLS',      '200'))
    should_seed   = os.environ.get('SEED_URLS',         'false').lower() == 'true'
    dashboard_url = os.environ.get('DASHBOARD_URL',     'http://localhost:8080')

    # CRAWLER_WORKERS overrides auto-scaling if you want manual control.
    # Leave unset to let each machine auto-detect its own CPU count.
    workers_env = os.environ.get('CRAWLER_WORKERS')
    max_workers = int(workers_env) if workers_env else None

    crawler = RankedCrawler(crawler_id, queue_server, file_server,
                            max_workers=max_workers)

    reporter = MetricsReporter(
        crawler_id    = crawler_id,
        dashboard_url = dashboard_url,
        queue_stub    = crawler.queue_stub,
        file_stub     = crawler.file_stub,
    )
    reporter.start()

    try:
        if should_seed:
            seed_urls(crawler.queue_stub)
            time.sleep(2)
        crawler.run(max_urls=max_urls)
    finally:
        reporter.stop()
        crawler.close()


if __name__ == '__main__':
    main()