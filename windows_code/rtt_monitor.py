"""
gRPC Round-Trip Time (RTT) Monitor

Measures latency of gRPC calls over Tailscale VPN tunnel.
- Sends probe requests to Queue Server and File Server
- Measures time for each gRPC operation
- Classifies health: Excellent/Good/Warning/Critical

Healthy Range:
    < 20ms  → Excellent (local or fast VPN)
    20-80ms → Good (normal Tailscale)
    80-200ms → Warning (congestion possible)
    > 200ms  → Critical (network issues)

Run on Mac:
    QUEUE_SERVER=localhost:50051 FILE_SERVER=100.x.x.2:50052 python monitor/rtt_monitor.py

Run on Windows:
    set QUEUE_SERVER=100.x.x.1:50051
    set FILE_SERVER=localhost:50052
    python monitor/rtt_monitor.py
"""

import sys
import os
import time
import logging
import statistics
from datetime import datetime, timedelta
from collections import deque

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [RTT] %(message)s'
)
logger = logging.getLogger(__name__)


def classify_rtt(rtt_ms: float) -> tuple:
    """
    Classify RTT health.
    Returns (label, emoji).
    """
    if rtt_ms < 0:
        return "DISCONNECTED", "⚫"
    elif rtt_ms < 20:
        return "EXCELLENT", "🟢"
    elif rtt_ms <= 80:
        return "GOOD", "🟢"
    elif rtt_ms <= 200:
        return "WARNING", "🟡"
    else:
        return "CRITICAL", "🔴"


def rtt_bar(rtt_ms: float, max_ms: float = 300, width: int = 25) -> str:
    """Generate visual bar for RTT."""
    if rtt_ms < 0:
        return "×" * width + " TIMEOUT"

    filled = min(int((rtt_ms / max_ms) * width), width)
    empty = width - filled

    if rtt_ms < 20:
        bar = "█" * filled + "·" * empty
    elif rtt_ms <= 80:
        bar = "▓" * filled + "·" * empty
    elif rtt_ms <= 200:
        bar = "▒" * filled + "·" * empty
    else:
        bar = "!" * filled + "·" * empty

    return bar


class RTTProbe:
    """Sends gRPC probe calls and measures round-trip time."""

    def __init__(self, queue_server: str, file_server: str = None):
        self.queue_server = queue_server
        self.file_server = file_server

        # Queue Server channel
        self.queue_channel = grpc.insecure_channel(
            queue_server,
            options=[
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
            ]
        )
        self.queue_stub = crawler_pb2_grpc.QueueServiceStub(self.queue_channel)

        # File Server channel
        self.file_stub = None
        self.file_channel = None
        if file_server:
            self.file_channel = grpc.insecure_channel(
                file_server,
                options=[
                    ('grpc.keepalive_time_ms', 10000),
                    ('grpc.keepalive_timeout_ms', 5000),
                ]
            )
            self.file_stub = crawler_pb2_grpc.FileServiceStub(self.file_channel)

    def probe_queue_getstats(self) -> float:
        """
        Probe: GetStats call to Queue Server.
        Lightweight — just returns counts.
        Returns RTT in milliseconds. -1 if failed.
        """
        try:
            start = time.perf_counter()
            self.queue_stub.GetStats(
                crawler_pb2.GetStatsRequest(),
                timeout=5
            )
            end = time.perf_counter()
            return (end - start) * 1000
        except grpc.RpcError:
            return -1.0

    def probe_queue_isvisited(self) -> float:
        """
        Probe: IsVisited call to Queue Server.
        Simulates the check a crawler does before processing.
        """
        try:
            start = time.perf_counter()
            self.queue_stub.IsVisited(
                crawler_pb2.IsVisitedRequest(url="https://rtt-probe.test/check"),
                timeout=5
            )
            end = time.perf_counter()
            return (end - start) * 1000
        except grpc.RpcError:
            return -1.0

    def probe_file_server(self) -> float:
        """
        Probe: StoreLink call to File Server.
        Uses a dummy probe URL that file server can handle.
        """
        if not self.file_stub:
            return -1.0

        try:
            start = time.perf_counter()
            self.file_stub.StoreLink(
                crawler_pb2.StoreLinkRequest(
                    url="__rtt_probe__",
                    crawler_id="rtt-monitor",
                    timestamp=0
                ),
                timeout=5
            )
            end = time.perf_counter()
            return (end - start) * 1000
        except grpc.RpcError:
            return -1.0

    def full_probe(self) -> dict:
        """
        Run all probes and return results.
        Simulates complete crawler gRPC flow.
        """
        results = {
            'queue_getstats': self.probe_queue_getstats(),
            'queue_isvisited': self.probe_queue_isvisited(),
            'file_storelink': self.probe_file_server(),
            'timestamp': time.time(),
        }

        # Calculate aggregates
        valid = [v for k, v in results.items()
                 if isinstance(v, float) and v >= 0 and k != 'timestamp']

        results['avg'] = statistics.mean(valid) if valid else -1
        results['total'] = sum(valid) if valid else -1
        results['all_connected'] = all(v >= 0 for v in [
            results['queue_getstats'],
            results.get('file_storelink', 0)
        ])

        return results


class RTTMonitor:
    """Continuous RTT monitoring with live dashboard."""

    def __init__(self, queue_server: str, file_server: str = None):
        self.queue_server = queue_server
        self.file_server = file_server
        self.probe = RTTProbe(queue_server, file_server)

        # History
        self.queue_history = deque(maxlen=3600)    # 1 hour
        self.file_history = deque(maxlen=3600)
        self.full_history = deque(maxlen=3600)

        # Stats
        self.start_time = time.time()
        self.total_probes = 0
        self.queue_failures = 0
        self.file_failures = 0

        # Peaks
        self.peak_queue_rtt = 0.0
        self.min_queue_rtt = float('inf')
        self.peak_file_rtt = 0.0

        # Alerts
        self.alerts = deque(maxlen=10)

    def _calc_stats(self, history: deque, window: int = None) -> dict:
        """Calculate statistics from history."""
        data = list(history)
        if window:
            data = data[-window:]

        valid = [x for x in data if x >= 0]

        if not valid:
            return {
                'min': 0, 'max': 0, 'avg': 0,
                'median': 0, 'p95': 0, 'p99': 0,
                'stdev': 0, 'count': 0
            }

        sorted_data = sorted(valid)
        count = len(sorted_data)

        return {
            'min': sorted_data[0],
            'max': sorted_data[-1],
            'avg': statistics.mean(sorted_data),
            'median': sorted_data[count // 2],
            'p95': sorted_data[int(count * 0.95)] if count > 1 else sorted_data[0],
            'p99': sorted_data[int(count * 0.99)] if count > 1 else sorted_data[0],
            'stdev': statistics.stdev(sorted_data) if count > 1 else 0,
            'count': count,
        }

    def _make_sparkline(self, history: deque, count: int = 40) -> str:
        """Generate sparkline from history."""
        data = list(history)[-count:]
        if not data:
            return "waiting for data..."

        valid = [max(0, x) for x in data]
        if not valid or max(valid) == 0:
            return "▁" * len(valid)

        max_val = max(valid)
        chars = "▁▂▃▄▅▆▇█"

        sparkline = ""
        for val in valid:
            idx = min(int(val / max_val * (len(chars) - 1)), len(chars) - 1)
            sparkline += chars[idx]

        return sparkline

    def _add_alert(self, msg: str):
        """Add alert with timestamp."""
        ts = datetime.now().strftime('%H:%M:%S')
        self.alerts.append(f"[{ts}] {msg}")

    def _print_dashboard(self, results: dict):
        """Print RTT dashboard."""
        elapsed = str(timedelta(seconds=int(time.time() - self.start_time)))

        # Current RTTs
        q_rtt = results['queue_getstats']
        q_visit_rtt = results['queue_isvisited']
        f_rtt = results.get('file_storelink', -1)

        # Classifications
        q_label, q_emoji = classify_rtt(q_rtt)
        qv_label, qv_emoji = classify_rtt(q_visit_rtt)
        f_label, f_emoji = classify_rtt(f_rtt)

        # Bars
        q_bar = rtt_bar(q_rtt)
        qv_bar = rtt_bar(q_visit_rtt)
        f_bar = rtt_bar(f_rtt)

        # Historical stats
        q_1min = self._calc_stats(self.queue_history, 60)
        q_5min = self._calc_stats(self.queue_history, 300)
        q_all = self._calc_stats(self.queue_history)

        # Availability
        q_avail = ((self.total_probes - self.queue_failures) /
                   max(self.total_probes, 1) * 100)
        f_avail = ((self.total_probes - self.file_failures) /
                   max(self.total_probes, 1) * 100) if self.file_server else 0

        # Jitter
        recent = [x for x in list(self.queue_history)[-30:] if x >= 0]
        jitter = statistics.stdev(recent) if len(recent) > 1 else 0

        # Sparkline
        q_spark = self._make_sparkline(self.queue_history, 50)

        # Clear screen
        os.system('cls' if os.name == 'nt' else 'clear')

        print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║              🏓 gRPC ROUND-TRIP TIME (RTT) MONITOR                   ║
║                  Tailscale VPN Tunnel Latency                        ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Queue Server:  {self.queue_server:>50s}  ║
║  File Server:   {str(self.file_server or 'N/A'):>50s}  ║
║  Uptime:        {elapsed:>50s}  ║
║  Total Probes:  {self.total_probes:>50d}  ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║                 CURRENT RTT (milliseconds)                           ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Queue → GetStats:                                                   ║
║    {q_rtt:>7.1f} ms  {q_emoji} {q_label:<12s}  [{q_bar}]           ║
║                                                                      ║
║  Queue → IsVisited:                                                  ║
║    {q_visit_rtt:>7.1f} ms  {qv_emoji} {qv_label:<12s}  [{qv_bar}]          ║
║                                                                      ║
║  File  → StoreLink:                                                  ║
║    {f_rtt:>7.1f} ms  {f_emoji} {f_label:<12s}  [{f_bar}]           ║
║                                                                      ║
║  Combined:  {results['total']:>7.1f} ms total  |  {results['avg']:>7.1f} ms avg         ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║              QUEUE SERVER STATISTICS                                 ║
╠══════════════════════════════════════════════════════════════════════╣
║                        Min      Avg      Max      P95      P99      ║
║  Last 1 min:     {q_1min['min']:>7.1f}  {q_1min['avg']:>7.1f}  {q_1min['max']:>7.1f}  {q_1min['p95']:>7.1f}  {q_1min['p99']:>7.1f}   ║
║  Last 5 min:     {q_5min['min']:>7.1f}  {q_5min['avg']:>7.1f}  {q_5min['max']:>7.1f}  {q_5min['p95']:>7.1f}  {q_5min['p99']:>7.1f}   ║
║  All time:       {q_all['min']:>7.1f}  {q_all['avg']:>7.1f}  {q_all['max']:>7.1f}  {q_all['p95']:>7.1f}  {q_all['p99']:>7.1f}   ║
║                                                                      ║
║  Jitter (σ):     {jitter:>7.1f} ms                                           ║
║  Availability:   {q_avail:>7.1f}% (Queue)  {f_avail:>7.1f}% (File)              ║
║                                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║  RTT Trend (last 50 probes):                                        ║
║  [{q_spark:<54s}]  ║
╠══════════════════════════════════════════════════════════════════════╣
║                  HEALTH THRESHOLDS                                   ║
╠══════════════════════════════════════════════════════════════════════╣
║  🟢 EXCELLENT : <  20ms  │ Local network or fast VPN                 ║
║  🟢 GOOD      : 20-80ms  │ Healthy Tailscale tunnel                  ║
║  🟡 WARNING   : 80-200ms │ Possible network congestion               ║
║  🔴 CRITICAL  : > 200ms  │ Network issues — check Tailscale          ║
╚══════════════════════════════════════════════════════════════════════╝""")

        # Alerts
        if self.alerts:
            print(f"\n  ⚠️  Alerts:")
            for alert in list(self.alerts)[-5:]:
                print(f"     {alert}")

        print(f"\n  Press Ctrl+C to stop")

    def _print_final_report(self):
        """Final RTT summary."""
        q_all = self._calc_stats(self.queue_history)
        f_all = self._calc_stats(self.file_history)

        elapsed = str(timedelta(seconds=int(time.time() - self.start_time)))
        q_avail = ((self.total_probes - self.queue_failures) /
                   max(self.total_probes, 1) * 100)

        q_label, q_emoji = classify_rtt(q_all['avg'])

        print(f"""

╔══════════════════════════════════════════════════════════════════════╗
║                    📋 FINAL RTT REPORT                               ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Duration:       {elapsed:>50s}  ║
║  Total Probes:   {self.total_probes:>50d}  ║
║  Availability:   {q_avail:>49.1f}%  ║
║                                                                      ║
║  Queue Server RTT:                                                   ║
║    Average:   {q_all['avg']:>7.1f} ms   {q_emoji} {q_label}                              ║
║    Median:    {q_all['median']:>7.1f} ms                                              ║
║    Min:       {q_all['min']:>7.1f} ms                                              ║
║    Max:       {q_all['max']:>7.1f} ms                                              ║
║    P95:       {q_all['p95']:>7.1f} ms                                              ║
║    P99:       {q_all['p99']:>7.1f} ms                                              ║
║    Jitter:    {q_all['stdev']:>7.1f} ms                                              ║
║                                                                      ║""")

        if f_all['count'] > 0:
            f_label, f_emoji = classify_rtt(f_all['avg'])
            print(f"""║  File Server RTT:                                                    ║
║    Average:   {f_all['avg']:>7.1f} ms   {f_emoji} {f_label}                              ║
║    Min:       {f_all['min']:>7.1f} ms                                              ║
║    Max:       {f_all['max']:>7.1f} ms                                              ║
║                                                                      ║""")

        # Verdict
        print("║  Verdict:                                                        ║")
        if q_all['avg'] < 20:
            print("║    🟢 Excellent — gRPC overhead is negligible                    ║")
        elif q_all['avg'] <= 80:
            print("║    🟢 Good — Tailscale tunnel performing well                    ║")
        elif q_all['avg'] <= 200:
            print("║    🟡 Warning — consider checking network congestion             ║")
        else:
            print("║    🔴 Critical — gRPC latency is a bottleneck                    ║")

        print("║                                                                      ║")
        print("╚══════════════════════════════════════════════════════════════════════╝\n")

    def run(self, interval: float = 1.0):
        """Main monitoring loop."""
        logger.info(f"Starting RTT Monitor")
        logger.info(f"Queue: {self.queue_server}")
        logger.info(f"File:  {self.file_server}")

        try:
            while True:
                self.total_probes += 1

                results = self.probe.full_probe()

                # Record history
                q_rtt = results['queue_getstats']
                f_rtt = results.get('file_storelink', -1)

                self.queue_history.append(q_rtt)
                self.file_history.append(f_rtt)

                # Track failures
                if q_rtt < 0:
                    self.queue_failures += 1
                    if self.queue_failures % 5 == 0:
                        self._add_alert(
                            f"Queue Server: {self.queue_failures} total failures"
                        )
                else:
                    if q_rtt > self.peak_queue_rtt:
                        self.peak_queue_rtt = q_rtt
                    if q_rtt < self.min_queue_rtt:
                        self.min_queue_rtt = q_rtt

                if f_rtt < 0 and self.file_server:
                    self.file_failures += 1

                # Alerts for high RTT
                if q_rtt > 200:
                    self._add_alert(f"🔴 CRITICAL: Queue RTT = {q_rtt:.0f}ms")
                elif q_rtt > 80:
                    if self.total_probes % 30 == 0:  # Alert every 30 probes
                        self._add_alert(f"🟡 WARNING: Queue RTT = {q_rtt:.0f}ms")

                self._print_dashboard(results)
                time.sleep(interval)

        except KeyboardInterrupt:
            self._print_final_report()


def main():
    queue_server = os.environ.get('QUEUE_SERVER', 'localhost:50051')
    file_server = os.environ.get('FILE_SERVER', None)
    interval = float(os.environ.get('RTT_INTERVAL', '1'))

    # Handle empty string as None
    if file_server in ('', 'None', 'none'):
        file_server = None

    monitor = RTTMonitor(queue_server, file_server)
    monitor.run(interval=interval)


if __name__ == '__main__':
    main()