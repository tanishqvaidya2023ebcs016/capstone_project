import sys
import os
import time
import logging
from datetime import datetime, timedelta
from collections import deque

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [LPM] %(message)s'
)
logger = logging.getLogger(__name__)


class LPMMonitor:
    """
    Links Per Minute Monitor.

    How it works:
    1. Every second, polls GetStats from Queue Server
    2. Records (timestamp, visited_count) snapshots
    3. Calculates LPM over different rolling windows
    4. Displays live dashboard

    Calculation:
        LPM = (links_now - links_at_window_start) / minutes_elapsed

    Example:
        At t=0:   visited = 0
        At t=10m: visited = 500
        LPM = 500 / 10 = 50 LPM
    """

    def __init__(self, queue_server: str):
        self.queue_server = queue_server

        # gRPC connection
        self.channel = grpc.insecure_channel(
            queue_server,
            options=[
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
            ]
        )
        self.stub = crawler_pb2_grpc.QueueServiceStub(self.channel)

        # Snapshots: list of (timestamp, visited_count, queue_size)
        self.snapshots = deque(maxlen=7200)  # 2 hours at 1/sec

        # Tracking
        self.start_time = time.time()
        self.start_visited = 0
        self.first_poll = True

        # Peak tracking
        self.peak_lpm_1min = 0.0
        self.peak_lpm_1min_time = None
        self.peak_lpm_5min = 0.0

        # Per-interval tracking for sparkline
        self.per_minute_counts = deque(maxlen=60)
        self.last_minute_mark = time.time()
        self.last_minute_visited = 0

    def _poll_stats(self) -> dict:
        """Poll current stats from Queue Server."""
        try:
            response = self.stub.GetStats(
                crawler_pb2.GetStatsRequest(),
                timeout=5
            )
            return {
                'visited': response.visited_count,
                'queue_size': response.queue_size,
                'timestamp': time.time(),
                'connected': True,
            }
        except grpc.RpcError as e:
            return {
                'visited': 0,
                'queue_size': 0,
                'timestamp': time.time(),
                'connected': False,
                'error': str(e.code()),
            }

    def _calculate_lpm(self, window_seconds: int) -> float:
        """
        Calculate LPM over a rolling window.

        window_seconds=60  → LPM over last 1 minute
        window_seconds=300 → LPM over last 5 minutes
        """
        if len(self.snapshots) < 2:
            return 0.0

        now = time.time()
        cutoff = now - window_seconds

        # Find the snapshot closest to window start
        window_start_snapshot = None
        for snapshot in self.snapshots:
            if snapshot['timestamp'] >= cutoff:
                window_start_snapshot = snapshot
                break

        if window_start_snapshot is None:
            return 0.0

        latest = self.snapshots[-1]

        links_in_window = latest['visited'] - window_start_snapshot['visited']
        time_elapsed = latest['timestamp'] - window_start_snapshot['timestamp']

        if time_elapsed < 1:  # Less than 1 second
            return 0.0

        minutes_elapsed = time_elapsed / 60.0
        lpm = links_in_window / minutes_elapsed

        return round(lpm, 2)

    def _calculate_overall_lpm(self) -> float:
        """Calculate LPM since monitoring started."""
        if not self.snapshots:
            return 0.0

        latest = self.snapshots[-1]
        total_links = latest['visited'] - self.start_visited
        elapsed_minutes = (time.time() - self.start_time) / 60.0

        if elapsed_minutes < 0.01:
            return 0.0

        return round(total_links / elapsed_minutes, 2)

    def _calculate_instantaneous_lpm(self) -> float:
        """LPM based on last 5 seconds (instant speed)."""
        return self._calculate_lpm(5)

    def _get_links_per_second(self) -> float:
        """Current links per second."""
        if len(self.snapshots) < 2:
            return 0.0

        latest = self.snapshots[-1]
        previous = self.snapshots[-2]

        links_diff = latest['visited'] - previous['visited']
        time_diff = latest['timestamp'] - previous['timestamp']

        if time_diff < 0.01:
            return 0.0

        return round(links_diff / time_diff, 2)

    def _generate_sparkline(self) -> str:
        """Generate ASCII sparkline of LPM over last 30 snapshots."""
        if len(self.snapshots) < 60:
            return "collecting data..."

        # Sample every 30 seconds for 30 data points
        points = []
        snapshot_list = list(self.snapshots)
        step = max(len(snapshot_list) // 30, 1)

        for i in range(0, len(snapshot_list) - step, step):
            s1 = snapshot_list[i]
            s2 = snapshot_list[min(i + step, len(snapshot_list) - 1)]

            links = s2['visited'] - s1['visited']
            elapsed = (s2['timestamp'] - s1['timestamp']) / 60.0
            if elapsed > 0:
                points.append(links / elapsed)
            else:
                points.append(0)

        if not points:
            return "no data"

        # Only take last 30
        points = points[-30:]

        max_val = max(max(points), 1)
        chars = " ▁▂▃▄▅▆▇█"

        sparkline = ""
        for val in points:
            idx = min(int(val / max_val * (len(chars) - 1)), len(chars) - 1)
            sparkline += chars[idx]

        return sparkline

    def _estimate_completion(self, current_stats: dict) -> str:
        """Estimate when queue will be drained."""
        lpm = self._calculate_lpm(60)
        if lpm <= 0:
            return "N/A"

        queue_size = current_stats['queue_size']
        if queue_size == 0:
            return "Queue empty!"

        minutes_remaining = queue_size / lpm
        eta = datetime.now() + timedelta(minutes=minutes_remaining)

        if minutes_remaining < 60:
            return f"{minutes_remaining:.0f}min (ETA: {eta.strftime('%H:%M:%S')})"
        else:
            hours = minutes_remaining / 60
            return f"{hours:.1f}hrs (ETA: {eta.strftime('%H:%M:%S')})"

    def _print_dashboard(self, stats: dict):
        """Print the LPM dashboard."""
        elapsed = time.time() - self.start_time
        elapsed_str = str(timedelta(seconds=int(elapsed)))

        total_links = stats['visited'] - self.start_visited

        # Calculate all LPMs
        lpm_instant = self._calculate_instantaneous_lpm()
        lpm_1min = self._calculate_lpm(60)
        lpm_5min = self._calculate_lpm(300)
        lpm_10min = self._calculate_lpm(600)
        lpm_overall = self._calculate_overall_lpm()
        lps = self._get_links_per_second()

        # Update peaks
        if lpm_1min > self.peak_lpm_1min:
            self.peak_lpm_1min = lpm_1min
            self.peak_lpm_1min_time = datetime.now()
        if lpm_5min > self.peak_lpm_5min:
            self.peak_lpm_5min = lpm_5min

        # New links since last poll
        new_links = 0
        if len(self.snapshots) >= 2:
            new_links = (self.snapshots[-1]['visited'] -
                        self.snapshots[-2]['visited'])

        # Sparkline
        sparkline = self._generate_sparkline()

        # ETA
        eta = self._estimate_completion(stats)

        # Connection status
        conn_status = "🟢 CONNECTED" if stats['connected'] else "🔴 DISCONNECTED"

        # LPM health
        if lpm_1min > 0:
            if lpm_1min >= lpm_overall * 0.8:
                lpm_health = "🟢 STEADY"
            elif lpm_1min >= lpm_overall * 0.5:
                lpm_health = "🟡 SLOWING"
            else:
                lpm_health = "🔴 DEGRADED"
        else:
            lpm_health = "⚫ NO DATA"

        # Clear screen
        os.system('cls' if os.name == 'nt' else 'clear')

        print(f"""
╔════════════════════════════════════════════════════════════════╗
║            📊 LINKS PER MINUTE (LPM) MONITOR                  ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  Server:     {self.queue_server:>44s}  ║
║  Uptime:     {elapsed_str:>44s}  ║
║  Status:     {conn_status:>44s}  ║
║                                                                ║
╠════════════════════════════════════════════════════════════════╣
║                     LINK COUNTS                                ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  📈 Total Crawled:     {total_links:>36d}  ║
║  📋 Queue Remaining:   {stats['queue_size']:>36d}  ║
║  🆕 New (last tick):   {new_links:>36d}  ║
║  ⚡ Links/Second:      {lps:>36.1f}  ║
║                                                                ║
╠════════════════════════════════════════════════════════════════╣
║                  CRAWL RATE (LPM)                              ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  ⚡ Instant (5s):      {lpm_instant:>32.1f} LPM  ║
║  📈 Last 1 minute:     {lpm_1min:>32.1f} LPM  ║
║  📊 Last 5 minutes:    {lpm_5min:>32.1f} LPM  ║
║  📉 Last 10 minutes:   {lpm_10min:>32.1f} LPM  ║
║  📋 Overall:           {lpm_overall:>32.1f} LPM  ║
║                                                                ║
║  🏆 Peak (1min):       {self.peak_lpm_1min:>32.1f} LPM  ║
║  🏆 Peak (5min):       {self.peak_lpm_5min:>32.1f} LPM  ║
║                                                                ║
╠════════════════════════════════════════════════════════════════╣
║                    HEALTH                                      ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  Throughput:  {lpm_health:>44s}  ║
║  ETA Empty:   {eta:>44s}  ║
║                                                                ║
╠════════════════════════════════════════════════════════════════╣
║  LPM Trend (last 15 min):                                     ║
║  [{sparkline:^54s}]  ║
╚════════════════════════════════════════════════════════════════╝

  Goal: Steady LPM even as crawl progresses
  Press Ctrl+C to stop
""")

    def _print_final_report(self):
        """Print final LPM report."""
        if not self.snapshots:
            print("\n  No data collected.\n")
            return

        elapsed = time.time() - self.start_time
        total_links = self.snapshots[-1]['visited'] - self.start_visited
        overall_lpm = self._calculate_overall_lpm()

        print(f"""
╔════════════════════════════════════════════════════════════════╗
║                   📊 FINAL LPM REPORT                          ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  Total Time:          {str(timedelta(seconds=int(elapsed))):>36s}  ║
║  Total Links:         {total_links:>36d}  ║
║  Overall LPM:         {overall_lpm:>36.2f}  ║
║  Peak LPM (1min):     {self.peak_lpm_1min:>36.2f}  ║
║  Peak LPM (5min):     {self.peak_lpm_5min:>36.2f}  ║
║  Queue Remaining:     {self.snapshots[-1]['queue_size']:>36d}  ║
║                                                                ║
║  Interpretation:                                               ║""")

        if overall_lpm >= 40:
            print("║    🟢 Excellent throughput!                                  ║")
        elif overall_lpm >= 20:
            print("║    🟢 Good throughput for distributed crawl                  ║")
        elif overall_lpm >= 10:
            print("║    🟡 Moderate — check network or increase parallelism       ║")
        else:
            print("║    🔴 Low — likely bottlenecked by network or rate limiting  ║")

        print("║                                                                ║")
        print("╚════════════════════════════════════════════════════════════════╝\n")

    def run(self, poll_interval: float = 1.0):
        """Main monitoring loop."""
        logger.info(f"Starting LPM Monitor → {self.queue_server}")
        logger.info(f"Poll interval: {poll_interval}s")

        # Initial poll to get starting count
        initial = self._poll_stats()
        if initial['connected']:
            self.start_visited = initial['visited']
            logger.info(f"Starting visited count: {self.start_visited}")
        else:
            logger.error("Cannot connect to Queue Server!")

        try:
            while True:
                stats = self._poll_stats()
                if stats['connected']:
                    self.snapshots.append(stats)
                    self._print_dashboard(stats)
                else:
                    print(f"\n  🔴 Cannot reach {self.queue_server}")
                    print(f"     Error: {stats.get('error', 'unknown')}")
                    print(f"     Retrying in {poll_interval}s...\n")

                time.sleep(poll_interval)

        except KeyboardInterrupt:
            self._print_final_report()


def main():
    queue_server = os.environ.get('QUEUE_SERVER', 'localhost:50051')
    interval = float(os.environ.get('LPM_INTERVAL', '1'))

    monitor = LPMMonitor(queue_server)
    monitor.run(poll_interval=interval)


if __name__ == '__main__':
    main()