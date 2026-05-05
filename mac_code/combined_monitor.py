import sys
import os
import time
import statistics
from datetime import datetime, timedelta
from collections import deque

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'generated'))
import crawler_pb2
import crawler_pb2_grpc


def classify_rtt(rtt_ms):
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


class CombinedMonitor:
    """Shows LPM and RTT in a single dashboard."""

    def __init__(self, queue_server, file_server=None):
        self.queue_server = queue_server
        self.file_server = file_server

        # gRPC
        self.queue_channel = grpc.insecure_channel(queue_server)
        self.queue_stub = crawler_pb2_grpc.QueueServiceStub(self.queue_channel)

        self.file_stub = None
        if file_server:
            self.file_channel = grpc.insecure_channel(file_server)
            self.file_stub = crawler_pb2_grpc.FileServiceStub(self.file_channel)

        # LPM tracking
        self.start_time = time.time()
        self.start_visited = 0
        self.snapshots = deque(maxlen=3600)
        self.peak_lpm = 0.0

        # RTT tracking
        self.queue_rtts = deque(maxlen=3600)
        self.file_rtts = deque(maxlen=3600)
        self.total_probes = 0
        self.rtt_failures = 0

    def _poll_stats(self):
        """Get stats + measure RTT in one call."""
        # Measure RTT of GetStats call
        try:
            start = time.perf_counter()
            response = self.queue_stub.GetStats(
                crawler_pb2.GetStatsRequest(), timeout=5
            )
            queue_rtt = (time.perf_counter() - start) * 1000

            return {
                'visited': response.visited_count,
                'queue_size': response.queue_size,
                'queue_rtt': queue_rtt,
                'connected': True,
            }
        except grpc.RpcError:
            return {
                'visited': 0, 'queue_size': 0,
                'queue_rtt': -1, 'connected': False,
            }

    def _probe_file_server(self):
        """Measure file server RTT."""
        if not self.file_stub:
            return -1

        try:
            start = time.perf_counter()
            self.file_stub.StoreLink(
                crawler_pb2.StoreLinkRequest(
                    url="__probe__", crawler_id="monitor", timestamp=0
                ), timeout=5
            )
            return (time.perf_counter() - start) * 1000
        except grpc.RpcError:
            return -1

    def _calc_lpm(self, window_seconds):
        if len(self.snapshots) < 2:
            return 0.0

        cutoff = time.time() - window_seconds
        start_snap = None
        for s in self.snapshots:
            if s['timestamp'] >= cutoff:
                start_snap = s
                break

        if not start_snap:
            return 0.0

        latest = self.snapshots[-1]
        links = latest['visited'] - start_snap['visited']
        minutes = (latest['timestamp'] - start_snap['timestamp']) / 60

        return links / minutes if minutes > 0.01 else 0.0

    def _calc_rtt_stats(self, data, window=None):
        d = list(data)
        if window:
            d = d[-window:]
        valid = [x for x in d if x >= 0]
        if not valid:
            return {'avg': 0, 'min': 0, 'max': 0, 'p95': 0}

        s = sorted(valid)
        return {
            'avg': statistics.mean(s),
            'min': s[0],
            'max': s[-1],
            'p95': s[int(len(s) * 0.95)] if len(s) > 1 else s[0],
        }

    def _sparkline(self, data, count=40):
        d = [max(0, x) for x in list(data)[-count:]]
        if not d or max(d) == 0:
            return "·" * min(count, max(len(d), 1))

        mx = max(d)
        chars = "▁▂▃▄▅▆▇█"
        return "".join(
            chars[min(int(v / mx * (len(chars) - 1)), len(chars) - 1)]
            for v in d
        )

    def _print_dashboard(self, stats, file_rtt):
        elapsed = str(timedelta(seconds=int(time.time() - self.start_time)))
        total_links = stats['visited'] - self.start_visited

        # LPM calculations
        lpm_1m = self._calc_lpm(60)
        lpm_5m = self._calc_lpm(300)
        lpm_10m = self._calc_lpm(600)
        lpm_overall = total_links / max((time.time() - self.start_time) / 60, 0.01)

        if lpm_1m > self.peak_lpm:
            self.peak_lpm = lpm_1m

        # LPM health
        if lpm_1m >= lpm_overall * 0.8 and lpm_1m > 0:
            lpm_status = "🟢 STEADY"
        elif lpm_1m > 0:
            lpm_status = "🟡 SLOWING"
        else:
            lpm_status = "⚫ WAITING"

        # RTT values
        q_rtt = stats['queue_rtt']
        q_label, q_emoji = classify_rtt(q_rtt)
        f_label, f_emoji = classify_rtt(file_rtt)

        # RTT stats
        q_stats_1m = self._calc_rtt_stats(self.queue_rtts, 60)

        # Sparklines
        lpm_spark = self._sparkline(
            [s['visited'] for s in self.snapshots], 40
        )
        rtt_spark = self._sparkline(self.queue_rtts, 40)

        # Availability
        avail = ((self.total_probes - self.rtt_failures) /
                 max(self.total_probes, 1) * 100)

        os.system('cls' if os.name == 'nt' else 'clear')

        print(f"""
╔════════════════════════════════════════════════════════════════════════╗
║          📊 DISTRIBUTED CRAWLER — PERFORMANCE DASHBOARD               ║
╠════════════════════════════════════════════════════════════════════════╣
║  Queue Server: {self.queue_server:<20s}  Uptime: {elapsed:>20s}     ║
║  File Server:  {str(self.file_server or 'N/A'):<20s}  Probes: {self.total_probes:>20d}     ║
╠════════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║  ┌─────────── LINKS PER MINUTE ───────────┐  ┌──── gRPC RTT (ms) ───┐ ║
║  │                                         │  │                       │ ║
║  │  Total Crawled: {total_links:>10d}            │  │  Queue:  {q_rtt:>7.1f} {q_emoji}     │ ║
║  │  Queue Left:    {stats['queue_size']:>10d}            │  │  File:   {file_rtt:>7.1f} {f_emoji}     │ ║
║  │                                         │  │                       │ ║
║  │  LPM (1 min):   {lpm_1m:>10.1f}            │  │  1min avg: {q_stats_1m['avg']:>7.1f}   │ ║
║  │  LPM (5 min):   {lpm_5m:>10.1f}            │  │  1min p95: {q_stats_1m['p95']:>7.1f}   │ ║
║  │  LPM (10 min):  {lpm_10m:>10.1f}            │  │  1min max: {q_stats_1m['max']:>7.1f}   │ ║
║  │  LPM (overall): {lpm_overall:>10.1f}            │  │                       │ ║
║  │  Peak LPM:      {self.peak_lpm:>10.1f}            │  │  Availability:        │ ║
║  │                                         │  │    {avail:>7.1f}%             │ ║
║  │  Status: {lpm_status:>12s}                │  │  Status:              │ ║
║  │                                         │  │    {q_emoji} {q_label:<18s}  │ ║
║  └─────────────────────────────────────────┘  └───────────────────────┘ ║
║                                                                        ║
╠════════════════════════════════════════════════════════════════════════╣
║  LPM Trend:  [{lpm_spark:<40s}]                       ║
║  RTT Trend:  [{rtt_spark:<40s}]                       ║
╠════════════════════════════════════════════════════════════════════════╣
║  Thresholds │ RTT: 🟢<20ms  🟢20-80ms  🟡80-200ms  🔴>200ms         ║
║             │ LPM: 🟢Steady  🟡Slowing  🔴Degraded                   ║
╚════════════════════════════════════════════════════════════════════════╝

  Press Ctrl+C for final report
""")

    def _print_final_report(self):
        if not self.snapshots:
            print("\n  No data.\n")
            return

        elapsed = str(timedelta(seconds=int(time.time() - self.start_time)))
        total = self.snapshots[-1]['visited'] - self.start_visited
        lpm = total / max((time.time() - self.start_time) / 60, 0.01)
        q_stats = self._calc_rtt_stats(self.queue_rtts)
        q_label, q_emoji = classify_rtt(q_stats['avg'])
        avail = ((self.total_probes - self.rtt_failures) /
                 max(self.total_probes, 1) * 100)

        print(f"""

╔════════════════════════════════════════════════════════════════════════╗
║                       📋 FINAL REPORT                                  ║
╠════════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║  Duration:          {elapsed:>50s}     ║
║  Total Links:       {total:>50d}     ║
║  Overall LPM:       {lpm:>50.2f}     ║
║  Peak LPM:          {self.peak_lpm:>50.2f}     ║
║                                                                        ║
║  Avg Queue RTT:     {q_stats['avg']:>47.1f} ms     ║
║  P95 Queue RTT:     {q_stats['p95']:>47.1f} ms     ║
║  RTT Health:        {q_emoji} {q_label:>48s}     ║
║  Availability:      {avail:>49.1f}%     ║
║                                                                        ║
╚════════════════════════════════════════════════════════════════════════╝
""")

    def run(self, interval=1.0):
        # Get starting count
        initial = self._poll_stats()
        if initial['connected']:
            self.start_visited = initial['visited']

        try:
            while True:
                self.total_probes += 1

                stats = self._poll_stats()
                file_rtt = self._probe_file_server()

                if stats['connected']:
                    stats['timestamp'] = time.time()
                    self.snapshots.append(stats)
                    self.queue_rtts.append(stats['queue_rtt'])
                    self.file_rtts.append(file_rtt)

                    if stats['queue_rtt'] < 0:
                        self.rtt_failures += 1

                    self._print_dashboard(stats, file_rtt)
                else:
                    self.rtt_failures += 1
                    print(f"\n  🔴 Cannot connect to {self.queue_server}\n")

                time.sleep(interval)

        except KeyboardInterrupt:
            self._print_final_report()


def main():
    queue_server = os.environ.get('QUEUE_SERVER', 'localhost:50051')
    file_server = os.environ.get('FILE_SERVER', None)
    interval = float(os.environ.get('MONITOR_INTERVAL', '1'))

    if file_server in ('', 'None', 'none'):
        file_server = None

    monitor = CombinedMonitor(queue_server, file_server)
    monitor.run(interval=interval)


if __name__ == '__main__':
    main()