#!/usr/bin/env python3
"""
Web Dashboard + Live Monitor
Routes:
  /                    → ranked links dashboard (index.html embedded)
  /monitor             → live monitor: LPM + RTT + live crawl feed per machine
  /api/links           → ranked links JSON
  /api/content         → extracted text JSON
  /api/stats           → basic stats JSON
  /api/monitor         → all monitor data JSON (LPM, RTT, machine list)
  /api/live-crawl      → last 15 URLs per crawler from grpc_links.txt
  /api/report-metrics  → POST — remote machines report their own RTT here
"""

import os
import re
import time
import sqlite3
import statistics
import threading
from collections import defaultdict, deque
from datetime import datetime

import grpc
from flask import Flask, jsonify, request, g

try:
    import crawler_pb2
    import crawler_pb2_grpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

app = Flask(__name__)

DB_FILE         = os.environ.get('DB_FILE',         './output/extracted.db')
LINKS_FILE      = os.environ.get('LINKS_FILE',      './output/links.txt')
GRPC_LINKS_FILE = os.environ.get('GRPC_LINKS_FILE', './output/grpc_links.txt')
QUEUE_SERVER    = os.environ.get('QUEUE_SERVER',    'queue-server:50051')
FILE_SERVER     = os.environ.get('FILE_SERVER',     'file-server:50052')

# ── Per-machine metrics store (filled by POST /api/report-metrics) ────────
_machine_metrics = {}
_machine_lock    = threading.Lock()


# ── Background collector: Mac measures its own RTT every second ───────────
class StatsCollector:
    def __init__(self):
        self.snapshots     = deque(maxlen=3600)
        self.q_rtts        = deque(maxlen=3600)
        self.f_rtts        = deque(maxlen=3600)
        self.start_time    = time.time()
        self.start_visited = 0
        self.total_probes  = 0
        self.failures      = 0
        self.lock          = threading.Lock()
        self._qstub        = None
        self._fstub        = None
        self._connect()
        threading.Thread(target=self._loop, daemon=True).start()

    def _connect(self):
        if not GRPC_AVAILABLE:
            return
        try:
            opts = [('grpc.keepalive_time_ms', 10000),
                    ('grpc.keepalive_timeout_ms', 5000)]
            self._qstub = crawler_pb2_grpc.QueueServiceStub(
                grpc.insecure_channel(QUEUE_SERVER, options=opts))
            self._fstub = crawler_pb2_grpc.FileServiceStub(
                grpc.insecure_channel(FILE_SERVER, options=opts))
        except Exception:
            pass

    def _poll(self):
        if not self._qstub:
            return
        try:
            t0 = time.perf_counter()
            r  = self._qstub.GetStats(crawler_pb2.GetStatsRequest(), timeout=3)
            q_rtt = (time.perf_counter() - t0) * 1000
            snap  = {'ts': time.time(), 'visited': r.visited_count,
                     'queue_size': r.queue_size}
            with self.lock:
                if self.total_probes == 0:
                    self.start_visited = r.visited_count
                self.snapshots.append(snap)
                self.q_rtts.append(q_rtt)
                self.total_probes += 1
        except Exception:
            with self.lock:
                self.q_rtts.append(-1)
                self.failures += 1
        if self._fstub:
            try:
                t0 = time.perf_counter()
                self._fstub.StoreLink(
                    crawler_pb2.StoreLinkRequest(
                        url='__rtt_probe__', crawler_id='monitor', timestamp=0
                    ), timeout=3)
                f_rtt = (time.perf_counter() - t0) * 1000
            except Exception:
                f_rtt = -1
            with self.lock:
                self.f_rtts.append(f_rtt)

    def _loop(self):
        while True:
            self._poll()
            time.sleep(1)

    def snapshot(self):
        with self.lock:
            return (list(self.snapshots), list(self.q_rtts),
                    list(self.f_rtts), self.total_probes, self.failures)


collector = StatsCollector()


# ── Helpers ───────────────────────────────────────────────────────────────
def _rtt_stats(rtts, window=None):
    d = list(rtts)
    if window:
        d = d[-window:]
    v = [x for x in d if x >= 0]
    if not v:
        return dict(current=-1, avg=-1, p95=-1, p99=-1, min=-1, max=-1, count=0)
    s = sorted(v)
    n = len(s)
    cur = d[-1] if d else -1
    return dict(
        current=round(cur, 1),
        avg=round(statistics.mean(s), 1),
        p95=round(s[int(n * 0.95)], 1),
        p99=round(s[min(int(n * 0.99), n - 1)], 1),
        min=round(s[0], 1),
        max=round(s[-1], 1),
        count=n,
    )

def _lpm(snaps, window_sec):
    if len(snaps) < 2:
        return 0.0
    cutoff = time.time() - window_sec
    start  = next((s for s in snaps if s['ts'] >= cutoff), None)
    if not start:
        return 0.0
    latest = snaps[-1]
    mins   = (latest['ts'] - start['ts']) / 60
    return round((latest['visited'] - start['visited']) / mins, 1) if mins > 0.01 else 0.0

def _sparkline(values, count=40):
    d = [max(0, x) for x in list(values)[-count:]]
    if not d or max(d) == 0:
        return '·' * min(count, max(len(d), 1))
    mx   = max(d)
    bars = '▁▂▃▄▅▆▇█'
    return ''.join(bars[min(int(v / mx * 7), 7)] for v in d)

def _parse_grpc_links():
    """Parse both link files → (counts_per_crawler, timeline)."""
    counts   = defaultdict(int)
    timeline = []
    seen     = set()
    for fpath in [GRPC_LINKS_FILE, LINKS_FILE]:
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue
                    m = re.match(
                        r'\[([^\]]+)\]\s+\[([^\]]+)\]\s+(https?://\S+)', line)
                    if m:
                        ts_str, cid, url = m.groups()
                        if url in seen:
                            continue
                        seen.add(url)
                        counts[cid] += 1
                        try:
                            ts = datetime.fromisoformat(ts_str).timestamp()
                            timeline.append({'ts': ts, 'crawler': cid, 'url': url})
                        except Exception:
                            pass
        except Exception:
            pass
    return dict(counts), sorted(timeline, key=lambda x: x['ts'])

def _per_crawler_lpm(timeline, window_sec=60):
    cutoff = time.time() - window_sec
    counts = defaultdict(int)
    for e in timeline:
        if e['ts'] >= cutoff:
            counts[e['crawler']] += 1
    return {k: round(v / (window_sec / 60), 1) for k, v in counts.items()}


# ── DB helper ─────────────────────────────────────────────────────────────
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_FILE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS extracted_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL, score REAL, title TEXT,
            text_content TEXT, extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')


# ══════════════════════════════════════════════════════════════════════════
# MAIN DASHBOARD HTML  (served at /)
# ══════════════════════════════════════════════════════════════════════════
MAIN_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>System Design Crawler Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f5f7fa;color:#1e293b}
.topbar{background:white;border-bottom:1px solid #e2e8f0;padding:0 24px;display:flex;align-items:center;height:52px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.topbar h1{font-size:17px;font-weight:700;color:#1e293b;margin-right:28px}
.tab-btn{background:none;border:none;color:#64748b;padding:0 16px;height:52px;font-size:14px;font-weight:500;cursor:pointer;border-bottom:2px solid transparent;transition:all .15s}
.tab-btn:hover{color:#1e293b}
.tab-btn.active{color:#4f46e5;border-bottom-color:#4f46e5}
.topbar-right{margin-left:auto;font-size:12px;color:#94a3b8}
.tab-panel{display:none;padding:24px;max-width:1300px;margin:0 auto}
.tab-panel.active{display:block}
.stats-row{display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap}
.stat-pill{background:white;border:1px solid #e2e8f0;border-radius:8px;padding:10px 18px;font-size:14px;color:#64748b;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.stat-pill span{color:#1e293b;font-weight:700}
.table-wrap{background:white;border-radius:12px;box-shadow:0 2px 6px rgba(0,0,0,.06);overflow:hidden}
table{width:100%;border-collapse:collapse}
th{text-align:left;padding:12px 18px;background:#f8fafc;font-weight:600;color:#475569;font-size:12px;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #e2e8f0}
td{padding:13px 18px;border-bottom:1px solid #f1f5f9;font-size:14px;vertical-align:middle}
tr:hover td{background:#fafbff}
.score{display:inline-block;background:#eef2ff;color:#4338ca;padding:2px 8px;border-radius:6px;font-weight:700;font-size:12px}
.url-link{color:#2563eb;text-decoration:none;word-break:break-all}
.url-link:hover{text-decoration:underline}
.view-btn{background:#eef2ff;border:none;color:#4f46e5;padding:6px 14px;border-radius:20px;font-size:12px;font-weight:500;cursor:pointer;transition:all .15s}
.view-btn:hover{background:#4f46e5;color:white}
.footer{margin-top:16px;text-align:center;color:#94a3b8;font-size:13px}
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:1000;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal-box{background:white;border-radius:16px;width:82%;max-width:860px;max-height:82vh;display:flex;flex-direction:column;box-shadow:0 20px 50px rgba(0,0,0,.2)}
.modal-header{padding:16px 20px;border-bottom:1px solid #e2e8f0;display:flex;justify-content:space-between;align-items:center}
.modal-header h3{font-size:15px;font-weight:600;color:#1e293b}
.modal-close{background:none;border:none;color:#94a3b8;font-size:22px;cursor:pointer;line-height:1}
.modal-close:hover{color:#1e293b}
.modal-body{padding:20px;overflow-y:auto;flex:1}
.modal-text{font-family:'Courier New',monospace;font-size:13px;line-height:1.7;color:#334155;white-space:pre-wrap;background:#f8fafc;padding:16px;border-radius:8px}
.loading{text-align:center;padding:40px;color:#94a3b8}
</style></head><body>
<div class="topbar">
  <h1>🏆 System Design Crawler</h1>
  <button class="tab-btn active" onclick="switchTab('rankings',this)">📚 Rankings</button>
  <button class="tab-btn" onclick="switchTab('monitor',this)">📊 Live Monitor</button>
  <div class="topbar-right" id="last-update">—</div>
</div>

<div id="tab-rankings" class="tab-panel active">
  <div class="stats-row">
    <div class="stat-pill">📚 Total Links: <span id="r-total">—</span></div>
    <div class="stat-pill">📊 Avg Score: <span id="r-avg">—</span></div>
    <div class="stat-pill" style="color:#94a3b8">Auto-refreshes every 3 hours</div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Rank</th><th>Score</th><th>Title / URL</th><th>Extracted</th><th>Action</th></tr></thead>
      <tbody id="r-body"><tr><td colspan="5" class="loading">Loading rankings…</td></tr></tbody>
    </table>
  </div>
  <div class="footer">Last update: <span id="r-updated">—</span></div>
</div>

<div id="tab-monitor" class="tab-panel">
  <!-- Monitor content is loaded via iframe pointing to /monitor -->
  <iframe src="/monitor" style="width:100%;height:calc(100vh - 80px);border:none;border-radius:12px" id="monitor-frame"></iframe>
</div>

<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal-box">
    <div class="modal-header">
      <h3 id="modal-title">Extracted Content</h3>
      <button class="modal-close" onclick="closeModal()">×</button>
    </div>
    <div class="modal-body"><div class="modal-text" id="modal-body">Loading…</div></div>
  </div>
</div>

<script>
function esc(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML}
function switchTab(name,btn){
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  btn.classList.add('active');
}
async function loadRankings(){
  try{
    const[links,stats]=await Promise.all([
      fetch('/api/links').then(r=>r.json()),
      fetch('/api/stats').then(r=>r.json())
    ]);
    document.getElementById('r-total').textContent=stats.total_links;
    document.getElementById('r-avg').textContent=stats.avg_score;
    const tbody=document.getElementById('r-body');
    if(!links.length){tbody.innerHTML='<tr><td colspan="5" class="loading">No links extracted yet.</td></tr>';return}
    tbody.innerHTML=links.map((l,i)=>{
      const t=l.title||l.url;
      const d=l.extracted_at?new Date(l.extracted_at).toLocaleDateString():'pending';
      return`<tr>
        <td style="color:#94a3b8;font-weight:600;font-size:13px">#${i+1}</td>
        <td><span class="score">${l.score.toFixed(2)}</span></td>
        <td style="max-width:480px"><a class="url-link" href="${l.url}" target="_blank">${esc(t)}</a></td>
        <td style="color:#94a3b8;font-size:12px">${d}</td>
        <td><button class="view-btn" onclick="viewText('${encodeURIComponent(l.url)}','${esc(t).replace(/'/g,"\\'")}')">📄 View</button></td>
      </tr>`;
    }).join('');
    const now=new Date().toLocaleString();
    document.getElementById('r-updated').textContent=now;
    document.getElementById('last-update').textContent='Updated '+new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}
async function viewText(u,title){
  document.getElementById('modal-title').textContent=decodeURIComponent(title);
  document.getElementById('modal-body').textContent='Loading…';
  document.getElementById('modal').classList.add('open');
  try{const d=await(await fetch('/api/content?url='+u)).json();
  document.getElementById('modal-body').textContent=d.text||'(No content)';}
  catch{document.getElementById('modal-body').textContent='Error loading content.';}
}
function closeModal(){document.getElementById('modal').classList.remove('open')}
loadRankings();
setInterval(loadRankings,3*60*60*1000);
</script></body></html>"""


# ══════════════════════════════════════════════════════════════════════════
# MONITOR HTML  (served at /monitor)
# Shows: LPM, RTT, per-machine table, load bars, live crawl feed
# ══════════════════════════════════════════════════════════════════════════
MONITOR_HTML = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live Monitor</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px}
.full{grid-column:1/-1}
.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:18px}
.ctitle{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#64748b;margin-bottom:12px}
.bignum{font-size:38px;font-weight:800;color:#f1f5f9;line-height:1}
.bigsub{font-size:12px;color:#475569;margin-top:3px}
.mrow{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #0f172a;font-size:13px}
.mrow:last-child{border:none}
.ml{color:#64748b}.mv{font-weight:600;color:#f1f5f9}
.spark{font-family:monospace;font-size:16px;color:#818cf8;letter-spacing:2px;margin-top:10px;word-break:break-all}

/* Machine table */
.mt{width:100%;border-collapse:collapse;font-size:13px}
.mt th{text-align:left;padding:9px 12px;color:#475569;font-weight:600;border-bottom:1px solid #334155;font-size:11px;text-transform:uppercase;letter-spacing:.04em;background:#162032}
.mt td{padding:10px 12px;border-bottom:1px solid #0f172a;vertical-align:middle}
.mt tr:last-child td{border:none}
.mname{font-weight:700;color:#f1f5f9}
.ltag{font-size:10px;color:#475569;background:#0f172a;padding:1px 6px;border-radius:4px;margin-left:5px}

/* Badges */
.badge{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:700}
.bg{background:#14532d;color:#4ade80}
.by{background:#713f12;color:#fbbf24}
.br{background:#7f1d1d;color:#f87171}
.bk{background:#1f2937;color:#9ca3af}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px;vertical-align:middle}
.dg{background:#22c55e}.dy{background:#f59e0b}.dr{background:#ef4444}.dk{background:#6b7280}

/* Load bars */
.brow{display:flex;align-items:center;gap:10px;margin-bottom:9px;font-size:13px}
.blabel{width:160px;color:#94a3b8;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bbg{flex:1;background:#0f172a;border-radius:4px;height:14px;overflow:hidden}
.bfill{height:100%;border-radius:4px;transition:width .6s}
.bcount{width:90px;text-align:right;color:#f1f5f9;font-weight:600;font-size:12px}

/* Live feed */
.feed-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.feed-title{font-size:13px;font-weight:700;color:#f1f5f9;margin-bottom:10px;display:flex;align-items:center;gap:8px}
.feed-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0}
.feed-count{font-size:11px;background:#0f172a;color:#64748b;padding:2px 8px;border-radius:4px}
.feed-item{padding:5px 0;border-bottom:1px solid #0f172a;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.feed-item:last-child{border:none}
.feed-item a{color:#818cf8;text-decoration:none}
.feed-item a:hover{text-decoration:underline}
.ftime{color:#334155;font-size:10px;margin-right:4px}
.fempty{color:#334155;font-size:12px;font-style:italic;padding:8px 0}

.ts{font-size:11px;color:#334155;text-align:right;margin-top:12px}

/* RTT colours */
.rg{color:#4ade80;font-weight:600}.ry{color:#fbbf24;font-weight:600}.rr{color:#f87171;font-weight:600}.rk{color:#6b7280}
</style></head><body>

<!-- Row 1: LPM + Queue + Mac RTT -->
<div class="grid3">
  <div class="card">
    <div class="ctitle">⚡ Links Per Minute (all machines)</div>
    <div class="bignum" id="lpm1">—</div><div class="bigsub">1-minute rate</div>
    <div style="margin-top:13px">
      <div class="mrow"><span class="ml">5-min LPM</span><span class="mv" id="lpm5">—</span></div>
      <div class="mrow"><span class="ml">10-min LPM</span><span class="mv" id="lpm10">—</span></div>
      <div class="mrow"><span class="ml">Overall LPM</span><span class="mv" id="lpma">—</span></div>
      <div class="mrow"><span class="ml">Peak LPM</span><span class="mv" id="lpmpeak">—</span></div>
    </div>
    <div class="spark" id="lspark">···</div>
  </div>

  <div class="card">
    <div class="ctitle">📦 Queue Status</div>
    <div class="bignum" id="qsize">—</div><div class="bigsub">URLs remaining in queue</div>
    <div style="margin-top:13px">
      <div class="mrow"><span class="ml">Total Visited</span><span class="mv" id="visited">—</span></div>
      <div class="mrow"><span class="ml">In DB (extracted)</span><span class="mv" id="indb">—</span></div>
      <div class="mrow"><span class="ml">Uptime</span><span class="mv" id="uptime">—</span></div>
    </div>
  </div>

  <div class="card">
    <div class="ctitle">🏓 Queue + File RTT (Mac local)</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">
      <div style="background:#0f172a;border-radius:8px;padding:12px">
        <div style="font-size:11px;color:#475569;margin-bottom:4px">Queue RTT</div>
        <div class="bignum" style="font-size:26px" id="qrtt">—</div>
        <div id="qbadge" style="margin-top:4px"></div>
      </div>
      <div style="background:#0f172a;border-radius:8px;padding:12px">
        <div style="font-size:11px;color:#475569;margin-bottom:4px">File RTT</div>
        <div class="bignum" style="font-size:26px" id="frtt">—</div>
        <div id="fbadge" style="margin-top:4px"></div>
      </div>
    </div>
    <div class="mrow"><span class="ml">Q avg 1m</span><span class="mv" id="qavg">—</span></div>
    <div class="mrow"><span class="ml">Q P95</span><span class="mv" id="qp95">—</span></div>
    <div class="mrow"><span class="ml">Q P99</span><span class="mv" id="qp99">—</span></div>
    <div class="mrow"><span class="ml">Availability</span><span class="mv" id="avail">—</span></div>
    <div class="spark" id="rspark">···</div>
  </div>
</div>

<!-- Row 2: Per-machine table (full width) -->
<div class="card full" style="margin-bottom:14px">
  <div class="ctitle">🖥️ Per-Machine RTT &amp; LPM</div>
  <table class="mt">
    <thead><tr>
      <th>Machine</th><th>Status</th>
      <th>Queue RTT</th><th>File RTT</th>
      <th>Q avg (1m)</th><th>Q P95</th>
      <th>LPM (1m)</th><th>Total URLs</th><th>Last Seen</th>
    </tr></thead>
    <tbody id="mtable"><tr><td colspan="9" style="color:#475569;font-style:italic;padding:16px">Waiting for machines…</td></tr></tbody>
  </table>
</div>

<!-- Row 3: Load bars + per-machine LPM bars -->
<div class="grid2" style="margin-bottom:14px">
  <div class="card">
    <div class="ctitle">🤝 Load Distribution (total URLs)</div>
    <div id="load-bars"><div style="color:#334155;font-style:italic;font-size:13px">No data yet</div></div>
  </div>
  <div class="card">
    <div class="ctitle">📈 LPM per Machine (last 1 min)</div>
    <div id="lpm-bars"><div style="color:#334155;font-style:italic;font-size:13px">No data yet</div></div>
  </div>
</div>

<!-- Row 4: Live crawl feed (one column per machine) -->
<div class="card full">
  <div class="ctitle">🔴 Live Crawl Feed — What Each Machine Is Crawling Right Now</div>
  <div class="feed-grid" id="feed-grid" style="margin-top:10px">
    <div style="color:#334155;font-style:italic;font-size:13px">Loading feed…</div>
  </div>
</div>

<div class="ts">Updated: <span id="ts">—</span> · Refreshes every 3s</div>

<script>
const COLORS=['#818cf8','#34d399','#fb923c','#f472b6','#60a5fa'];
let peakLpm=0;

function fmt(ms){
  if(ms==null||ms<0)return'<span class="rk">—</span>';
  const c=ms<20?'rg':ms<=80?'rg':ms<=200?'ry':'rr';
  return`<span class="${c}">${ms.toFixed(1)}ms</span>`;
}
function badge(ms){
  if(ms==null||ms<0)return'<span class="badge bk">OFFLINE</span>';
  if(ms<20)return'<span class="badge bg">EXCELLENT</span>';
  if(ms<=80)return'<span class="badge bg">GOOD</span>';
  if(ms<=200)return'<span class="badge by">WARNING</span>';
  return'<span class="badge br">CRITICAL</span>';
}
function dot(ms){
  if(ms==null||ms<0)return'<span class="dot dk"></span>';
  if(ms<=80)return'<span class="dot dg"></span>';
  if(ms<=200)return'<span class="dot dy"></span>';
  return'<span class="dot dr"></span>';
}
function fmtUptime(s){
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=Math.floor(s%60);
  return`${h}h ${m}m ${sc}s`;
}
function fmtAgo(ts){
  if(!ts)return'never';
  const a=Math.round(Date.now()/1000-ts);
  return a<60?`${a}s ago`:`${Math.floor(a/60)}m ago`;
}
function bars(obj,suffix,maxVal){
  const entries=Object.entries(obj).sort((a,b)=>b[1]-a[1]);
  const total=entries.reduce((s,[,v])=>s+v,0)||1;
  const mx=maxVal!=null?maxVal:Math.max(...entries.map(([,v])=>v),1);
  return entries.map(([id,val],i)=>{
    const pct=Math.round(val/mx*100);
    const label=suffix?`${val.toFixed(1)} ${suffix}`:`${val.toLocaleString()} (${Math.round(val/total*100)}%)`;
    return`<div class="brow">
      <div class="blabel" title="${id}">${id}</div>
      <div class="bbg"><div class="bfill" style="width:${pct}%;background:${COLORS[i%COLORS.length]}"></div></div>
      <div class="bcount">${label}</div>
    </div>`;
  }).join('')||'<div style="color:#334155;font-style:italic;font-size:13px">No data</div>';
}

async function refresh(){
  try{
    const[mon,feed]=await Promise.all([
      fetch('/api/monitor').then(r=>r.json()),
      fetch('/api/live-crawl').then(r=>r.json())
    ]);

    /* LPM */
    if(mon.lpm['1m']>peakLpm)peakLpm=mon.lpm['1m'];
    document.getElementById('lpm1').textContent=mon.lpm['1m'].toFixed(1);
    document.getElementById('lpm5').textContent=mon.lpm['5m'].toFixed(1);
    document.getElementById('lpm10').textContent=mon.lpm['10m'].toFixed(1);
    document.getElementById('lpma').textContent=mon.lpm.overall.toFixed(1);
    document.getElementById('lpmpeak').textContent=peakLpm.toFixed(1);
    document.getElementById('lspark').textContent=mon.lpm_spark||'···';

    /* Queue */
    document.getElementById('qsize').textContent=mon.queue_size.toLocaleString();
    document.getElementById('visited').textContent=mon.visited.toLocaleString();
    document.getElementById('indb').textContent=mon.in_db.toLocaleString();
    document.getElementById('uptime').textContent=fmtUptime(mon.uptime_sec);

    /* Mac RTT */
    const qr=mon.rtt.queue,fr=mon.rtt.file;
    document.getElementById('qrtt').innerHTML=fmt(qr.current);
    document.getElementById('frtt').innerHTML=fmt(fr.current);
    document.getElementById('qbadge').innerHTML=badge(qr.current);
    document.getElementById('fbadge').innerHTML=badge(fr.current);
    document.getElementById('qavg').innerHTML=fmt(qr.avg);
    document.getElementById('qp95').innerHTML=fmt(qr.p95);
    document.getElementById('qp99').innerHTML=fmt(qr.p99);
    document.getElementById('avail').textContent=mon.availability.toFixed(1)+'%';
    document.getElementById('rspark').textContent=mon.q_rtt_spark||'···';

    /* Per-machine table */
    const now=Date.now()/1000;
    document.getElementById('mtable').innerHTML=mon.machines.length
      ?mon.machines.map((m,i)=>{
          const qc=m.queue_rtt?.current,fc=m.file_rtt?.current;
          const ago=m.last_seen?Math.round(now-m.last_seen):null;
          const online=ago!=null&&ago<30;
          const lpm=(mon.per_crawler_lpm||{})[m.crawler_id]??0;
          const total=(mon.crawler_counts||{})[m.crawler_id]??0;
          return`<tr>
            <td><span class="mname">${m.crawler_id}</span>${m.is_local?'<span class="ltag">local</span>':''}</td>
            <td>${dot(online?qc:-1)}${online?badge(qc):'<span class="badge bk">OFFLINE</span>'}</td>
            <td>${fmt(qc)}</td><td>${fmt(fc)}</td>
            <td>${fmt(m.queue_rtt?.avg)}</td><td>${fmt(m.queue_rtt?.p95)}</td>
            <td style="color:#34d399;font-weight:600">${lpm.toFixed(1)}</td>
            <td style="font-weight:600">${total.toLocaleString()}</td>
            <td style="color:#475569;font-size:12px">${fmtAgo(m.last_seen)}</td>
          </tr>`;
        }).join('')
      :'<tr><td colspan="9" style="color:#475569;font-style:italic;padding:16px">No machines connected yet</td></tr>';

    /* Load bars */
    document.getElementById('load-bars').innerHTML=bars(mon.crawler_counts||{},null,null);
    document.getElementById('lpm-bars').innerHTML=bars(mon.per_crawler_lpm||{},'LPM',null);

    /* Live crawl feed */
    const crawlers=Object.keys(feed);
    if(!crawlers.length){
      document.getElementById('feed-grid').innerHTML='<div style="color:#334155;font-style:italic;font-size:13px">No crawl data yet — waiting for crawlers…</div>';
    } else {
      document.getElementById('feed-grid').innerHTML=crawlers.map((cid,i)=>{
        const urls=feed[cid]||[];
        const color=COLORS[i%COLORS.length];
        const items=urls.length
          ?urls.map(u=>`<div class="feed-item">
              <span class="ftime">${u.time}</span>
              <a href="${u.url}" target="_blank" title="${u.url}">${u.url}</a>
            </div>`).join('')
          :'<div class="fempty">No URLs crawled yet</div>';
        return`<div class="card" style="padding:14px">
          <div class="feed-title">
            <span class="feed-dot" style="background:${color}"></span>
            ${cid}
            <span class="feed-count">${urls.length} recent</span>
          </div>
          ${items}
        </div>`;
      }).join('');
    }

    document.getElementById('ts').textContent=new Date().toLocaleTimeString();
  }catch(e){console.error('Monitor error:',e)}
}

refresh();
setInterval(refresh,3000);
</script></body></html>"""


# ══════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    return MAIN_HTML, 200, {'Content-Type': 'text/html; charset=utf-8'}

@app.route('/monitor')
def monitor():
    return MONITOR_HTML, 200, {'Content-Type': 'text/html; charset=utf-8'}


@app.route('/api/report-metrics', methods=['POST'])
def api_report_metrics():
    """Remote machines POST their RTT here every 5 seconds."""
    data = request.get_json(silent=True)
    if not data or 'crawler_id' not in data:
        return jsonify({'error': 'Missing crawler_id'}), 400
    cid = data['crawler_id']
    with _machine_lock:
        if cid not in _machine_metrics:
            _machine_metrics[cid] = {
                'crawler_id': cid, 'is_local': False,
                'last_seen': None, 'queue_rtt': {}, 'file_rtt': {}, 'uptime_sec': 0,
            }
        _machine_metrics[cid].update({
            'last_seen':  data.get('timestamp', time.time()),
            'queue_rtt':  data.get('queue_rtt', {}),
            'file_rtt':   data.get('file_rtt', {}),
            'uptime_sec': data.get('uptime_sec', 0),
        })
    return jsonify({'ok': True})


@app.route('/api/live-crawl')
def api_live_crawl():
    """
    Returns the last 15 URLs crawled by each machine.
    Reads grpc_links.txt (and links.txt for Mac).
    Format: { crawler_id: [ {url, time}, ... ] }
    """
    per_machine = defaultdict(list)
    seen        = set()

    for fpath in [GRPC_LINKS_FILE, LINKS_FILE]:
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue
                    m = re.match(
                        r'\[([^\]]+)\]\s+\[([^\]]+)\]\s+(https?://\S+)', line)
                    if m:
                        ts_str, cid, url = m.groups()
                        key = f"{cid}:{url}"
                        if key in seen:
                            continue
                        seen.add(key)
                        # Format time as HH:MM:SS
                        try:
                            dt  = datetime.fromisoformat(ts_str)
                            t   = dt.strftime('%H:%M:%S')
                        except Exception:
                            t = ts_str[:8]
                        per_machine[cid].append({'url': url, 'time': t})
        except Exception:
            pass

    # Return only last 15 per crawler, most recent first
    result = {
        cid: list(reversed(urls[-15:]))
        for cid, urls in per_machine.items()
    }
    return jsonify(result)


@app.route('/api/monitor')
def api_monitor():
    snaps, q_rtts, f_rtts, probes, fails = collector.snapshot()

    lpm = {
        '1m':     _lpm(snaps, 60),
        '5m':     _lpm(snaps, 300),
        '10m':    _lpm(snaps, 600),
        'overall': 0.0,
    }
    if snaps:
        elapsed_min = (time.time() - collector.start_time) / 60
        lpm['overall'] = round(
            (snaps[-1]['visited'] - collector.start_visited) / elapsed_min, 1
        ) if elapsed_min > 0 else 0.0

    latest = snaps[-1] if snaps else {}

    try:
        in_db = get_db().execute(
            'SELECT COUNT(*) as c FROM extracted_content').fetchone()['c']
    except Exception:
        in_db = 0

    crawler_counts, timeline = _parse_grpc_links()
    per_crawler_lpm = _per_crawler_lpm(timeline, 60)

    q_stats = _rtt_stats(q_rtts, 60)
    f_stats = _rtt_stats(f_rtts, 60)
    avail   = ((probes - fails) / max(probes, 1)) * 100

    mac_entry = {
        'crawler_id': 'crawler-mac', 'is_local': True,
        'last_seen': time.time(),
        'queue_rtt': {
            'current': q_stats['current'], 'avg': q_stats['avg'],
            'p95': q_stats['p95'], 'p99': q_stats['p99'],
            'min': q_stats['min'], 'max': q_stats['max'],
        },
        'file_rtt': {
            'current': f_stats['current'], 'avg': f_stats['avg'],
            'p95': f_stats['p95'], 'min': f_stats['min'], 'max': f_stats['max'],
        },
        'uptime_sec': time.time() - collector.start_time,
    }

    with _machine_lock:
        remote = list(_machine_metrics.values())

    all_machines = [mac_entry] + [r for r in remote if r['crawler_id'] != 'crawler-mac']

    return jsonify({
        'lpm':             lpm,
        'lpm_spark':       _sparkline([s['visited'] for s in snaps], 40),
        'queue_size':      latest.get('queue_size', 0),
        'visited':         latest.get('visited', 0),
        'in_db':           in_db,
        'uptime_sec':      time.time() - collector.start_time,
        'rtt': {
            'queue': mac_entry['queue_rtt'],
            'file':  mac_entry['file_rtt'],
        },
        'q_rtt_spark':     _sparkline(q_rtts, 40),
        'f_rtt_spark':     _sparkline(f_rtts, 40),
        'availability':    round(avail, 1),
        'machines':        all_machines,
        'crawler_counts':  crawler_counts,
        'per_crawler_lpm': per_crawler_lpm,
    })


@app.route('/api/links')
def api_links():
    cur = get_db().execute(
        'SELECT url,score,title,extracted_at FROM extracted_content ORDER BY score DESC')
    return jsonify([{
        'url': r['url'], 'score': round(r['score'] or 0, 2),
        'title': r['title'] or r['url'], 'extracted_at': r['extracted_at'],
    } for r in cur.fetchall()])


@app.route('/api/content')
def api_content():
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Missing url'}), 400
    row = get_db().execute(
        'SELECT text_content FROM extracted_content WHERE url=?', (url,)).fetchone()
    return jsonify({'text': row['text_content']}) if row else \
           (jsonify({'text': 'Not yet extracted.'}), 404)


@app.route('/api/stats')
def api_stats():
    row = get_db().execute(
        'SELECT COUNT(*) as total, AVG(score) as avg_score FROM extracted_content'
    ).fetchone()
    return jsonify({
        'total_links': row['total'],
        'avg_score':   round(row['avg_score'] or 0, 2),
    })


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=False)