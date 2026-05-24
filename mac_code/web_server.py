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

# ── Chart history: stores data points every 5s for real-time graphs ──────
_chart_history = deque(maxlen=120)
_chart_lock    = threading.Lock()

def _save_chart_point():
    snaps, q_rtts, f_rtts, _, _ = collector.snapshot()
    _, timeline = _parse_grpc_links()
    per_lpm = _per_crawler_lpm(timeline, 60)
    machine_rtts = {'crawler-mac': round(q_rtts[-1], 1) if q_rtts else -1}
    with _machine_lock:
        for cid, m in _machine_metrics.items():
            machine_rtts[cid] = m.get('queue_rtt', {}).get('current', -1)
    total_lpm = _lpm(snaps, 60)
    point = {
        'ts':          round(time.time(), 1),
        'label':       datetime.now().strftime('%H:%M:%S'),
        'total_lpm':   total_lpm,
        'per_lpm':     dict(per_lpm),
        'mac_q_rtt':   machine_rtts.get('crawler-mac', -1),
        'mac_f_rtt':   round(f_rtts[-1], 1) if f_rtts else -1,
        'machine_rtts': machine_rtts,
    }
    with _chart_lock:
        _chart_history.append(point)

def _chart_history_loop():
    while True:
        try:
            _save_chart_point()
        except Exception:
            pass
        time.sleep(5)

threading.Thread(target=_chart_history_loop, daemon=True).start()


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
MAIN_HTML = r"""<!DOCTYPE html>
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
  <h1>&#127942; System Design Crawler</h1>
  <button class="tab-btn active" onclick="switchTab('rankings',this)">&#128218; Rankings</button>
  <button class="tab-btn" onclick="switchTab('monitor',this)">&#128202; Live Monitor</button>
  <div class="topbar-right" id="last-update">&#8212;</div>
</div>

<div id="tab-rankings" class="tab-panel active">
  <div class="stats-row">
    <div class="stat-pill">Total Links: <span id="r-total">&#8212;</span></div>
    <div class="stat-pill">Avg Score: <span id="r-avg">&#8212;</span></div>
    <div class="stat-pill" style="color:#94a3b8">Auto-refreshes every 3 hours</div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Rank</th><th>Score</th><th>Title / URL</th><th>Extracted</th><th>Action</th></tr></thead>
      <tbody id="r-body"><tr><td colspan="5" class="loading">Loading rankings&#8230;</td></tr></tbody>
    </table>
  </div>
  <div class="footer">Last update: <span id="r-updated">&#8212;</span></div>
</div>

<div id="tab-monitor" class="tab-panel">
  <iframe src="/monitor" style="width:100%;height:calc(100vh - 80px);border:none;border-radius:12px" id="monitor-frame"></iframe>
</div>

<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal-box">
    <div class="modal-header">
      <h3 id="modal-title">Extracted Content</h3>
      <button class="modal-close" onclick="closeModal()">&#215;</button>
    </div>
    <div class="modal-body"><div class="modal-text" id="modal-body">Loading&#8230;</div></div>
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
        <td><button class="view-btn" onclick="viewText('${encodeURIComponent(l.url)}','${esc(t).replace(/'/g,"\\'")}')">View</button></td>
      </tr>`;
    }).join('');
    document.getElementById('r-updated').textContent=new Date().toLocaleString();
    document.getElementById('last-update').textContent='Updated '+new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}
async function viewText(u,title){
  document.getElementById('modal-title').textContent=decodeURIComponent(title);
  document.getElementById('modal-body').textContent='Loading\u2026';
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
# Per-machine LPM + RTT live graphs, aggregated stats, table, feed
# ══════════════════════════════════════════════════════════════════════════
MONITOR_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@700;800&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'JetBrains Mono',monospace;background:#f2f6fc;color:#c9d4e8;padding:16px 18px;min-height:100vh}

.section-label{
  font-family:'Syne',sans-serif;font-size:10px;font-weight:700;letter-spacing:.2em;
  text-transform:uppercase;color:#2d4068;margin:0 0 10px 1px;
  display:flex;align-items:center;gap:10px
}
.section-label::after{content:'';flex:1;height:1px;background:#0f1e38}

/* ── per-machine cards ── */
.machine-grid{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(400px,1fr));
  gap:14px;margin-bottom:20px
}
.mcard{
  background:#0b1526;border:1px solid #162035;border-radius:14px;
  overflow:hidden;transition:border-color .25s,box-shadow .25s
}
.mcard:hover{border-color:#2a4070;box-shadow:0 0 20px rgba(99,130,200,.07)}
.mcard-head{
  display:flex;align-items:center;gap:10px;
  padding:12px 16px 11px;border-bottom:1px solid #0f1e38
}
.mcard-icon{font-size:17px;line-height:1}
.mcard-name{
  font-family:'Syne',sans-serif;font-size:14px;font-weight:800;
  color:#dce8ff;letter-spacing:.02em
}
.mcard-id{font-size:9px;color:#2d4068;margin-top:1px}
.mcard-status{
  margin-left:auto;font-size:9px;font-weight:700;letter-spacing:.07em;
  padding:3px 10px;border-radius:20px;text-transform:uppercase
}
.st-online{background:#091e10;color:#22c55e;border:1px solid #14532d}
.st-warn{background:#1c1006;color:#f59e0b;border:1px solid #78350f}
.st-offline{background:#12090a;color:#4b5563;border:1px solid #1f2937}

/* KPI row */
.mcard-kpis{
  display:grid;grid-template-columns:repeat(4,1fr);
  gap:1px;background:#0f1e38;border-bottom:1px solid #0f1e38
}
.kpi{background:#0b1526;padding:9px 12px}
.kpi-label{font-size:8px;color:#2d4068;text-transform:uppercase;letter-spacing:.12em;margin-bottom:3px}
.kpi-val{font-size:16px;font-weight:700;color:#dce8ff;line-height:1.1}
.kpi-val.good{color:#22c55e}
.kpi-val.warn{color:#f59e0b}
.kpi-val.bad{color:#f87171}
.kpi-val.dim{color:#243354}

/* chart area */
.mcard-charts{display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:12px 14px 14px}
.chart-label{font-size:8px;color:#2d4068;letter-spacing:.13em;text-transform:uppercase;margin-bottom:5px}
.chart-wrap{height:88px;position:relative}

/* ── existing stat cards ── */
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:14px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}
.full{grid-column:1/-1}
.card{background:#0b1526;border:1px solid #162035;border-radius:12px;padding:16px}
.ctitle{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.12em;color:#2d4068;margin-bottom:12px}
.bignum{font-size:34px;font-weight:800;color:#dce8ff;line-height:1;font-family:'Syne',sans-serif}
.bigsub{font-size:10px;color:#2d4068;margin-top:3px}
.mrow{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #0a1220;font-size:11px}
.mrow:last-child{border:none}
.ml{color:#2d4068}.mv{font-weight:700;color:#dce8ff}
.spark{font-family:monospace;font-size:14px;color:#3b5ea6;letter-spacing:2px;margin-top:10px;word-break:break-all}
.mt{width:100%;border-collapse:collapse;font-size:11px}
.mt th{text-align:left;padding:8px 12px;color:#2d4068;font-weight:700;border-bottom:1px solid #0f1e38;font-size:8px;text-transform:uppercase;letter-spacing:.1em;background:#070d1a}
.mt td{padding:8px 12px;border-bottom:1px solid #0a1220;vertical-align:middle}
.mt tr:last-child td{border:none}
.mname{font-weight:700;color:#dce8ff}
.ltag{font-size:8px;color:#2d4068;background:#070d1a;padding:1px 5px;border-radius:3px;margin-left:5px}
.badge{display:inline-block;padding:2px 8px;border-radius:20px;font-size:9px;font-weight:700}
.bg{background:#14532d;color:#4ade80}.by{background:#713f12;color:#fbbf24}
.br{background:#7f1d1d;color:#f87171}.bk{background:#1f2937;color:#6b7280}
.dot{display:inline-block;width:6px;height:6px;border-radius:50%;margin-right:5px;vertical-align:middle}
.dg{background:#22c55e}.dy{background:#f59e0b}.dr{background:#ef4444}.dk{background:#6b7280}
.brow{display:flex;align-items:center;gap:10px;margin-bottom:7px;font-size:11px}
.blabel{width:140px;color:#4b5563;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bbg{flex:1;background:#070d1a;border-radius:3px;height:11px;overflow:hidden}
.bfill{height:100%;border-radius:3px;transition:width .6s}
.bcount{width:90px;text-align:right;color:#dce8ff;font-weight:700;font-size:10px}
.feed-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.feed-title{font-size:11px;font-weight:700;color:#dce8ff;margin-bottom:8px;display:flex;align-items:center;gap:6px}
.feed-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.feed-count{font-size:9px;background:#070d1a;color:#2d4068;padding:1px 7px;border-radius:3px}
.feed-item{padding:4px 0;border-bottom:1px solid #0a1220;font-size:10px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.feed-item:last-child{border:none}
.feed-item a{color:#5b78c4;text-decoration:none}
.feed-item a:hover{text-decoration:underline}
.ftime{color:#162035;font-size:9px;margin-right:3px}
.fempty{color:#162035;font-size:11px;font-style:italic;padding:6px 0}
.ts{font-size:9px;color:#162035;text-align:right;margin-top:10px}
.rg{color:#4ade80;font-weight:700}.ry{color:#fbbf24;font-weight:700}
.rr{color:#f87171;font-weight:700}.rk{color:#2d4068}
</style></head><body>

<!-- ═══ PER-MACHINE GRAPHS ═══ -->
<div class="section-label">Per-Machine &#8212; LPM &amp; RTT Live Graphs</div>
<div class="machine-grid" id="machine-graph-grid">
  <div style="color:#162035;font-style:italic;font-size:12px;padding:16px;grid-column:1/-1">
    Waiting for machines to connect&#8230;
  </div>
</div>

<!-- ═══ AGGREGATED STATS ═══ -->
<div class="section-label">Cluster Aggregates</div>
<div class="grid3">
  <div class="card">
    <div class="ctitle">Links Per Minute &#8212; all machines</div>
    <div class="bignum" id="lpm1">&#8212;</div><div class="bigsub">1-minute rate</div>
    <div style="margin-top:12px">
      <div class="mrow"><span class="ml">5-min LPM</span><span class="mv" id="lpm5">&#8212;</span></div>
      <div class="mrow"><span class="ml">10-min LPM</span><span class="mv" id="lpm10">&#8212;</span></div>
      <div class="mrow"><span class="ml">Overall LPM</span><span class="mv" id="lpma">&#8212;</span></div>
      <div class="mrow"><span class="ml">Peak LPM</span><span class="mv" id="lpmpeak">&#8212;</span></div>
    </div>
    <div class="spark" id="lspark">&#183;&#183;&#183;</div>
  </div>
  <div class="card">
    <div class="ctitle">Queue Status</div>
    <div class="bignum" id="qsize">&#8212;</div><div class="bigsub">URLs remaining in queue</div>
    <div style="margin-top:12px">
      <div class="mrow"><span class="ml">Total Visited</span><span class="mv" id="visited">&#8212;</span></div>
      <div class="mrow"><span class="ml">In DB (extracted)</span><span class="mv" id="indb">&#8212;</span></div>
      <div class="mrow"><span class="ml">Uptime</span><span class="mv" id="uptime">&#8212;</span></div>
    </div>
  </div>
  <div class="card">
    <div class="ctitle">Queue + File RTT &#8212; Mac local</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px">
      <div style="background:#070d1a;border-radius:7px;padding:10px">
        <div style="font-size:8px;color:#2d4068;margin-bottom:3px;text-transform:uppercase;letter-spacing:.1em">Queue RTT</div>
        <div class="bignum" style="font-size:22px" id="qrtt">&#8212;</div>
        <div id="qbadge" style="margin-top:4px"></div>
      </div>
      <div style="background:#070d1a;border-radius:7px;padding:10px">
        <div style="font-size:8px;color:#2d4068;margin-bottom:3px;text-transform:uppercase;letter-spacing:.1em">File RTT</div>
        <div class="bignum" style="font-size:22px" id="frtt">&#8212;</div>
        <div id="fbadge" style="margin-top:4px"></div>
      </div>
    </div>
    <div class="mrow"><span class="ml">Q avg 1m</span><span class="mv" id="qavg">&#8212;</span></div>
    <div class="mrow"><span class="ml">Q P95</span><span class="mv" id="qp95">&#8212;</span></div>
    <div class="mrow"><span class="ml">Q P99</span><span class="mv" id="qp99">&#8212;</span></div>
    <div class="mrow"><span class="ml">Availability</span><span class="mv" id="avail">&#8212;</span></div>
    <div class="spark" id="rspark">&#183;&#183;&#183;</div>
  </div>
</div>

<!-- Machine detail table -->
<div class="section-label">Machine Detail</div>
<div class="card full" style="margin-bottom:14px">
  <table class="mt">
    <thead><tr>
      <th>Machine</th><th>Status</th><th>Queue RTT</th><th>File RTT</th>
      <th>Q avg (1m)</th><th>Q P95</th><th>LPM (1m)</th><th>Total URLs</th><th>Last Seen</th>
    </tr></thead>
    <tbody id="mtable">
      <tr><td colspan="9" style="color:#2d4068;font-style:italic;padding:16px">Waiting for machines&#8230;</td></tr>
    </tbody>
  </table>
</div>

<!-- Load distribution bars -->
<div class="grid2" style="margin-bottom:14px">
  <div class="card">
    <div class="ctitle">Load Distribution (total URLs)</div>
    <div id="load-bars"><div style="color:#162035;font-style:italic;font-size:11px">No data yet</div></div>
  </div>
  <div class="card">
    <div class="ctitle">LPM per Machine (last 1 min)</div>
    <div id="lpm-bars"><div style="color:#162035;font-style:italic;font-size:11px">No data yet</div></div>
  </div>
</div>

<!-- Live crawl feed -->
<div class="section-label">Live Crawl Feed</div>
<div class="card full" style="margin-bottom:10px">
  <div class="feed-grid" id="feed-grid">
    <div style="color:#162035;font-style:italic;font-size:11px">Loading feed&#8230;</div>
  </div>
</div>
<div class="ts">Updated: <span id="ts">&#8212;</span> &#183; Refreshes every 3s</div>

<script>
// ─── machine palette ─────────────────────────────────────────────────────
const PALETTE = {
  'crawler-mac':     { accent:'#818cf8', dimAccent:'#818cf820', icon:'\uD83C\uDF4E', label:'Mac' },
  'crawler-windows': { accent:'#34d399', dimAccent:'#34d39918', icon:'\uD83E\uDE9F', label:'Windows' },
  'crawler-oracle':  { accent:'#fb923c', dimAccent:'#fb923c18', icon:'\u2601\uFE0F',  label:'Oracle' },
};
const FALLBACK_COLORS = ['#f472b6','#60a5fa','#a78bfa','#facc15','#2dd4bf'];
let _fallbackIdx = 0;
function machineInfo(cid) {
  if (PALETTE[cid]) return PALETTE[cid];
  const c = FALLBACK_COLORS[_fallbackIdx++ % FALLBACK_COLORS.length];
  PALETTE[cid] = { accent: c, dimAccent: c+'18', icon: '\uD83D\uDCBB', label: cid };
  return PALETTE[cid];
}

// ─── per-machine buffers ─────────────────────────────────────────────────
const MAX_PTS = 40;
const buffers = {};   // cid -> { labels, lpm, qRtt, fRtt }
const charts  = {};   // cid -> { lpm: Chart, rtt: Chart }

function ensureBuf(cid) {
  if (!buffers[cid]) buffers[cid] = { labels:[], lpm:[], qRtt:[], fRtt:[] };
}
function pushPt(cid, ts, lpmV, qR, fR) {
  ensureBuf(cid);
  const b = buffers[cid];
  b.labels.push(ts);
  b.lpm.push(lpmV);
  b.qRtt.push(qR >= 0 ? qR : null);
  b.fRtt.push(fR >= 0 ? fR : null);
  while (b.labels.length > MAX_PTS) {
    b.labels.shift(); b.lpm.shift(); b.qRtt.shift(); b.fRtt.shift();
  }
}

// ─── chart factories ─────────────────────────────────────────────────────
const BASE_OPTS = {
  responsive:true, maintainAspectRatio:false,
  animation:{ duration:250 },
  elements:{ point:{ radius:0 } },
  plugins:{ legend:{ display:false } },
};
function axisOpts(beginAtZero=true) {
  return {
    y:{ beginAtZero, grid:{ color:'#0f1e38' },
        ticks:{ color:'#2d4068', font:{ size:8, family:"'JetBrains Mono',monospace" }, maxTicksLimit:4 } },
    x:{ display:false }
  };
}
function makeLpmChart(ctx, accent, dimAccent) {
  return new Chart(ctx, {
    type:'line',
    data:{ labels:[], datasets:[{
      label:'LPM', borderColor:accent, backgroundColor:dimAccent,
      fill:true, tension:0.4, borderWidth:1.5, data:[]
    }]},
    options:{ ...BASE_OPTS, scales:axisOpts() }
  });
}
function makeRttChart(ctx, accent) {
  return new Chart(ctx, {
    type:'line',
    data:{ labels:[], datasets:[
      { label:'Q-RTT', borderColor:accent, backgroundColor:accent+'14',
        fill:true, tension:0.4, borderWidth:1.5, data:[], spanGaps:true },
      { label:'F-RTT', borderColor:'#475569', backgroundColor:'#47556914',
        fill:true, tension:0.4, borderWidth:1.2, data:[], spanGaps:true },
    ]},
    options:{
      ...BASE_OPTS,
      plugins:{ legend:{
        display:true,
        labels:{ color:'#2d4068', font:{ size:8, family:"'JetBrains Mono',monospace" }, boxWidth:9, padding:6 }
      }},
      scales:axisOpts()
    }
  });
}

// ─── build or refresh a per-machine card ─────────────────────────────────
function upsertCard(cid, lpmV, qR, fR, totalUrls, isOnline) {
  const info = machineInfo(cid);
  const ts   = new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  pushPt(cid, ts, lpmV, qR, fR);

  const grid = document.getElementById('machine-graph-grid');

  if (!charts[cid]) {
    // Remove placeholder on first real card
    if (grid.children.length === 1 && grid.children[0].getAttribute('style')) {
      grid.innerHTML = '';
    }
    const card = document.createElement('div');
    card.className = 'mcard'; card.id = 'mc-' + cid;
    card.innerHTML =
      '<div class="mcard-head">' +
        '<span class="mcard-icon">' + info.icon + '</span>' +
        '<div>' +
          '<div class="mcard-name">' + (info.label || cid) + '</div>' +
          '<div class="mcard-id">' + cid + '</div>' +
        '</div>' +
        '<span class="mcard-status st-offline" id="mcs-' + cid + '">OFFLINE</span>' +
      '</div>' +
      '<div class="mcard-kpis">' +
        '<div class="kpi"><div class="kpi-label">LPM (1m)</div><div class="kpi-val dim" id="kl-' + cid + '">&#8212;</div></div>' +
        '<div class="kpi"><div class="kpi-label">Q-RTT</div><div class="kpi-val dim" id="kq-' + cid + '">&#8212;</div></div>' +
        '<div class="kpi"><div class="kpi-label">F-RTT</div><div class="kpi-val dim" id="kf-' + cid + '">&#8212;</div></div>' +
        '<div class="kpi"><div class="kpi-label">Total URLs</div><div class="kpi-val" id="kt-' + cid + '">&#8212;</div></div>' +
      '</div>' +
      '<div class="mcard-charts">' +
        '<div><div class="chart-label">Links Per Minute</div><div class="chart-wrap"><canvas id="cl-' + cid + '"></canvas></div></div>' +
        '<div><div class="chart-label">RTT &#8212; Queue &amp; File (ms)</div><div class="chart-wrap"><canvas id="cr-' + cid + '"></canvas></div></div>' +
      '</div>';
    grid.appendChild(card);

    const lCtx = document.getElementById('cl-' + cid).getContext('2d');
    const rCtx = document.getElementById('cr-' + cid).getContext('2d');
    charts[cid] = {
      lpm: makeLpmChart(lCtx, info.accent, info.dimAccent),
      rtt: makeRttChart(rCtx, info.accent),
    };
  }

  // Status badge
  const sEl = document.getElementById('mcs-' + cid);
  if (sEl) {
    sEl.className = 'mcard-status ' + (isOnline ? 'st-online' : 'st-offline');
    sEl.textContent = isOnline ? 'ONLINE' : 'OFFLINE';
  }

  // KPI helpers
  const rCls = ms => ms == null || ms < 0 ? 'dim' : ms <= 80 ? 'good' : ms <= 200 ? 'warn' : 'bad';
  const rTxt = ms => ms == null || ms < 0 ? '\u2014' : ms.toFixed(1) + 'ms';

  const el = id => document.getElementById(id);
  const lEl = el('kl-' + cid); if(lEl){lEl.textContent=lpmV.toFixed(1);lEl.className='kpi-val '+(lpmV>0?'good':'dim');}
  const qEl = el('kq-' + cid); if(qEl){qEl.textContent=rTxt(qR);qEl.className='kpi-val '+rCls(qR);}
  const fEl = el('kf-' + cid); if(fEl){fEl.textContent=rTxt(fR);fEl.className='kpi-val '+rCls(fR);}
  const tEl = el('kt-' + cid); if(tEl){tEl.textContent=totalUrls.toLocaleString();}

  // Refresh charts
  const b = buffers[cid], c = charts[cid];
  c.lpm.data.labels = [...b.labels];
  c.lpm.data.datasets[0].data = [...b.lpm];
  c.lpm.update('none');

  c.rtt.data.labels = [...b.labels];
  c.rtt.data.datasets[0].data = [...b.qRtt];
  c.rtt.data.datasets[1].data = [...b.fRtt];
  c.rtt.update('none');
}

// ─── existing helpers (unchanged) ────────────────────────────────────────
let peakLpm = 0;
const COLORS = ['#818cf8','#34d399','#fb923c','#f472b6','#60a5fa'];

function fmt(ms){
  if(ms==null||ms<0)return'<span class="rk">\u2014</span>';
  const c=ms<=80?'rg':ms<=200?'ry':'rr';
  return'<span class="'+c+'">'+ms.toFixed(1)+'ms</span>';
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
  return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m '+Math.floor(s%60)+'s';
}
function fmtAgo(ts){
  if(!ts)return'never';
  const a=Math.round(Date.now()/1000-ts);
  return a<60?a+'s ago':Math.floor(a/60)+'m ago';
}
function bars(obj,suffix){
  const entries=Object.entries(obj).sort((a,b)=>b[1]-a[1]);
  const mx=Math.max(...entries.map(([,v])=>v),1);
  const tot=entries.reduce((s,[,v])=>s+v,0)||1;
  return entries.map(([id,val],i)=>{
    const pct=Math.round(val/mx*100);
    const lbl=suffix?val.toFixed(1)+' '+suffix:val.toLocaleString()+' ('+Math.round(val/tot*100)+'%)';
    return'<div class="brow"><div class="blabel" title="'+id+'">'+id+'</div>'+
      '<div class="bbg"><div class="bfill" style="width:'+pct+'%;background:'+COLORS[i%COLORS.length]+'"></div></div>'+
      '<div class="bcount">'+lbl+'</div></div>';
  }).join('')||'<div style="color:#162035;font-style:italic;font-size:11px">No data</div>';
}

// ─── main refresh loop ────────────────────────────────────────────────────
async function refresh() {
  try {
    const [mon, feed] = await Promise.all([
      fetch('/api/monitor').then(r=>r.json()),
      fetch('/api/live-crawl').then(r=>r.json())
    ]);

    // Per-machine cards
    const nowSec = Date.now() / 1000;
    for (const m of mon.machines) {
      const cid  = m.crawler_id;
      const qR   = m.queue_rtt?.current ?? -1;
      const fR   = m.file_rtt?.current  ?? -1;
      const lpmV = (mon.per_crawler_lpm || {})[cid] ?? 0;
      const tot  = (mon.crawler_counts  || {})[cid] ?? 0;
      const ago  = m.last_seen ? Math.round(nowSec - m.last_seen) : 9999;
      upsertCard(cid, lpmV, qR, fR, tot, ago < 30);
    }

    // Aggregated LPM
    if (mon.lpm['1m'] > peakLpm) peakLpm = mon.lpm['1m'];
    document.getElementById('lpm1').textContent  = mon.lpm['1m'].toFixed(1);
    document.getElementById('lpm5').textContent  = mon.lpm['5m'].toFixed(1);
    document.getElementById('lpm10').textContent = mon.lpm['10m'].toFixed(1);
    document.getElementById('lpma').textContent  = mon.lpm.overall.toFixed(1);
    document.getElementById('lpmpeak').textContent = peakLpm.toFixed(1);
    document.getElementById('lspark').textContent  = mon.lpm_spark || '\u00B7\u00B7\u00B7';

    // Queue stats
    document.getElementById('qsize').textContent  = mon.queue_size.toLocaleString();
    document.getElementById('visited').textContent = mon.visited.toLocaleString();
    document.getElementById('indb').textContent    = mon.in_db.toLocaleString();
    document.getElementById('uptime').textContent  = fmtUptime(mon.uptime_sec);

    // Mac RTT
    const qr=mon.rtt.queue, fr=mon.rtt.file;
    document.getElementById('qrtt').innerHTML  = fmt(qr.current);
    document.getElementById('frtt').innerHTML  = fmt(fr.current);
    document.getElementById('qbadge').innerHTML = badge(qr.current);
    document.getElementById('fbadge').innerHTML = badge(fr.current);
    document.getElementById('qavg').innerHTML  = fmt(qr.avg);
    document.getElementById('qp95').innerHTML  = fmt(qr.p95);
    document.getElementById('qp99').innerHTML  = fmt(qr.p99);
    document.getElementById('avail').textContent = mon.availability.toFixed(1)+'%';
    document.getElementById('rspark').textContent = mon.q_rtt_spark || '\u00B7\u00B7\u00B7';

    // Machine table
    document.getElementById('mtable').innerHTML = mon.machines.length
      ? mon.machines.map(m => {
          const qc  = m.queue_rtt?.current, fc = m.file_rtt?.current;
          const ago = m.last_seen ? Math.round(nowSec - m.last_seen) : null;
          const online = ago != null && ago < 30;
          const lpm  = (mon.per_crawler_lpm||{})[m.crawler_id]??0;
          const total = (mon.crawler_counts||{})[m.crawler_id]??0;
          return '<tr><td><span class="mname">'+m.crawler_id+'</span>'+(m.is_local?'<span class="ltag">local</span>':'')+
            '</td><td>'+dot(online?qc:-1)+(online?badge(qc):'<span class="badge bk">OFFLINE</span>')+
            '</td><td>'+fmt(qc)+'</td><td>'+fmt(fc)+'</td><td>'+fmt(m.queue_rtt?.avg)+
            '</td><td>'+fmt(m.queue_rtt?.p95)+'</td>'+
            '<td style="color:#4ade80;font-weight:700">'+lpm.toFixed(1)+'</td>'+
            '<td style="font-weight:700">'+total.toLocaleString()+'</td>'+
            '<td style="color:#2d4068;font-size:10px">'+fmtAgo(m.last_seen)+'</td></tr>';
        }).join('')
      : '<tr><td colspan="9" style="color:#2d4068;font-style:italic;padding:16px">No machines connected yet</td></tr>';

    // Load bars
    document.getElementById('load-bars').innerHTML = bars(mon.crawler_counts||{}, null);
    document.getElementById('lpm-bars').innerHTML  = bars(mon.per_crawler_lpm||{}, 'LPM');

    // Live crawl feed
    const crawlers = Object.keys(feed);
    if (!crawlers.length) {
      document.getElementById('feed-grid').innerHTML =
        '<div style="color:#162035;font-style:italic;font-size:11px">No crawl data yet\u2026</div>';
    } else {
      document.getElementById('feed-grid').innerHTML = crawlers.map((cid,i) => {
        const urls = feed[cid]||[], info = machineInfo(cid);
        const items = urls.length
          ? urls.map(u=>'<div class="feed-item"><span class="ftime">'+u.time+'</span>'+
              '<a href="'+u.url+'" target="_blank" title="'+u.url+'">'+u.url+'</a></div>').join('')
          : '<div class="fempty">No URLs crawled yet</div>';
        return '<div class="card" style="padding:11px"><div class="feed-title">'+
          '<span class="feed-dot" style="background:'+info.accent+'"></span>'+
          (info.label||cid)+'<span class="feed-count">'+urls.length+' recent</span>'+
          '</div>'+items+'</div>';
      }).join('');
    }

    document.getElementById('ts').textContent = new Date().toLocaleTimeString();
  } catch(e) { console.error('Monitor error:', e); }
}

refresh();
setInterval(refresh, 3000);
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
                        try:
                            dt = datetime.fromisoformat(ts_str)
                            t  = dt.strftime('%H:%M:%S')
                        except Exception:
                            t = ts_str[:8]
                        per_machine[cid].append({'url': url, 'time': t})
        except Exception:
            pass
    return jsonify({cid: list(reversed(urls[-15:])) for cid, urls in per_machine.items()})


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