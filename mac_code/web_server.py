#!/usr/bin/env python3
"""
Web Dashboard + Live Monitor + Simple Local Search
"""
import os
import re
import time
import sqlite3
import statistics
import threading
import requests as http_requests
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

DB_FILE             = os.environ.get('DB_FILE',             './output/extracted.db')
LINKS_FILE          = os.environ.get('LINKS_FILE',          './output/links.txt')
GRPC_LINKS_FILE     = os.environ.get('GRPC_LINKS_FILE',     './output/grpc_links.txt')
QUEUE_SERVER        = os.environ.get('QUEUE_SERVER',        'queue-server:50051')
FILE_SERVER         = os.environ.get('FILE_SERVER',         'file-server:50052')

# ── OpenRouter API key ─────────────────────────────────────────────────────
OPENROUTER_API_KEY  = os.environ.get('OPENROUTER_API_KEY', '')

# ── Per-machine metrics store ─────────────────────────────────────────────
_machine_metrics = {}
_machine_lock    = threading.Lock()

# ── Crawler ID Normalization Mapping ──────────────────────────────────────
CRAWLER_ALIASES = {
    'Mac': 'crawler-mac',
}


# ── Background collector ──────────────────────────────────────────────────
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
            opts = [('grpc.keepalive_time_ms', 10000), ('grpc.keepalive_timeout_ms', 5000)]
            self._qstub = crawler_pb2_grpc.QueueServiceStub(grpc.insecure_channel(QUEUE_SERVER, options=opts))
            self._fstub = crawler_pb2_grpc.FileServiceStub(grpc.insecure_channel(FILE_SERVER, options=opts))
        except Exception:
            pass

    def _poll(self):
        if not self._qstub:
            return
        try:
            t0 = time.perf_counter()
            r  = self._qstub.GetStats(crawler_pb2.GetStatsRequest(), timeout=3)
            q_rtt = (time.perf_counter() - t0) * 1000
            snap  = {'ts': time.time(), 'visited': r.visited_count, 'queue_size': r.queue_size}
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
                self._fstub.StoreLink(crawler_pb2.StoreLinkRequest(url='__rtt_probe__', crawler_id='monitor', timestamp=0), timeout=3)
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
            return (list(self.snapshots), list(self.q_rtts), list(self.f_rtts), self.total_probes, self.failures)

collector = StatsCollector()

# ── Chart history ──────────────────────────────────────────────────────────
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
        'ts': round(time.time(), 1),
        'label': datetime.now().strftime('%H:%M:%S'),
        'total_lpm': total_lpm,
        'per_lpm': dict(per_lpm),
        'mac_q_rtt': machine_rtts.get('crawler-mac', -1),
        'mac_f_rtt': round(f_rtts[-1], 1) if f_rtts else -1,
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


# ── Helper functions ──────────────────────────────────────────────────────
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
        current=round(cur, 1), avg=round(statistics.mean(s), 1),
        p95=round(s[int(n * 0.95)], 1), p99=round(s[min(int(n * 0.99), n - 1)], 1),
        min=round(s[0], 1), max=round(s[-1], 1), count=n,
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

_RAW_LOG_RE = re.compile(r'^\[([^\]]+)\]\s+\[([^\]]+)\]\s+\[SCORE:\s*[\d.]+\]\s+(https?://\S+)')
def _parse_raw_log_line(line):
    m = _RAW_LOG_RE.match(line)
    if m:
        ts_str, cid, url = m.groups()
        cid = cid.strip()  # fix: strip whitespace
        cid = CRAWLER_ALIASES.get(cid, cid)
        return (ts_str, cid, url)
    return None

def _parse_grpc_links():
    counts   = defaultdict(int)
    timeline = []
    seen     = set()
    if not os.path.exists(GRPC_LINKS_FILE):
        return dict(counts), timeline
    try:
        with open(GRPC_LINKS_FILE, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('='):
                    continue
                parsed = _parse_raw_log_line(line)
                if not parsed:
                    continue
                ts_str, cid, url = parsed
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


# ── MAIN DASHBOARD HTML (Simple Local Search) ────────────────────────────
MAIN_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CrawlerX</title>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI",Roboto,Helvetica,sans-serif;background:#f5f5f7;color:#1d1d1f;-webkit-font-smoothing:antialiased}
.topbar{background:rgba(255,255,255,0.72);backdrop-filter:saturate(180%) blur(20px);border-bottom:1px solid rgba(0,0,0,0.08);padding:0 24px;display:flex;align-items:center;height:56px;position:sticky;top:0;z-index:100}
.topbar h1{font-size:18px;font-weight:600;color:#ff6600;margin-right:32px;letter-spacing:-0.5px}
.tab-btn{background:none;border:none;color:#86868b;padding:0 16px;height:56px;font-size:14px;font-weight:500;cursor:pointer;border-bottom:2px solid transparent;transition:all .15s}
.tab-btn:hover{color:#1d1d1f}
.tab-btn.active{color:#ff6600;border-bottom-color:#ff6600}
.topbar-right{margin-left:auto;font-size:12px;color:#86868b}
.tab-panel{display:none;padding:32px 24px;max-width:1300px;margin:0 auto}
.tab-panel.active{display:block}

/* Stats Row */
.stats-row{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}
.stat-pill{background:white;border:none;border-radius:14px;padding:12px 20px;font-size:14px;color:#86868b;box-shadow:0 2px 10px rgba(0,0,0,.04)}
.stat-pill span{color:#1d1d1f;font-weight:600}

/* Simple Local Search Bar */
#search-wrapper {
    margin-bottom: 24px;
    background: white;
    border-radius: 18px;
    padding: 16px 20px;
    box-shadow: 0 4px 24px rgba(0,0,0,.04);
}
#simple-search {
    width: 100%;
    padding: 12px 16px;
    border: 1px solid #e5e5ea;
    border-radius: 980px;
    font-size: 15px;
    background: #fafafa;
    outline: none;
    transition: all .15s;
}
#simple-search:focus { border-color: #ff6600; background: white; }

/* Local Ranked Links */
.hit-item {
    background: white;
    border-radius: 14px;
    padding: 16px 20px;
    margin-bottom: 12px;
    border: 1px solid #e5e5ea;
    box-shadow: 0 2px 8px rgba(0,0,0,.03);
    transition: all .15s;
}
.hit-item:hover { border-color: #d2d2d7; box-shadow: 0 4px 12px rgba(0,0,0,.06); }
.hit-rank { color: #86868b; font-weight: 500; font-size: 14px; min-width: 32px; display: inline-block; }
.hit-score { background: rgba(255,102,0,0.1); color: #ff6600; padding: 4px 10px; border-radius: 8px; font-weight: 600; font-size: 12px; margin-right: 12px; }
.hit-url { color: #0071e3; text-decoration: none; word-break: break-all; font-weight: 500; }
.hit-url:hover { text-decoration: underline; }
.hit-meta { margin-top: 8px; padding-left: 44px; font-size: 12px; color: #86868b; display: flex; gap: 12px; flex-wrap: wrap; }

/* Footer */
.footer{margin-top:24px;text-align:center;color:#86868b;font-size:13px}

/* Modals */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.4);backdrop-filter:blur(4px);z-index:1000;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal-box{background:white;border-radius:20px;width:82%;max-width:860px;max-height:82vh;display:flex;flex-direction:column;box-shadow:0 24px 48px rgba(0,0,0,.12)}
.modal-header{padding:20px 24px;border-bottom:1px solid #e5e5ea;display:flex;justify-content:space-between;align-items:center}
.modal-header h3{font-size:16px;font-weight:600;color:#1d1d1f}
.modal-close{background:#f5f5f7;border:none;color:#86868b;font-size:18px;cursor:pointer;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;transition:all .2s}
.modal-close:hover{background:#e5e5ea;color:#1d1d1f}
.modal-body{padding:24px;overflow-y:auto;flex:1}
.modal-text{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:13px;line-height:1.6;color:#1d1d1f;white-space:pre-wrap;background:#f5f5f7;padding:20px;border-radius:12px}
.loading{text-align:center;padding:40px;color:#86868b}
</style></head><body>
<div class="topbar">
  <h1>CrawlerX</h1>
  <button class="tab-btn active" onclick="switchTab('rankings',this)">Rankings</button>
  <button class="tab-btn" onclick="switchTab('monitor',this)">Live Monitor</button>
  <div class="topbar-right" id="last-update">&#8212;</div>
</div>

<!-- Rankings Tab (Simple Local Search) -->
<div id="tab-rankings" class="tab-panel active">
  <div class="stats-row">
    <div class="stat-pill">Total Links: <span id="r-total">&#8212;</span></div>
    <div class="stat-pill">Avg Score: <span id="r-avg">&#8212;</span></div>
    <div class="stat-pill" style="color:#86868b;box-shadow:none;background:transparent;padding-left:0">Local search</div>
  </div>

  <!-- Simple Search Bar -->
  <div id="search-wrapper">
    <input type="text" id="simple-search" placeholder="Search titles, URLs, or keywords..." />
  </div>

  <!-- Local Ranked Links -->
  <div id="local-rankings">
    <div id="local-rankings-loading" style="text-align:center;padding:40px;color:#86868b">Loading ranked links…</div>
    <div id="local-rankings-table" style="display:none"></div>
  </div>
</div>

<!-- Monitor Tab (Iframe) -->
<div id="tab-monitor" class="tab-panel" style="padding: 0;">
  <iframe src="/monitor" style="width:100%;height:calc(100vh - 56px);border:none;display:block;" id="monitor-frame"></iframe>
</div>

<!-- View Content Modal -->
<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal-box">
    <div class="modal-header">
      <h3 id="modal-title">Extracted Content</h3>
      <button class="modal-close" onclick="closeModal()">&#215;</button>
    </div>
    <div class="modal-body"><div class="modal-text" id="modal-body">Loading&#8230;</div></div>
  </div>
</div>

<!-- Summary Modal (Fixed Markdown rendering) -->
<div class="modal-overlay" id="summary-modal" onclick="if(event.target===this)closeSummaryModal()">
  <div class="modal-box">
    <div class="modal-header" style="background:rgba(255,102,0,0.05);border-radius:20px 20px 0 0;border-bottom:1px solid rgba(255,102,0,0.1)">
      <div>
        <h3 id="summary-modal-title" style="color:#ff6600">&#129302; AI Summary</h3>
        <div id="summary-modal-url" style="font-size:12px;color:#86868b;margin-top:4px;word-break:break-all"></div>
      </div>
      <button class="modal-close" onclick="closeSummaryModal()">&#215;</button>
    </div>
    <div class="modal-body">
      <div id="summary-modal-body" style="font-size:14px;line-height:1.8;color:#1d1d1f;white-space:pre-wrap;padding:4px 0"></div>
    </div>
    <div style="padding:12px 24px;border-top:1px solid #e5e5ea;font-size:12px;color:#86868b;text-align:right">
      Powered by OpenRouter
    </div>
  </div>
</div>

<script>
// ─── Tab Switching ────────────────────────────────────────────────────────
function switchTab(name,btn){
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  btn.classList.add('active');
}

// ─── Modals ──────────────────────────────────────────────────────────────
function esc(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML}
function closeModal(){document.getElementById('modal').classList.remove('open')}
function closeSummaryModal(){document.getElementById('summary-modal').classList.remove('open')}

async function viewText(u,title){
  document.getElementById('modal-title').textContent=decodeURIComponent(title);
  document.getElementById('modal-body').textContent='Loading…';
  document.getElementById('modal').classList.add('open');
  try{const d=await(await fetch('/api/content?url='+u)).json();
  document.getElementById('modal-body').textContent=d.text||'(No content)';}
  catch{document.getElementById('modal-body').textContent='Error loading content.';}
}

async function summarizeText(u,title,idx){
  const btn=document.getElementById('sb-'+idx);
  if(btn){btn.disabled=true;btn.innerHTML='<span class="summary-spinner"></span>Summarizing…';}
  const decoded=decodeURIComponent(u);
  const decodedTitle=decodeURIComponent(title);
  document.getElementById('summary-modal-title').textContent='🤖 AI Summary';
  document.getElementById('summary-modal-url').textContent=decodedTitle;
  document.getElementById('summary-modal-body').textContent='Generating summary… this may take a few seconds.';
  document.getElementById('summary-modal').classList.add('open');
  try{
    const d=await(await fetch('/api/summarize?url='+u)).json();
    if(d.error){
      document.getElementById('summary-modal-body').innerHTML=
        '<span style="color:#ff3b30">⚠️ Error: '+esc(d.error)+'</span>';
    } else {
      document.getElementById('summary-modal-body').innerHTML = marked.parse(d.summary || '(No summary generated)');
    }
  } catch(e){
    document.getElementById('summary-modal-body').innerHTML=
      '<span style="color:#ff3b30">⚠️ Network error. Is the server running?</span>';
  } finally {
    if(btn){btn.disabled=false;btn.innerHTML='Summary';}
  }
}

// ─── Load and Filter Rankings ─────────────────────────────────────────────
let allLinks = [];

async function loadLocalRankings(query = '') {
  try {
    const loading = document.getElementById('local-rankings-loading');
    const table   = document.getElementById('local-rankings-table');

    // Only fetch if we haven't fetched before
    if (allLinks.length === 0) {
        const response = await fetch('/api/links');
        allLinks = await response.json();
    }

    if (!allLinks || allLinks.length === 0) {
      loading.innerHTML = '<div style="color:#86868b;padding:40px;text-align:center;">No ranked links yet. Start the crawler to populate rankings.</div>';
      return;
    }

    loading.style.display = 'none';
    table.style.display   = 'block';

    // Filter based on query
    let filtered = allLinks;
    if (query && query.trim() !== '') {
        const q = query.toLowerCase().trim();
        filtered = allLinks.filter(l => 
            (l.title && l.title.toLowerCase().includes(q)) || 
            l.url.toLowerCase().includes(q)
        );
    }

    if (filtered.length === 0) {
        table.innerHTML = '<div style="color:#86868b;padding:40px;text-align:center;">No links match your search.</div>';
        return;
    }

    table.innerHTML = filtered.map((l, i) => `
      <div class="hit-item">
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
          <span class="hit-rank">#${i+1}</span>
          <span class="hit-score">${l.score.toFixed(2)}</span>
          <a class="hit-url" href="${esc(l.url)}" target="_blank">${esc(l.title||l.url)}</a>
        </div>
        <div class="hit-meta">
          <span>${esc(l.url)}</span>
          <button onclick="viewText('${encodeURIComponent(l.url)}','${encodeURIComponent(l.title||l.url)}')" style="background:#f5f5f7;border:none;padding:3px 10px;border-radius:6px;font-size:12px;cursor:pointer;color:#1d1d1f">View</button>
          <button id="sb-${i}" onclick="summarizeText('${encodeURIComponent(l.url)}','${encodeURIComponent(l.title||l.url)}',${i})" style="background:#f5f5f7;border:none;padding:3px 10px;border-radius:6px;font-size:12px;cursor:pointer;color:#1d1d1f">Summary</button>
        </div>
      </div>
    `).join('');
  } catch(e) {
    document.getElementById('local-rankings-loading').innerHTML =
      '<div style="color:#ff3b30;padding:40px;text-align:center;">Error loading rankings: ' + e.message + '</div>';
  }
}

// ─── Search Listener ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('simple-search');
    let debounceTimer;
    
    searchInput.addEventListener('input', (e) => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            loadLocalRankings(e.target.value);
        }, 250); // 250ms debounce so it doesn't overload the browser on every keystroke
    });
});

// ─── Load stats ──────────────────────────────────────────────────────────
async function loadStats() {
  try{
    const stats = await fetch('/api/stats').then(r=>r.json());
    document.getElementById('r-total').textContent=stats.total_links;
    document.getElementById('r-avg').textContent=stats.avg_score;
    document.getElementById('last-update').textContent='Updated '+new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}

// ─── Start everything ────────────────────────────────────────────────────
loadStats();
loadLocalRankings();   // Load the initial list
setInterval(loadStats, 60000);  // refresh stats every minute
</script></body></html>"""


# ══════════════════════════════════════════════════════════════════════════
# MONITOR HTML
# ══════════════════════════════════════════════════════════════════════════
MONITOR_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI",Roboto,Helvetica,sans-serif;background:#f5f5f7;color:#1d1d1f;padding:32px 24px;min-height:100vh;-webkit-font-smoothing:antialiased}

.section-label{font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;color:#86868b;margin:0 0 16px 4px;display:flex;align-items:center;gap:16px}
.section-label::after{content:'';flex:1;height:1px;background:#e5e5ea}

.machine-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:20px;margin-bottom:32px}
.mcard{background:#fff;border:1px solid #e5e5ea;border-radius:18px;overflow:hidden;transition:all .2s;box-shadow:0 4px 14px rgba(0,0,0,.03)}
.mcard:hover{border-color:#d2d2d7;box-shadow:0 8px 24px rgba(0,0,0,.06)}
.mcard-head{display:flex;align-items:center;gap:12px;padding:16px 20px;border-bottom:1px solid #f5f5f7}
.mcard-icon{font-size:20px;line-height:1}
.mcard-name{font-size:15px;font-weight:600;color:#1d1d1f}
.mcard-id{font-size:11px;color:#86868b;margin-top:2px}
.mcard-status{margin-left:auto;font-size:10px;font-weight:600;letter-spacing:.05em;padding:4px 12px;border-radius:980px;text-transform:uppercase}
.st-online{background:#e3f5e1;color:#1d8236;}
.st-warn{background:#fff3d4;color:#f59e0b;}
.st-offline{background:#e5e5ea;color:#86868b;}

.mcard-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:#e5e5ea;border-bottom:1px solid #e5e5ea}
.kpi{background:#fafafa;padding:12px 16px}
.kpi-label{font-size:10px;color:#86868b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;font-weight:500}
.kpi-val{font-size:18px;font-weight:600;color:#1d1d1f;line-height:1.1;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.kpi-val.good{color:#34c759}
.kpi-val.warn{color:#ff9500}
.kpi-val.bad{color:#ff3b30}
.kpi-val.dim{color:#86868b}

.mcard-charts{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px 20px 20px}
.chart-label{font-size:10px;color:#86868b;letter-spacing:.05em;text-transform:uppercase;margin-bottom:8px;font-weight:500}
.chart-wrap{height:90px;position:relative}

.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;margin-bottom:32px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:32px}
.full{grid-column:1/-1}
.card{background:#fff;border:1px solid #e5e5ea;border-radius:18px;padding:24px;box-shadow:0 4px 14px rgba(0,0,0,.03)}
.ctitle{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:#86868b;margin-bottom:16px}
.bignum{font-size:36px;font-weight:600;color:#1d1d1f;line-height:1;letter-spacing:-1px}
.bigsub{font-size:12px;color:#86868b;margin-top:6px}
.mrow{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #f5f5f7;font-size:13px}
.mrow:last-child{border:none}
.ml{color:#86868b}.mv{font-weight:600;color:#1d1d1f}
.spark{font-family:ui-monospace,SFMono-Regular,monospace;font-size:16px;color:#ff6600;letter-spacing:2px;margin-top:16px;word-break:break-all}
.mt{width:100%;border-collapse:collapse;font-size:13px}
.mt th{text-align:left;padding:12px 16px;color:#86868b;font-weight:600;border-bottom:1px solid #e5e5ea;font-size:10px;text-transform:uppercase;letter-spacing:.05em;background:#fafafa}
.mt td{padding:12px 16px;border-bottom:1px solid #f5f5f7;vertical-align:middle;color:#1d1d1f}
.mt tr:last-child td{border:none}
.mname{font-weight:600;color:#1d1d1f}
.ltag{font-size:10px;color:#86868b;background:#f5f5f7;padding:2px 8px;border-radius:980px;margin-left:8px;font-weight:500}
.badge{display:inline-block;padding:3px 10px;border-radius:980px;font-size:10px;font-weight:600}
.bg{background:#e3f5e1;color:#1d8236}.by{background:#fff3d4;color:#f59e0b}
.br{background:#ffe3e3;color:#d93025}.bk{background:#e5e5ea;color:#86868b}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:8px;vertical-align:middle}
.dg{background:#34c759}.dy{background:#ff9500}.dr{background:#ff3b30}.dk{background:#86868b}
.brow{display:flex;align-items:center;gap:12px;margin-bottom:10px;font-size:13px}
.blabel{width:140px;color:#86868b;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:500}
.bbg{flex:1;background:#f5f5f7;border-radius:980px;height:8px;overflow:hidden}
.bfill{height:100%;border-radius:980px;transition:width .6s}
.bcount{width:90px;text-align:right;color:#1d1d1f;font-weight:600;font-size:12px;font-family:ui-monospace,SFMono-Regular,monospace}
.feed-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:20px}
.feed-title{font-size:13px;font-weight:600;color:#1d1d1f;margin-bottom:12px;display:flex;align-items:center;gap:8px}
.feed-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.feed-count{font-size:11px;background:#f5f5f7;color:#86868b;padding:2px 8px;border-radius:980px;font-weight:500}
.feed-item{padding:6px 0;border-bottom:1px solid #f5f5f7;font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.feed-item:last-child{border:none}
.feed-item a{color:#0071e3;text-decoration:none}
.feed-item a:hover{text-decoration:underline}
.ftime{color:#86868b;font-size:11px;margin-right:6px;font-family:ui-monospace,SFMono-Regular,monospace}
.fempty{color:#86868b;font-size:13px;padding:12px 0}
.ts{font-size:12px;color:#86868b;text-align:right;margin-top:16px}
.rg{color:#34c759;font-weight:600}.ry{color:#ff9500;font-weight:600}
.rr{color:#ff3b30;font-weight:600}.rk{color:#86868b}
</style></head><body>
<div class="section-label">Per-Machine &mdash; LPM &amp; RTT Live Graphs</div>
<div class="machine-grid" id="machine-graph-grid">
  <div style="color:#86868b;font-size:14px;padding:24px;grid-column:1/-1;text-align:center">Waiting for machines to connect&#8230;</div>
</div>
<div class="section-label">Cluster Aggregates</div>
<div class="grid3">
  <div class="card">
    <div class="ctitle">Links Per Minute &mdash; all machines</div>
    <div class="bignum" id="lpm1">&#8212;</div><div class="bigsub">1-minute rate</div>
    <div style="margin-top:20px">
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
    <div style="margin-top:20px">
      <div class="mrow"><span class="ml">Total Visited</span><span class="mv" id="visited">&#8212;</span></div>
      <div class="mrow"><span class="ml">In DB (extracted)</span><span class="mv" id="indb">&#8212;</span></div>
      <div class="mrow"><span class="ml">Uptime</span><span class="mv" id="uptime">&#8212;</span></div>
    </div>
  </div>
  <div class="card">
    <div class="ctitle">Queue + File RTT &mdash; Mac local</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
      <div style="background:#fafafa;border:1px solid #f5f5f7;border-radius:12px;padding:14px">
        <div style="font-size:10px;color:#86868b;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;font-weight:600">Queue RTT</div>
        <div class="bignum" style="font-size:24px" id="qrtt">&#8212;</div>
        <div id="qbadge" style="margin-top:8px"></div>
      </div>
      <div style="background:#fafafa;border:1px solid #f5f5f7;border-radius:12px;padding:14px">
        <div style="font-size:10px;color:#86868b;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;font-weight:600">File RTT</div>
        <div class="bignum" style="font-size:24px" id="frtt">&#8212;</div>
        <div id="fbadge" style="margin-top:8px"></div>
      </div>
    </div>
    <div class="mrow"><span class="ml">Q avg 1m</span><span class="mv" id="qavg">&#8212;</span></div>
    <div class="mrow"><span class="ml">Q P95</span><span class="mv" id="qp95">&#8212;</span></div>
    <div class="mrow"><span class="ml">Q P99</span><span class="mv" id="qp99">&#8212;</span></div>
    <div class="mrow"><span class="ml">Availability</span><span class="mv" id="avail">&#8212;</span></div>
    <div class="spark" id="rspark">&#183;&#183;&#183;</div>
  </div>
</div>
<div class="section-label">Machine Detail</div>
<div class="card full" style="margin-bottom:32px;padding:0;overflow:hidden">
  <table class="mt">
    <thead><tr>
      <th>Machine</th><th>Status</th><th>Queue RTT</th><th>File RTT</th>
      <th>Q avg (1m)</th><th>Q P95</th><th>LPM (1m)</th><th>Total URLs</th><th>Last Seen</th>
    </tr></thead>
    <tbody id="mtable">
      <tr><td colspan="9" style="color:#86868b;text-align:center;padding:32px">Waiting for machines&#8230;</td></tr>
    </tbody>
  </table>
</div>
<div class="grid2" style="margin-bottom:32px">
  <div class="card">
    <div class="ctitle">Load Distribution (total URLs)</div>
    <div id="load-bars"><div style="color:#86868b;font-size:13px">No data yet</div></div>
  </div>
  <div class="card">
    <div class="ctitle">LPM per Machine (last 1 min)</div>
    <div id="lpm-bars"><div style="color:#86868b;font-size:13px">No data yet</div></div>
  </div>
</div>
<div class="section-label">Live Crawl Feed</div>
<div class="card full" style="margin-bottom:16px;background:transparent;border:none;box-shadow:none;padding:0">
  <div class="feed-grid" id="feed-grid">
    <div style="color:#86868b;font-size:14px;text-align:center;grid-column:1/-1">Loading feed&#8230;</div>
  </div>
</div>
<div class="ts">Updated: <span id="ts">&#8212;</span> &middot; Refreshes every 3s</div>
<script>
const PALETTE = {
  'crawler-mac':     { accent:'#ff6600', dimAccent:'rgba(255,102,0,0.1)', icon:'\uD83C\uDF4E', label:'Mac' },
  'crawler-windows': { accent:'#0071e3', dimAccent:'rgba(0,113,227,0.1)', icon:'\uD83E\uDE9F', label:'Windows' },
  'crawler-oracle':  { accent:'#34c759', dimAccent:'rgba(52,199,89,0.1)', icon:'\u2601\uFE0F',  label:'Oracle' },
};
const FALLBACK_COLORS = ['#ff6600','#0071e3','#34c759','#5e5ce6','#ff3b30'];
let _fallbackIdx = 0;
function machineInfo(cid) {
  if (PALETTE[cid]) return PALETTE[cid];
  const c = FALLBACK_COLORS[_fallbackIdx++ % FALLBACK_COLORS.length];
  PALETTE[cid] = { accent: c, dimAccent: c+'20', icon: '\uD83D\uDCBB', label: cid };
  return PALETTE[cid];
}
const MAX_PTS = 40;
const buffers = {};
const charts  = {};
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
const BASE_OPTS = {
  responsive:true, maintainAspectRatio:false,
  animation:{ duration:250 },
  elements:{ point:{ radius:0 } },
  plugins:{ legend:{ display:false } },
};
function axisOpts(beginAtZero=true) {
  return {
    y:{ beginAtZero, grid:{ color:'#f5f5f7' },
        ticks:{ color:'#86868b', font:{ size:10, family:'-apple-system, sans-serif' }, maxTicksLimit:4 } },
    x:{ display:false }
  };
}
function makeLpmChart(ctx, accent, dimAccent) {
  return new Chart(ctx, {
    type:'line',
    data:{ labels:[], datasets:[{
      label:'LPM', borderColor:accent, backgroundColor:dimAccent,
      fill:true, tension:0.4, borderWidth:2, data:[]
    }]},
    options:{ ...BASE_OPTS, scales:axisOpts() }
  });
}
function makeRttChart(ctx, accent) {
  return new Chart(ctx, {
    type:'line',
    data:{ labels:[], datasets:[
      { label:'Q-RTT', borderColor:accent, backgroundColor:accent+'14',
        fill:true, tension:0.4, borderWidth:2, data:[], spanGaps:true },
      { label:'F-RTT', borderColor:'#86868b', backgroundColor:'rgba(134,134,139,0.1)',
        fill:true, tension:0.4, borderWidth:1.5, data:[], spanGaps:true },
    ]},
    options:{
      ...BASE_OPTS,
      plugins:{ legend:{
        display:true,
        labels:{ color:'#86868b', font:{ size:10, family:'-apple-system, sans-serif' }, boxWidth:10, padding:8 }
      }},
      scales:axisOpts()
    }
  });
}
function upsertCard(cid, lpmV, qR, fR, totalUrls, isOnline) {
  const info = machineInfo(cid);
  const ts   = new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  pushPt(cid, ts, lpmV, qR, fR);
  const grid = document.getElementById('machine-graph-grid');
  if (!charts[cid]) {
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
  const sEl = document.getElementById('mcs-' + cid);
  if (sEl) {
    sEl.className = 'mcard-status ' + (isOnline ? 'st-online' : 'st-offline');
    sEl.textContent = isOnline ? 'ONLINE' : 'OFFLINE';
  }
  const rCls = ms => ms == null || ms < 0 ? 'dim' : ms <= 80 ? 'good' : ms <= 200 ? 'warn' : 'bad';
  const rTxt = ms => ms == null || ms < 0 ? '\u2014' : ms.toFixed(1) + 'ms';
  const el = id => document.getElementById(id);
  const lEl = el('kl-' + cid); if(lEl){lEl.textContent=lpmV.toFixed(1);lEl.className='kpi-val '+(lpmV>0?'good':'dim');}
  const qEl = el('kq-' + cid); if(qEl){qEl.textContent=rTxt(qR);qEl.className='kpi-val '+rCls(qR);}
  const fEl = el('kf-' + cid); if(fEl){fEl.textContent=rTxt(fR);fEl.className='kpi-val '+rCls(fR);}
  const tEl = el('kt-' + cid); if(tEl){tEl.textContent=totalUrls.toLocaleString();}
  const b = buffers[cid], c = charts[cid];
  c.lpm.data.labels = [...b.labels];
  c.lpm.data.datasets[0].data = [...b.lpm];
  c.lpm.update('none');
  c.rtt.data.labels = [...b.labels];
  c.rtt.data.datasets[0].data = [...b.qRtt];
  c.rtt.data.datasets[1].data = [...b.fRtt];
  c.rtt.update('none');
}
let peakLpm = 0;
const COLORS = ['#ff6600','#0071e3','#34c759','#5e5ce6','#ff3b30'];
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
  }).join('')||'<div style="color:#86868b;font-size:13px">No data</div>';
}
async function refresh() {
  try {
    const [mon, feed] = await Promise.all([
      fetch('/api/monitor?_t=' + Date.now()).then(r=>r.json()),
      fetch('/api/live-crawl?_t=' + Date.now()).then(r=>r.json())
    ]);
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
    if (mon.lpm['1m'] > peakLpm) peakLpm = mon.lpm['1m'];
    document.getElementById('lpm1').textContent  = mon.lpm['1m'].toFixed(1);
    document.getElementById('lpm5').textContent  = mon.lpm['5m'].toFixed(1);
    document.getElementById('lpm10').textContent = mon.lpm['10m'].toFixed(1);
    document.getElementById('lpma').textContent  = mon.lpm.overall.toFixed(1);
    document.getElementById('lpmpeak').textContent = peakLpm.toFixed(1);
    document.getElementById('lspark').textContent  = mon.lpm_spark || '\u00B7\u00B7\u00B7';
    document.getElementById('qsize').textContent  = mon.queue_size.toLocaleString();
    document.getElementById('visited').textContent = mon.visited.toLocaleString();
    document.getElementById('indb').textContent    = mon.in_db.toLocaleString();
    document.getElementById('uptime').textContent  = fmtUptime(mon.uptime_sec);
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
            '<td style="color:#34c759;font-weight:600;font-family:ui-monospace,monospace">'+lpm.toFixed(1)+'</td>'+
            '<td style="font-weight:600;font-family:ui-monospace,monospace">'+total.toLocaleString()+'</td>'+
            '<td style="color:#86868b;font-size:11px">'+fmtAgo(m.last_seen)+'</td></tr>';
        }).join('')
      : '<tr><td colspan="9" style="color:#86868b;text-align:center;padding:32px">No machines connected yet</td></tr>';
    document.getElementById('load-bars').innerHTML = bars(mon.crawler_counts||{}, null);
    document.getElementById('lpm-bars').innerHTML  = bars(mon.per_crawler_lpm||{}, 'LPM');
    const crawlers = Object.keys(feed);
    if (!crawlers.length) {
      document.getElementById('feed-grid').innerHTML =
        '<div style="color:#86868b;font-size:14px;text-align:center;grid-column:1/-1">No crawl data yet\u2026</div>';
    } else {
      document.getElementById('feed-grid').innerHTML = crawlers.map((cid,i) => {
        const urls = feed[cid]||[], info = machineInfo(cid);
        const items = urls.length
          ? urls.map(u=>'<div class="feed-item"><span class="ftime">'+u.time+'</span>'+
              '<a href="'+u.url+'" target="_blank" title="'+u.url+'">'+u.url+'</a></div>').join('')
          : '<div class="fempty">No URLs crawled yet</div>';
        return '<div class="card" style="padding:20px;border-radius:18px"><div class="feed-title">'+
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
    data = request.get_json(silent=True)
    if not data or 'crawler_id' not in data:
        return jsonify({'error': 'Missing crawler_id'}), 400
    cid = data['crawler_id']
    with _machine_lock:
        if cid not in _machine_metrics:
            _machine_metrics[cid] = {
                'crawler_id': cid, 'is_local': False,
                'last_seen': None, 'queue_rtt': {}, 'file_rtt': {},
                'uptime_sec': 0, 'lpm_crawled': 0.0, 'lpm_stored': 0.0,
                'total_crawled': 0, 'total_stored': 0,
            }
        _machine_metrics[cid].update({
            'last_seen': data.get('timestamp', time.time()),
            'queue_rtt': data.get('queue_rtt', {}),
            'file_rtt': data.get('file_rtt', {}),
            'uptime_sec': data.get('uptime_sec', 0),
            'lpm_crawled': data.get('lpm_crawled', 0.0),
            'lpm_stored': data.get('lpm_stored', 0.0),
            'total_crawled': data.get('total_crawled', 0),
            'total_stored': data.get('total_stored', 0),
        })
    return jsonify({'ok': True})

@app.route('/api/live-crawl')
def api_live_crawl():
    per_machine = defaultdict(list)
    seen = set()

    raw_re = re.compile(r'^\[([^\]]+)\]\s+\[([^\]]+)\]\s+\[SCORE:\s*[\d.]+\]\s+(https?://\S+)')
    ranked_re = re.compile(r'^\[\s*\d+\]\s+\[SCORE:\s*[\d.]+\]\s+\[([^\]]+)\]\s+(https?://\S+)')

    for fpath in [GRPC_LINKS_FILE, LINKS_FILE]:
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or line.startswith('='):
                        continue

                    # Try raw log format (grpc_links.txt)
                    m = raw_re.match(line)
                    if m:
                        ts_str, cid, url = m.groups()
                        cid = cid.strip()                  # ← strip whitespace!
                        cid = CRAWLER_ALIASES.get(cid, cid) # normalize alias
                        key = f"{cid}:{url}"
                        if key in seen:
                            continue
                        seen.add(key)
                        try:
                            dt = datetime.fromisoformat(ts_str)
                            t = dt.strftime('%H:%M:%S')
                        except Exception:
                            t = ts_str[:8]
                        per_machine[cid].append({'url': url, 'time': t})
                        continue

                    # Try ranked format (links.txt)
                    m = ranked_re.match(line)
                    if m:
                        cid, url = m.groups()
                        cid = cid.strip()                  # ← strip whitespace!
                        cid = CRAWLER_ALIASES.get(cid, cid) # normalize alias
                        key = f"{cid}:{url}"
                        if key in seen:
                            continue
                        seen.add(key)
                        t = datetime.fromtimestamp(os.path.getmtime(fpath)).strftime('%H:%M:%S')
                        per_machine[cid].append({'url': url, 'time': t})
        except Exception:
            pass

    # Return only the latest 15 per crawler (reversed so newest first)
    return jsonify({cid: list(reversed(urls[-15:])) for cid, urls in per_machine.items()})

@app.route('/api/monitor')
def api_monitor():
    snaps, q_rtts, f_rtts, probes, fails = collector.snapshot()

    lpm = {
        '1m': _lpm(snaps, 60),
        '5m': _lpm(snaps, 300),
        '10m': _lpm(snaps, 600),
        'overall': 0.0,
    }
    if snaps:
        elapsed_min = (time.time() - collector.start_time) / 60
        lpm['overall'] = round((snaps[-1]['visited'] - collector.start_visited) / elapsed_min, 1) if elapsed_min > 0 else 0.0

    latest = snaps[-1] if snaps else {}

    try:
        in_db = get_db().execute('SELECT COUNT(*) as c FROM extracted_content').fetchone()['c']
    except Exception:
        in_db = 0

    crawler_counts, timeline = _parse_grpc_links()
    per_crawler_lpm = _per_crawler_lpm(timeline, 60)

    q_stats = _rtt_stats(q_rtts, 60)
    f_stats = _rtt_stats(f_rtts, 60)
    avail = ((probes - fails) / max(probes, 1)) * 100

    mac_entry = {
        'crawler_id': 'crawler-mac', 'is_local': True,
        'last_seen': time.time(),
        'queue_rtt': {'current': q_stats['current'], 'avg': q_stats['avg'], 'p95': q_stats['p95'], 'p99': q_stats['p99'], 'min': q_stats['min'], 'max': q_stats['max']},
        'file_rtt': {'current': f_stats['current'], 'avg': f_stats['avg'], 'p95': f_stats['p95'], 'min': f_stats['min'], 'max': f_stats['max']},
        'uptime_sec': time.time() - collector.start_time,
        'lpm_crawled': lpm.get('1m', 0.0),
        'lpm_stored': _per_crawler_lpm(timeline, 60).get('crawler-mac', 0.0),
        'total_crawled': latest.get('visited', 0),
        'total_stored': crawler_counts.get('crawler-mac', 0),
    }

    with _machine_lock:
        remote = list(_machine_metrics.values())

    all_machines = [mac_entry] + [r for r in remote if r['crawler_id'] != 'crawler-mac']

    reported_lpm = {'crawler-mac': mac_entry['lpm_crawled']}
    with _machine_lock:
        for cid, m in _machine_metrics.items():
            if cid != 'crawler-mac':
                reported_lpm[cid] = m.get('lpm_crawled', 0.0)

    return jsonify({
        'lpm': lpm,
        'lpm_spark': _sparkline([s['visited'] for s in snaps], 40),
        'queue_size': latest.get('queue_size', 0),
        'visited': latest.get('visited', 0),
        'in_db': in_db,
        'uptime_sec': time.time() - collector.start_time,
        'rtt': {'queue': mac_entry['queue_rtt'], 'file': mac_entry['file_rtt']},
        'q_rtt_spark': _sparkline(q_rtts, 40),
        'f_rtt_spark': _sparkline(f_rtts, 40),
        'availability': round(avail, 1),
        'machines': all_machines,
        'crawler_counts': crawler_counts,
        'per_crawler_lpm': reported_lpm,
    })

@app.route('/api/links')
def api_links():
    cur = get_db().execute('SELECT url,score,title,extracted_at FROM extracted_content ORDER BY score DESC')
    return jsonify([{
        'url': r['url'], 'score': round(r['score'] or 0, 2),
        'title': r['title'] or r['url'], 'extracted_at': r['extracted_at'],
    } for r in cur.fetchall()])

@app.route('/api/content')
def api_content():
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Missing url'}), 400
    row = get_db().execute('SELECT text_content FROM extracted_content WHERE url=?', (url,)).fetchone()
    return jsonify({'text': row['text_content']}) if row else (jsonify({'text': 'Not yet extracted.'}), 404)

@app.route('/api/summarize')
def api_summarize():
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'Missing url parameter'}), 400

    if not OPENROUTER_API_KEY:
        return jsonify({'error': 'OpenRouter API key not configured.'}), 500

    row = get_db().execute('SELECT text_content, title FROM extracted_content WHERE url=?', (url,)).fetchone()
    if not row or not row['text_content']:
        return jsonify({'error': 'No scraped content found for this URL.'}), 404

    text = row['text_content'][:6000]
    title = row['title'] or url

    try:
        resp = http_requests.post(
            'https://openrouter.ai/api/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                'Content-Type': 'application/json',
                'HTTP-Referer': 'http://localhost:5000',
                'X-Title': 'System Design Crawler',
            },
            json={
                'model': 'openrouter/free',
                'max_tokens': 600,
                'messages': [
                    {'role': 'system', 'content': 'You are a concise technical assistant. Summarise the supplied system-design content in 4–6 bullet points, each one sentence. Highlight key technologies, patterns, and takeaways.'},
                    {'role': 'user', 'content': f'Summarise this page titled "{title}":\n\n{text}'},
                ],
            },
            timeout=45,
        )
        data = resp.json()
        if 'error' in data:
            msg = data['error'] if isinstance(data['error'], str) else data['error'].get('message', 'Unknown API error')
            return jsonify({'error': msg}), 500
        summary = data['choices'][0]['message']['content']
        return jsonify({'summary': summary})
    except http_requests.exceptions.Timeout:
        return jsonify({'error': 'OpenRouter request timed out (>45 s). Try again.'}), 504
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500

@app.route('/api/stats')
def api_stats():
    row = get_db().execute('SELECT COUNT(*) as total, AVG(score) as avg_score FROM extracted_content').fetchone()
    return jsonify({'total_links': row['total'], 'avg_score': round(row['avg_score'] or 0, 2)})

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=False)