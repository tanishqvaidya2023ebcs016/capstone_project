# 🕸️ CrawlerX – Distributed System Design Crawler

**A fault-tolerant, distributed web crawler that autonomously discovers, ranks, and curates high-quality system-design resources from across the web.**

---

## 📌 Overview

CrawlerX is a distributed system built to solve the problem of information overload in system-design learning. It scans the web, filters out noise, scores pages using a multi-dimensional ranking algorithm, and provides a curated, searchable knowledge base via a live dashboard.

- **3-Machine Cluster** (Mac, Oracle Cloud VM, Windows) over a secure Tailscale mesh
- **Automatic Failover** using Redis Sentinel (3-node quorum)
- **Intelligent Scoring** – Source Authority + Keyword Relevance + Freshness + Content Length
- **Real-Time Dashboard** – LPM, RTT, queue stats, and searchable ranked links
- **Scalable** – Add new worker nodes in minutes

---

## 🚀 Tech Stack

| Layer | Technology |
| :--- | :--- |
| **Language** | Python 3.12+ |
| **Communication** | gRPC / Protobuf |
| **Queue & Dedup** | Redis (with Sentinel) |
| **Scraping/Extraction** | `requests`, `BeautifulSoup`, `dateparser` |
| **Database** | SQLite (for extracted content) |
| **Web Server** | Flask (Dashboard) |
| **Networking** | Tailscale (VPN) |
| **Containerization** | Docker & Docker Compose |

---

## 📂 Repository Structure

```
├── crawler.py                    # Main crawler worker
├── grpc_server.py                # Queue Server (Redis-backed)
├── file_server.py                # File Server (stores links.txt)
├── text_extractor_worker.py      # Extracts clean content to SQLite
├── web_server.py                 # Flask Dashboard
├── crawler.proto                 # Protobuf definition
├── generated/                    # Generated gRPC stubs
├── docker-compose.yml            # Mac stack configuration
├── docker-compose-windows.yml    # Windows worker configuration
├── run.bat                       # Windows crawler launcher
├── oracle_setup_redis.sh         # Oracle VM setup script
├── windows_run_sentinel.sh       # Windows Sentinel script
├── requirement.txt               # Python dependencies
└── tests/                        # Test suite
```

---

## 🏗️ System Architecture

```
┌──────────────────────┐        ┌──────────────────────┐
│   Mac (Primary)      │        │   Oracle VM (Backup) │
│  ┌────────────────┐  │        │  ┌────────────────┐  │
│  │ Redis Master   │◄─┼────────┼──│ Redis Replica  │  │
│  │ gRPC Queue Svc │  │        │  │ Sentinel       │  │
│  │ File Server    │  │        │  │ Standby Queue  │  │
│  │ Flask Dashboard│  │        │  └────────────────┘  │
│  │ Text Extractor │  │        └──────────┬───────────┘
│  └────────────────┘  │                   │
└──────────┬───────────┘                   │
           │         Tailscale VPN Mesh    │
           └───────────────────────────────┤
                                           │
┌──────────────────────┐                   │
│   Windows (Worker)   │                   │
│  ┌────────────────┐  │                   │
│  │ Crawler Worker │  │◄──────────────────┘
│  │ Sentinel       │  │
│  └────────────────┘  │
└──────────────────────┘
```

---

## 📊 Key Algorithms

### 1. Atomic Queue Dequeue (Redis Lua Script)
```lua
local url = redis.call('SPOP', KEYS[1])
if url then
    redis.call('SADD', KEYS[2], url)
    return url
end
return nil
```
Prevents race conditions when multiple crawlers fetch URLs.

### 2. Resilient Failover (Client-Side)
```python
class ResilientQueueStub:
    FAILOVER_THRESHOLD = 3
    
    def call_grpc(self, method):
        try:
            result = method()
            self.fail_count = 0
            return result
        except RpcError:
            self.fail_count += 1
            if self.fail_count >= self.FAILOVER_THRESHOLD:
                self.active = (self.active + 1) % len(self.servers)
                self.fail_count = 0
            raise
```

### 3. Multi-Dimensional Scoring
```
Final_Score = (3.0 × SourceAuthority) + (2.5 × Relevance) + (2.0 × Freshness) + LengthBonus
```
- **SourceAuthority:** Pre-defined trust (bytebytego.com = 1.0)
- **Relevance:** Weighted keywords (e.g., "sharding" = 5) normalized via `1 - e^(-0.02 × weight)`
- **Freshness:** Exponential decay with 2-year half-life
- **LengthBonus:** -0.5 to +0.5 based on content length

---

## 🖥️ User Manual

### 1. Accessing the Dashboard
Open your browser and navigate to `http://<Mac_IP>:8080`

**Main Tabs:**
- **Rankings:** Searchable, ranked list of all crawled links
- **Live Monitor:** Real-time performance metrics

### 2. Using the Rankings Tab
- **Search:** Type keywords (e.g., "sharding", "Kafka") – filters instantly
- **Open Resource:** Click any URL to visit the original article
- **View Summary:** Some entries have AI-generated summaries

### 3. Using the Live Monitor Tab
- **LPM (Links Per Minute):** How fast the system is crawling
- **RTT (Round-Trip Time):** gRPC latency between machines
- **Queue Status:** Total URLs pending and visited
- **Availability:** 100% when healthy

### 4. Stopping the System
```bash
docker-compose down
```
This stops all services gracefully.

---

## 🔧 Installation Guide

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Tailscale (installed & authenticated)
- Redis (installed locally or via Docker)

### Step 1: Join Tailscale Network
```bash
# On all machines
tailscale up
tailscale ip -4   # Note your machine's IP
```

### Step 2: Set Up Mac (Primary Master)
```bash
git clone <repository_url>
cd capstone_project
docker-compose up -d
```
This starts Redis, Queue Server, File Server, Dashboard, and Text Extractor.

### Step 3: Set Up Oracle VM (Backup Master)
```bash
# Configure Redis as replica
redis-server --replicaof <Mac_IP> 6379

# Start Sentinel
bash oracle_setup_redis.sh
```

### Step 4: Set Up Windows Machine (Crawler Worker)
```batch
:: Edit run.bat
SET QUEUE_SERVERS=<Mac_IP>:50051,<Oracle_IP>:50051
SET FILE_SERVER=<Mac_IP>:50052
SET DASHBOARD_URL=http://<Mac_IP>:8080
SET CRAWLER_ID=crawler-windows

:: Run crawler
python crawler.py
```

### Step 5: Verify Installation
1. Check `links.txt` on Mac for entries from your machine
2. Visit dashboard – confirm your machine shows "ONLINE"

---

## ➕ How to Add New Worker Nodes

Adding a new crawler machine is **simple** and requires **no changes** to the existing cluster.

### Step 1: Join Tailscale Network
```bash
tailscale up
```

### Step 2: Set Up Python Environment
```bash
git clone <repository_url>
cd capstone_project
pip install -r requirement.txt
```

### Step 3: Create a Launcher Script
```bash
# On Linux/Mac
export QUEUE_SERVERS=<Mac_IP>:50051,<Oracle_IP>:50051
export FILE_SERVER=<Mac_IP>:50052
export DASHBOARD_URL=http://<Mac_IP>:8080
export CRAWLER_ID=crawler-new-node
python crawler.py
```

**On Windows:**
```batch
SET QUEUE_SERVERS=<Mac_IP>:50051,<Oracle_IP>:50051
SET FILE_SERVER=<Mac_IP>:50052
SET DASHBOARD_URL=http://<Mac_IP>:8080
SET CRAWLER_ID=crawler-new-node
python crawler.py
```

### Step 4: (Optional) Add as Sentinel
If you want the new node to participate in failover voting:
```bash
bash windows_run_sentinel.sh
```
(or manually configure a Sentinel instance)

### Step 5: Verify
- The dashboard should show your new machine as **ONLINE**
- Your machine will automatically start receiving URLs to crawl
- LPM will increase, scaling throughput linearly

---

## 📈 Performance Results

| Metric | Standalone MVP | Distributed Cluster |
| :--- | :--- | :--- |
| **1-Minute LPM** | 32.3 | **364.3 (Mac)** |
| **Overall LPM** | 43.1 | **288.3** |
| **Peak LPM** | 78.7 | **419.2** |
| **Availability** | - | **100%** |
| **Q-RTT (Mac)** | - | **3.9ms** |

---

## 🧪 Testing

Run all tests:
```bash
python -m pytest tests/ -v
```

**Test Files:**
- `test_url_utils.py` – URL filtering logic
- `test_scoring.py` – Scoring algorithm
- `test_resilient_stub.py` – Failover rotation
- `test_text_extractor.py` – Text extraction pipeline
- `test_grpc_servers.py` – gRPC service logic
- `test_web_server.py` – Flask dashboard endpoints

---

## 📄 License

This project is created for academic purposes at BITS Pilani.

---

## 👥 Team

- **Tanishq Vijay Vaidya** (2023EBCS016)
- **Shivam Chaudhary** (2023EBCS430)

**Guide:** Dharma Teja  
**Program:** BSc Computer Science (Online Mode), BITS Pilani  
**Academic Year:** 2025–2026

---

*Built with ❤️ using Python, gRPC, Redis, Tailscale, and Docker.*
