#!/bin/bash
# ============================================================
# Oracle VM — Redis Replica + Sentinel + gRPC server standby
# ============================================================

set -e

MAC_IP="100.109.122.25"
ORACLE_IP="$(tailscale ip -4 2>/dev/null || echo '100.78.1.11')"
WINDOWS_IP="100.110.129.23"   # ← Change this to your Windows Tailscale IP!

echo "Mac IP    : $MAC_IP"
echo "Oracle IP : $ORACLE_IP"
echo "Windows IP: $WINDOWS_IP"
echo ""

# ---- Install Redis and Sentinel if missing ----
if ! command -v redis-server &>/dev/null; then
    echo "Installing Redis..."
    sudo apt-get update -y
    sudo apt-get install -y redis-server
fi

if ! command -v redis-sentinel &>/dev/null; then
    echo "Installing redis-sentinel..."
    sudo apt-get install -y redis-sentinel
fi

# ---- Create directories (Using absolute paths) ----
mkdir -p $HOME/redis/{conf,logs,data}
sudo chown -R $USER:$USER $HOME/redis

# ---- Write Redis replica config ----
cat > $HOME/redis/conf/redis.conf <<EOF
bind 0.0.0.0
protected-mode no
port 6379
replicaof $MAC_IP 6379
replica-read-only yes
replica-serve-stale-data yes
appendonly yes
appendfsync everysec
save 900 1
save 300 10
repl-timeout 60
loglevel notice
logfile $HOME/redis/logs/redis-replica.log
dir $HOME/redis/data
EOF

# ---- Write Sentinel config ----
cat > $HOME/redis/conf/sentinel.conf <<EOF
port 26379
sentinel announce-ip $ORACLE_IP
sentinel announce-port 26379
sentinel monitor mymaster $MAC_IP 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 30000
sentinel parallel-syncs mymaster 1
loglevel notice
logfile $HOME/redis/logs/sentinel.log
dir $HOME/redis/data
EOF

# ---- Stop any existing Redis processes ----
pkill -f "redis-server.*redis.conf" 2>/dev/null || true
pkill -f "redis-sentinel.*sentinel.conf" 2>/dev/null || true
sleep 1

# ---- Start Redis replica ----
echo "Starting Redis replica..."
redis-server $HOME/redis/conf/redis.conf --daemonize yes
sleep 2

if redis-cli ping | grep -q PONG; then
    ROLE=$(redis-cli info replication | grep "^role:" | tr -d '\r')
    echo "Redis status: $ROLE"
else
    echo "❌ Redis did not start. Check: tail -f $HOME/redis/logs/redis-replica.log"
    exit 1
fi

# ---- Start Sentinel ----
echo "Starting Sentinel..."
redis-sentinel $HOME/redis/conf/sentinel.conf --daemonize yes
sleep 2
echo "✅ Sentinel started on port 26379"

# ---- Start grpc_server.py (standby) ----
echo "Starting grpc_server.py standby..."
screen -dmS grpc-standby bash -c "
    cd $HOME && \
    export REDIS_HOST=localhost REDIS_PORT=6379 GRPC_PORT=50051 \
    SENTINEL_HOSTS='${MAC_IP}:26379,${ORACLE_IP}:26379,${WINDOWS_IP}:26379' \
    SENTINEL_MASTER_NAME=mymaster \
    python3 grpc_server.py 2>&1 | tee $HOME/redis/logs/grpc_server.log
"
sleep 2
echo "✅ grpc_server.py running (screen -r grpc-standby)"

# ---- Write the final run.sh ----
cat > $HOME/run.sh <<'EOF'
#!/bin/bash
MAC_IP="100.109.122.25"
ORACLE_IP="100.78.1.11"

export QUEUE_SERVERS="${MAC_IP}:50051,${ORACLE_IP}:50051"
export FILE_SERVER="${MAC_IP}:50052"
export DASHBOARD_URL="http://${MAC_IP}:8080"
export CRAWLER_ID="crawler-oracle"
export MAX_URLS="500"
export CRAWLER_WORKERS="3"
export SEED_URLS="false"

echo ""
echo "================================================================"
echo " Queue : $QUEUE_SERVERS  (Mac first, Oracle fallback)"
echo " File  : $FILE_SERVER"
echo "================================================================"
echo ""
python3 crawler.py
EOF
chmod +x $HOME/run.sh

echo ""
echo "================================================================"
echo "✅ Setup complete!"
echo ""
echo "To start the crawler right now, run:"
echo "  bash $HOME/run.sh"
echo ""
echo "To run it in the background (so it survives SSH disconnect):"
echo "  screen -dmS crawler bash $HOME/run.sh"
echo ""
echo "Check logs if anything fails:"
echo "  tail -f $HOME/redis/logs/redis-replica.log"
echo "  tail -f $HOME/redis/logs/sentinel.log"
echo "  tail -f $HOME/redis/logs/grpc_server.log"
echo "================================================================"
