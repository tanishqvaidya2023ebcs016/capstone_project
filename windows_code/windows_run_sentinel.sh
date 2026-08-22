#!/bin/bash
# ============================================================
# Windows Sentinel (WSL2/Git Bash) – 3rd quorum voter
# ============================================================

MAC_IP="100.109.122.25"
ORACLE_IP="100.78.1.11"    # ← fill in with Oracle's Tailscale IP
WINDOWS_IP=$(tailscale ip -4 2>/dev/null)
if [ -z "$WINDOWS_IP" ]; then
    echo "❌ Could not get Tailscale IP. Please set WINDOWS_IP manually."
    exit 1
fi

SENTINEL_CONF="/tmp/windows_sentinel.conf"

echo "Writing sentinel config..."
cat > $SENTINEL_CONF <<EOF
port 26379
sentinel announce-ip $WINDOWS_IP
sentinel announce-port 26379
sentinel monitor mymaster $MAC_IP 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 30000
sentinel parallel-syncs mymaster 1
sentinel resolve-hostnames no
sentinel announce-hostnames no
loglevel notice
logfile /tmp/sentinel_windows.log
dir /tmp
EOF

echo "Starting Sentinel on $WINDOWS_IP:26379..."
# Run in background with screen if available
if command -v screen &> /dev/null; then
    screen -dmS sentinel-windows bash -c "
        if command -v redis-sentinel &> /dev/null; then
            redis-sentinel $SENTINEL_CONF
        else
            redis-server $SENTINEL_CONF --sentinel
        fi
    "
    echo "✅ Sentinel started in screen session 'sentinel-windows'."
    echo "   To view logs: screen -r sentinel-windows"
else
    if command -v redis-sentinel &> /dev/null; then
        nohup redis-sentinel $SENTINEL_CONF > /tmp/sentinel_windows.out 2>&1 &
    else
        nohup redis-server $SENTINEL_CONF --sentinel > /tmp/sentinel_windows.out 2>&1 &
    fi
    echo "✅ Sentinel started in background. Logs: /tmp/sentinel_windows.out"
fi