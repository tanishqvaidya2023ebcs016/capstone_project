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
