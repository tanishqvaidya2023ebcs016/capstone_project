@echo off
REM ================================================================
REM  Shivam's Windows — Crawler (with HA failover)
REM ================================================================

SET MAC_IP=100.109.122.26

REM ---- Do not edit below this line ----
SET QUEUE_SERVERS=%MAC_IP%:50051,127.0.0.1:50051, 100.78.1.11:50051
SET FILE_SERVER=%MAC_IP%:50052
SET DASHBOARD_URL=http://%MAC_IP%:8080
SET CRAWLER_ID=crawler-windows
SET MAX_URLS=500
SET CRAWLER_WORKERS=3
SET SEED_URLS=false

echo.
echo ================================================================
echo  Machine   : Shivam's Windows
echo  Queue     : %QUEUE_SERVERS%
echo  File      : %FILE_SERVER%
echo  Dashboard : %DASHBOARD_URL%
echo  ID        : %CRAWLER_ID%
echo ================================================================
echo.

python crawler.py
pause