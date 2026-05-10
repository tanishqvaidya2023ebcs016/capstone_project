@echo off
REM ================================================================
REM  Shivam's Windows — Crawler
REM
REM  BEFORE RUNNING:
REM  1. Ask Tanishq to run on his Mac:   tailscale ip -4
REM  2. Paste that IP below (MAC_IP)
REM  3. Double-click this file to start
REM ================================================================

SET MAC_IP=100.109.122.26

REM ---- Do not edit below this line ----
SET QUEUE_SERVER=%MAC_IP%:50051
SET FILE_SERVER=%MAC_IP%:50052
SET DASHBOARD_URL=http://%MAC_IP%:8080
SET CRAWLER_ID=crawler-windows
SET MAX_URLS=500
SET CRAWLER_WORKERS=3
SET SEED_URLS=false

echo.
echo ================================================================
echo  Machine   : Shivam's Windows
echo  Queue     : %QUEUE_SERVER%
echo  File      : %FILE_SERVER%
echo  Dashboard : %DASHBOARD_URL%
echo  ID        : %CRAWLER_ID%
echo ================================================================
echo.

python crawler.py
pause