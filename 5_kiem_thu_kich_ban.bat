@echo off
cd /d "%~dp0"
echo === CANH BAO: lenh nay XOA SACH du lieu (can 2_run_api.bat dang chay). Bam Ctrl+C de huy. ===
pause
for %%k in (ratelimit dedup multi burst) do (
  echo.
  echo ---------- Kich ban: %%k ----------
  docker exec errlog-redis redis-cli FLUSHALL
  for %%t in (error_logs error_logs_by_device error_logs_by_hash error_groups error_group_counter error_trend_daily) do docker exec errlog-cassandra cqlsh -e "TRUNCATE error_collector.%%t"
  .venv311\Scripts\python.exe simulate.py %%k
)
echo.
echo XONG. Chep dong ket qua cua tung kich ban.
pause
