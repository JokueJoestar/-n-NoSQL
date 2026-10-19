@echo off
cd /d "%~dp0"
echo === Khoi dong Cassandra va Redis ===
docker compose up -d
echo Dang doi Cassandra san sang (1-2 phut), dung dong cua so nay...
:wait
for /f %%s in ('docker inspect -f "{{.State.Health.Status}}" errlog-cassandra') do set ST=%%s
if not "%ST%"=="healthy" (
  timeout /t 5 /nobreak >nul
  goto wait
)
echo Cassandra da san sang. Tao bang...
.venv311\Scripts\python.exe init_db.py
echo.
echo XONG. Buoc tiep theo: bam dup 2_run_api.bat
pause
