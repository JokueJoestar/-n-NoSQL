@echo off
cd /d "%~dp0"
echo === Nap 300 log qua API (can 2_run_api.bat dang chay) ===
.venv311\Scripts\python.exe seed.py
.venv311\Scripts\python.exe worker.py --once
echo.
echo XONG. Mo http://127.0.0.1:8000 de xem.
pause
