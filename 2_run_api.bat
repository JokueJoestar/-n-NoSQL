@echo off
cd /d "%~dp0"
echo === API dang chay tai http://127.0.0.1:8000 (de cua so nay mo, Ctrl+C de dung) ===
.venv311\Scripts\python.exe -m uvicorn app.main:app --port 8000
pause
