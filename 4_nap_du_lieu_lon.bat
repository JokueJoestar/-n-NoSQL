@echo off
cd /d "%~dp0"
echo === Nap 500.000 log vao Cassandra (xoa du lieu cu). Doi den khi hien dong XONG ===
.venv311\Scripts\python.exe bulk_seed.py 500000 --reset
echo.
echo === Dem so dong tung bang ===
.venv311\Scripts\python.exe bulk_seed.py --verify
echo.
echo Chup man hinh ca hai phan o tren.
pause
