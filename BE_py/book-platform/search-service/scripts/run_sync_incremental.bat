@echo off
setlocal

REM ==== Config – cập nhật đường dẫn tương ứng với máy bạn ====
set PROJECT_DIR=D:\12_22110190_22110243_XayDungWebsiteBanSachTichHopHeThongGoiY\source-code\BE_py\book-platform\search-service
set VENV_PY=D:\12_22110190_22110243_XayDungWebsiteBanSachTichHopHeThongGoiY\source-code\BE_py\book-platform\.venv\Scripts\python.exe
set LOG_DIR=%PROJECT_DIR%\logs
set LOG_FILE=%LOG_DIR%\reindex_full.log

REM ==== Đảm bảo thư mục log tồn tại ====
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo ================================================== > "%LOG_FILE%"
echo [START] %date% %time% >> "%LOG_FILE%"

REM ==== Di chuyển vào thư mục project ====
cd /d "%PROJECT_DIR%"
if errorlevel 1 (
    echo [ERROR] Không cd được vào %PROJECT_DIR% >> "%LOG_FILE%"
    exit /b 1
)

REM ==== Kiểm tra Python venv ====
if not exist "%VENV_PY%" (
    echo [ERROR] Python venv không tìm thấy: %VENV_PY% >> "%LOG_FILE%"
    exit /b 1
)

REM ==== Kiểm tra OpenSearch ====
"C:\Windows\System32\curl.exe" -k -s https://localhost:9200 >nul 2>&1
if errorlevel 1 (
    echo [ERROR] OpenSearch đang DOWN – hủy reindex >> "%LOG_FILE%"
    exit /b 1
)

REM ==== Chạy Full Reindex (thủ công sau khi import dữ liệu lớn) ====
echo [RUN] Bat đang chạy full reindex... >> "%LOG_FILE%"
"%VENV_PY%" -m search_app.jobs.reindex_full >> "%LOG_FILE%" 2>&1

echo [END] %date% %time% >> "%LOG_FILE%"
endlocal
