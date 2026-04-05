@echo off
chcp 65001 >nul 2>&1
SET PYTHONIOENCODING=utf-8
SET PYTHONUTF8=1

SET PROJECT_DIR=E:\KLTN\12_22110190_22110243_XayDungWebsiteBanSachTichHopHeThongGoiY\source-code\BE_py\book-platform\recommendation-service
SET VENV=E:\KLTN\12_22110190_22110243_XayDungWebsiteBanSachTichHopHeThongGoiY\source-code\BE_py\book-platform\.venv\Scripts\activate.bat
SET LOG=%PROJECT_DIR%\logs\daily_cf_latest.log

cd /d %PROJECT_DIR%
call %VENV%

SET T_START=%time%
FOR /F "tokens=*" %%G IN ('powershell -Command "[int](Get-Date).TimeOfDay.TotalSeconds"') DO SET SEC_START=%%G
echo ============================================= > %LOG%
echo [%date% %T_START%] START CF RETRAIN >> %LOG%
echo ============================================= >> %LOG%

REM --- Step 1: CF_IMPLICIT ---
SET T1_START=%time%
echo [%T1_START%] Step 1: build_cf_implicit... >> %LOG%
python -m recommend_app.services.collab_filtering.build_cf_implicit >> %LOG% 2>&1
echo [%time%] Step 1 done (start: %T1_START% / end: %time%). >> %LOG%

REM --- Step 2: CF_PURCHASE ---
SET T2_START=%time%
echo [%T2_START%] Step 2: build_cf_purchase... >> %LOG%
python -m recommend_app.services.collab_filtering.build_cf_purchase >> %LOG% 2>&1
echo [%time%] Step 2 done (start: %T2_START% / end: %time%). >> %LOG%

REM --- Step 3: User CF Batch ---
SET T3_START=%time%
echo [%T3_START%] Step 3: rebuild_user_cf_batch... >> %LOG%
python -m recommend_app.services.collab_filtering.rebuild_user_cf_batch 30 90 50 >> %LOG% 2>&1
echo [%time%] Step 3 done (start: %T3_START% / end: %time%). >> %LOG%

FOR /F "tokens=*" %%G IN ('powershell -Command "[int](Get-Date).TimeOfDay.TotalSeconds"') DO SET SEC_END=%%G
SET /A ELAPSED=%SEC_END%-%SEC_START%
SET /A ELAPSED_MIN=%ELAPSED%/60
SET /A ELAPSED_SEC=%ELAPSED%%%60

echo ============================================= >> %LOG%
echo TOTAL TIME: %ELAPSED% seconds (%ELAPSED_MIN%m %ELAPSED_SEC%s) >> %LOG%
echo TOTAL: start=%T_START% / finish=%time% >> %LOG%
echo [%time%] ALL DONE. >> %LOG%
echo ============================================= >> %LOG%
