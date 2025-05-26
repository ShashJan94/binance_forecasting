@echo off
REM ===== Kafka/Zookeeper Clean Script =====

REM --- Clean Kafka logs and Zookeeper data in D: drive (keep folders, delete contents) ---
echo Cleaning Kafka logs in D:\tmp\kafka-logs ...
if exist "D:\tmp\kafka-logs" (
    del /f /q "D:\tmp\kafka-logs\*"
    for /d %%i in ("D:\tmp\kafka-logs\*") do rmdir /s /q "%%i"
)

echo Cleaning Zookeeper data in D:\tmp\zookeeper ...
if exist "D:\tmp\zookeeper" (
    del /f /q "D:\tmp\zookeeper\*"
    for /d %%i in ("D:\tmp\zookeeper\*") do rmdir /s /q "%%i"
)

REM --- (Optional) Clean Kafka logs in C: drive (keep folder, delete contents) ---
echo Cleaning Kafka logs in C:\kafka\logs ...
if exist "C:\kafka\logs" (
    del /f /q "C:\kafka\logs\*"
    for /d %%i in ("C:\kafka\logs\*") do rmdir /s /q "%%i"
)

REM --- You can add more log/data folders below if needed ---
REM echo Cleaning custom logs in C:\my_custom_logs ...
REM if exist "C:\my_custom_logs" (
REM     del /f /q "C:\my_custom_logs\*"
REM     for /d %%i in ("C:\my_custom_logs\*") do rmdir /s /q "%%i"
REM )

echo.
echo ====== All specified Kafka/Zookeeper log folders cleaned (folders kept)! ======
pause
