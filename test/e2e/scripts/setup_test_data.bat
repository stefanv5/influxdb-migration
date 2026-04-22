@echo off
REM Data Setup Script for InfluxDB v1 E2E Tests (Windows)
REM This script prepares test data in the source InfluxDB instance

setlocal enabledelayedexpansion

set "SOURCE_URL=http://127.0.0.1:8084"
set "SOURCE_DB=test_source"

set "TIMESTAMP=2025-04-22T00:00:00Z"

echo [INFO] Checking prerequisites...
curl -s "%SOURCE_URL%/ping" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Cannot connect to source InfluxDB at %SOURCE_URL%
    exit /b 1
)
echo [INFO] Prerequisites OK

echo [INFO] Creating database %SOURCE_DB% if not exists...
curl -s -X POST "%SOURCE_URL%/query" --data-urlencode "q=CREATE DATABASE IF NOT EXISTS %SOURCE_DB%" >nul
echo [INFO] Database %SOURCE_DB% ready

echo [INFO] Writing TC-S-F01 test data (cpu measurement, 100 records)...

REM Generate line protocol data
set "lines="
set "base_ts=1745280000000000000"

for /L %%i in (1,1,100) do (
    set /a "offset=(%%i-1)*60*1000000000"
    set /a "ts=base_ts+offset"
    set /a "cpu_usage=!random!%%100"
    set /a "cpu_dec=!random!%%100"
    set "status=ok"
    if %%i equ 10 set "status=warning"
    if %%i equ 20 set "status=error"

    set "line=cpu,host=server-001,region=us-west cpu_usage=!cpu_usage!.!cpu_dec!,status=\"!status!\" !ts!"
    if %%i lss 100 set "lines=!lines!!line!\n"
    if %%i equ 100 set "lines=!lines!!line!"
)

REM Write to InfluxDB using PowerShell to handle the data
echo !lines! > "%TEMP%\influx_lines.txt"
for /f "tokens=*" %%a in ('type "%TEMP%\influx_lines.txt"') do (
    curl -s -X POST "%SOURCE_URL%/write?db=%SOURCE_DB%" -H "Content-Type: text/plain" --data-binary "%%a" >nul
)
del "%TEMP%\influx_lines.txt" 2>nul

echo [INFO] Successfully wrote 100 records to cpu measurement

echo [INFO] Verifying data...
for %%m in (cpu memory disk) do (
    curl -s -G "%SOURCE_URL%/query" --data-urlencode "db=%SOURCE_DB%" --data-urlencode "q=SELECT COUNT(*) FROM %%m" | findstr /C:"\"value\":" >nul 2>&1
    if errorlevel 1 (
        echo [INFO]   - %%m: 0 records
    ) else (
        echo [INFO]   - %%m: found
    )
)

echo [INFO] Data setup complete!
echo.
echo To run migration test:
echo   ./migrate run --config test/e2e/config_tc_s_f01.yaml
echo.
echo To cleanup:
echo   curl -s -X POST "%SOURCE_URL%/query" --data-urlencode "q=DROP MEASUREMENT cpu" --data-urlencode "db=%SOURCE_DB%"
