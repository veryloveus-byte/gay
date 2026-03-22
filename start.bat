@echo off
setlocal
cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"

if not exist "%VENV_PY%" (
    echo Virtual environment not found. Running setup.bat...
    call "%~dp0setup.bat"
    if errorlevel 1 (
        echo Setup did not finish successfully.
        pause
        exit /b 1
    )
)

if not exist ".env" (
    echo .env file not found.
    echo Run setup.bat first and fill in the settings.
    pause
    exit /b 1
)

echo Starting bot...
call "%VENV_PY%" ".\main.py"
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo Bot stopped with exit code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%
