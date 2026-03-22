@echo off
setlocal
cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"
set "BOOTSTRAP="

if exist "%VENV_PY%" goto install

python -V >nul 2>nul
if not errorlevel 1 (
    set "BOOTSTRAP=python"
) else (
    py -3 -V >nul 2>nul
    if not errorlevel 1 set "BOOTSTRAP=py -3"
)

if not defined BOOTSTRAP (
    echo Python 3.11+ not found.
    echo Install Python and run setup.bat again.
    pause
    exit /b 1
)

echo [1/4] Creating virtual environment...
call %BOOTSTRAP% -m venv ".venv"
if errorlevel 1 goto fail

:install
echo [2/4] Upgrading pip...
call "%VENV_PY%" -m pip install --upgrade pip
if errorlevel 1 goto fail

echo [3/4] Installing dependencies...
call "%VENV_PY%" -m pip install -r requirements.txt
if errorlevel 1 goto fail

if not exist ".env" (
    echo [4/4] Creating .env from .env.example...
    copy /Y ".env.example" ".env" >nul
) else (
    echo [4/4] .env already exists.
)

echo.
echo Setup finished.
echo Edit .env if needed, then run start.bat
pause
exit /b 0

:fail
echo.
echo Setup failed.
pause
exit /b 1
