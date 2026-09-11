@echo off
setlocal EnableExtensions

rem Clean all Python bytecode cache directories under the project directory.
set "PROJECT_DIR=%~dp0.."

pushd "%PROJECT_DIR%" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Unable to access project directory: "%PROJECT_DIR%"
    exit /b 1
)

set "FOUND=0"
for /d /r %%D in (__pycache__) do (
    if exist "%%D" (
        set "FOUND=1"
        echo Removing "%%D"
        rd /s /q "%%D"
    )
)

popd

if "%FOUND%"=="0" (
    echo No __pycache__ directories found under project.
) else (
    echo Project __pycache__ cleanup complete.
)

endlocal
exit /b 0
