@echo off
chcp 65001 > nul
echo === Oracle and GaussDB Data Consistency Checker ===

REM Enable delayed variable expansion
setlocal enabledelayedexpansion

REM Get configuration file parameter (default to config.yml)
set CONFIG_FILE=config.yml
if not "%1"=="" (
    set CONFIG_FILE=%1
)
echo Using config file: %CONFIG_FILE%

REM Check Java environment
java -version > nul 2>&1
if errorlevel 1 (
    echo Error: Java environment not found, please ensure JDK 1.8 is installed and configured in PATH
    pause
    exit /b 1
)

echo Using Java version:
java -version

REM Check if compiled class file exists
if not exist "DataConsistencyChecker.class" (
    echo Error: Compiled class file not found, please run build.bat first
    pause
    exit /b 1
)

REM Check if config file exists
if not exist "%CONFIG_FILE%" (
    echo Error: %CONFIG_FILE% not found, please create and configure database connection information first
    pause
    exit /b 1
)

REM Build classpath, include current directory and all jar files in lib directory
set CLASSPATH=.

REM Check if lib directory exists
if not exist "lib" (
    echo Error: lib directory not found
    pause
    exit /b 1
)

REM Add all jar files in lib directory
for %%f in (lib\*.jar) do (
    set CLASSPATH=!CLASSPATH!;%%f
    echo Found dependency library: %%f
)

REM Create reports directory
if not exist "reports" (
    mkdir reports
    echo Creating reports directory
)

echo.
echo Starting data consistency checker...
echo Classpath: !CLASSPATH!
echo Config file: %CONFIG_FILE%
echo.

REM Run Java program
java -cp "!CLASSPATH!" -Dfile.encoding=UTF-8 DataConsistencyChecker "%CONFIG_FILE%"

if errorlevel 1 (
    echo Program execution failed with exit code: %errorlevel%
    pause
    exit /b 1
)

echo.
echo Program execution completed
echo Please check report files in reports directory

REM List latest report files
echo.
echo Latest generated report files:
for /f "delims=" %%f in ('dir /b /o-d reports\*.txt 2^>nul') do (
    echo   - %%f
    goto :found
)
echo   (No report files found)
:found

echo.
echo Execution script completed
