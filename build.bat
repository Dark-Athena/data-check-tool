@echo off
chcp 65001 > nul
echo === Oracle and GaussDB Data Consistency Checker - Build Script ===

REM Enable delayed variable expansion
setlocal enabledelayedexpansion

REM Check Java environment
java -version > nul 2>&1
if errorlevel 1 (
    echo Error: Java environment not found, please ensure JDK 1.8 is installed and configured in PATH
    pause
    exit /b 1
)

echo Java environment detected
java -version

REM Check if source file exists
if not exist "DataConsistencyChecker.java" (
    echo Error: Source file DataConsistencyChecker.java not found
    pause
    exit /b 1
)

REM Check if lib directory exists
if not exist "lib" (
    echo Error: lib directory not found
    pause
    exit /b 1
)

REM Check if SnakeYAML library exists
set SNAKEYAML_JAR=lib/snakeyaml-2.3.jar
if not exist "%SNAKEYAML_JAR%" (
    echo Error: SnakeYAML dependency %SNAKEYAML_JAR% not found
    echo Please put snakeyaml-2.3.jar in lib directory
    pause
    exit /b 1
)

REM Build classpath, include all jar files in lib directory
set CLASSPATH=.
for %%f in (lib\*.jar) do (
    set CLASSPATH=!CLASSPATH!;%%f
)

REM Compile Java source file
echo.
echo Compiling DataConsistencyChecker.java...
javac -cp "!CLASSPATH!" -encoding UTF-8 DataConsistencyChecker.java

if errorlevel 1 (
    echo Compilation failed, please check source code for syntax errors
    pause
    exit /b 1
)

echo Compilation successful! Generated DataConsistencyChecker.class

REM Check required files
if not exist "config.yml" (
    echo Warning: config.yml not found, please configure database connection information first
)

echo.
echo Compilation completed! Please ensure:
echo 1. Configuration file config.yml is properly set
echo 2. Oracle and GaussDB JDBC driver jar files are placed in lib directory
echo 3. Use run.bat command to run the program
echo.
echo Build script execution completed
