#!/bin/bash

# Oracle and GaussDB Data Consistency Validation Tool Execution Script

echo "=== Oracle and GaussDB Data Consistency Checker ==="

# Get configuration file parameter (default to config.yml)
CONFIG_FILE="${1:-config.yml}"
echo "Using config file: $CONFIG_FILE"

# Check Java environment
if ! command -v java &> /dev/null; then
    echo "Error: Java environment not found, please ensure JDK 1.8 is installed and configured in PATH"
    exit 1
fi

echo "Using Java version:"
java -version

# Check if compiled class file exists
if [ ! -f "DataConsistencyChecker.class" ]; then
    echo "Error: Compiled class file not found, please run ./build.sh first"
    exit 1
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: $CONFIG_FILE not found, please create and configure database connection information first"
    exit 1
fi

# Check if lib directory exists
if [ ! -d "lib" ]; then
    echo "Error: lib directory not found"
    exit 1
fi

# Build classpath, include current directory and all jar files in lib directory
CLASSPATH="."

# Add all jar files in lib directory
for jar in lib/*.jar; do
    if [ -f "$jar" ]; then
        CLASSPATH="$CLASSPATH:$jar"
        echo "Found dependency library: $jar"
    fi
done

# Create reports directory
if [ ! -d "reports" ]; then
    mkdir -p reports
    echo "Creating reports directory"
fi

echo ""
echo "Starting data consistency checker..."
echo "Classpath: $CLASSPATH"
echo "Config file: $CONFIG_FILE"
echo ""

# Run Java program
if java -cp "$CLASSPATH" -Dfile.encoding=UTF-8 DataConsistencyChecker "$CONFIG_FILE"; then
    echo ""
    echo "Program execution completed"
    echo "Please check report files in reports directory"
    
    # List latest report files
    echo ""
    echo "Latest generated report files:"
    if ls reports/*.txt &> /dev/null; then
        ls -lt reports/*.txt | head -2 | while read line; do
            filename=$(echo "$line" | awk '{print $NF}')
            echo "  - $(basename "$filename")"
        done
    else
        echo "  (No report files found)"
    fi
else
    echo "Program execution failed with exit code: $?"
    exit 1
fi

echo ""
echo "Execution script completed"
