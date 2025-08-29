#!/bin/bash

# Oracle and GaussDB Data Consistency Checker Build Script

echo "=== Oracle and GaussDB Data Consistency Checker Build Script ==="

# Check Java environment
if ! command -v java &> /dev/null; then
    echo "Error: Java environment not found, please ensure JDK 1.8 is installed and configured in PATH"
    exit 1
fi

echo "Java environment detected"
java -version

# Check if source file exists
if [ ! -f "DataConsistencyChecker.java" ]; then
    echo "Error: Source file DataConsistencyChecker.java not found"
    exit 1
fi

# Check if lib directory exists
if [ ! -d "lib" ]; then
    echo "Error: lib directory not found"
    exit 1
fi

# Check if SnakeYAML library exists
SNAKEYAML_JAR="lib/snakeyaml-2.3.jar"
if [ ! -f "$SNAKEYAML_JAR" ]; then
    echo "Error: SnakeYAML dependency $SNAKEYAML_JAR not found"
    echo "Please place snakeyaml-2.3.jar in the lib directory"
    exit 1
fi

# Build classpath, include all jar files in lib directory
CLASSPATH="."
for jar in lib/*.jar; do
    if [ -f "$jar" ]; then
        CLASSPATH="$CLASSPATH:$jar"
    fi
done

echo ""
echo "Compiling DataConsistencyChecker.java..."

# Compile Java source file
if javac -cp "$CLASSPATH" -encoding UTF-8 DataConsistencyChecker.java; then
    echo "Compilation successful! Generated DataConsistencyChecker.class"
    
    # Check if necessary configuration files exist
    if [ ! -f "config.yml" ]; then
        echo "Warning: Configuration file config.yml not found, please configure database connection information first"
    fi
    
    echo ""
    echo "Compilation completed! Please ensure:"
    echo "1. Configuration file config.yml is properly configured"
    echo "2. Oracle and GaussDB JDBC driver jar files are placed in the lib directory"
    echo "3. Use ./run.sh command to run the program"
else
    echo "Compilation failed, please check if there are syntax errors in the source code"
    exit 1
fi

echo ""
echo "Build script execution completed"
