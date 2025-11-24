#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# Set up environment variables if not already set
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3}
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python3}
# Add py4j to PYTHONPATH (it's in Spark's python/lib directory)
PY4J_ZIP=$(find "$SPARK_HOME/python/lib" -name "py4j-*.zip" | head -1)
if [ -n "$PY4J_ZIP" ]; then
    export PYTHONPATH=${SPARK_HOME}/python:${PY4J_ZIP}:${PYTHONPATH}
else
    export PYTHONPATH=${SPARK_HOME}/python:${PYTHONPATH}
fi

# Configure Spark to run in local mode (no cluster needed for testing)
export SPARK_MASTER=${SPARK_MASTER:-local[*]}

EXAMPLES_DIR="/opt/workspace/examples"
FAILED_TESTS=0
PASSED_TESTS=0

echo "========================================="
echo "Testing Jupyter Notebooks"
echo "========================================="

# Check if examples directory exists
if [ ! -d "$EXAMPLES_DIR" ]; then
    echo "Error: Examples directory $EXAMPLES_DIR does not exist"
    exit 1
fi

# Find all .ipynb files only in the root level (exclude subdirectories like contrib)
NOTEBOOK_FILES=$(find "$EXAMPLES_DIR" -maxdepth 1 -name "*.ipynb" -type f)

if [ -z "$NOTEBOOK_FILES" ]; then
    echo "No .ipynb files found in $EXAMPLES_DIR (root level only)"
    exit 0
fi

NOTEBOOK_COUNT=$(echo "$NOTEBOOK_FILES" | wc -l)
echo "Found $NOTEBOOK_COUNT notebook file(s) to test (root level only, excluding subdirectories)"
echo ""

# Convert and test each notebook
for NOTEBOOK in $NOTEBOOK_FILES; do
    NOTEBOOK_NAME=$(basename "$NOTEBOOK" .ipynb)
    PYTHON_FILE="${NOTEBOOK%.ipynb}.py"

    echo "Testing: $NOTEBOOK_NAME"
    echo "  Converting notebook to Python..."

    # Convert notebook to Python using jupyter nbconvert
    if jupyter nbconvert --to python "$NOTEBOOK" --stdout > "$PYTHON_FILE" 2>/dev/null; then
        echo "  ✓ Converted to $PYTHON_FILE"
    else
        echo "  ✗ Failed to convert notebook"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        continue
    fi

    # Check if the Python file was created and has content
    if [ ! -s "$PYTHON_FILE" ]; then
        echo "  ✗ Converted Python file is empty"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        continue
    fi

    # Modify the Python file to use local mode instead of cluster mode
    # Replace common Spark master URLs with local[*]
    sed -i 's|\.master("spark://[^"]*")|.master("local[*]")|g' "$PYTHON_FILE"
    sed -i 's|\.master('"'"'spark://[^'"'"']*'"'"')|.master("local[*]")|g' "$PYTHON_FILE"
    # Also handle cases where master might be set via config
    sed -i 's|spark://localhost:7077|local[*]|g' "$PYTHON_FILE"

    # Fix data file paths - notebooks reference "data/" but files are in "examples/data/"
    # Replace paths like "data/file.csv" with "examples/data/file.csv"
    sed -i 's|"data/|"examples/data/|g' "$PYTHON_FILE"
    sed -i "s|'data/|'examples/data/|g" "$PYTHON_FILE"
    # Also handle absolute paths
    sed -i 's|/opt/workspace/data/|/opt/workspace/examples/data/|g' "$PYTHON_FILE"

    # Remove IPython magic commands that don't work in plain Python
    # Use Python to properly extract code from get_ipython().run_cell_magic() calls
    PYTHON_FILE_PATH="$PYTHON_FILE" python3 << 'PYTHON_CLEANUP'
import re
import os
import ast

file_path = os.environ['PYTHON_FILE_PATH']
with open(file_path, 'r') as f:
    lines = f.readlines()

output_lines = []
i = 0
while i < len(lines):
    line = lines[i]
    if 'get_ipython().run_cell_magic' in line:
        # Try to parse this line as Python code to extract the string argument
        try:
            # Use AST to parse the call and extract the third argument
            tree = ast.parse(line.strip())
            if isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Call):
                call = tree.body[0].value
                if (isinstance(call.func, ast.Attribute) and
                    isinstance(call.func.value, ast.Call) and
                    call.func.attr == 'run_cell_magic' and
                    len(call.args) >= 3):
                    # Third argument is the code string
                    code_node = call.args[2]
                    if isinstance(code_node, ast.Constant):
                        code = code_node.value
                    elif isinstance(code_node, ast.Str):  # Python < 3.8
                        code = code_node.s
                    else:
                        code = ast.literal_eval(code_node)
                    # Add the extracted code as separate lines
                    output_lines.append(code)
                    i += 1
                    continue
        except:
            # If AST parsing fails, try regex fallback
            # Match: run_cell_magic('...', '...', 'CODE')
            # This regex handles strings with escaped characters
            match = re.search(r"run_cell_magic\([^,]+,\s*[^,]+,\s*('(?:[^'\\\\]|\\\\.)*'|\"(?:[^\"\\\\]|\\\\.)*\")\)", line)
            if match:
                code_str = match.group(1)
                try:
                    code = ast.literal_eval(code_str)
                    output_lines.append(code)
                    i += 1
                    continue
                except:
                    pass
        # If all else fails, skip this line
        i += 1
        continue
    elif 'get_ipython()' in line:
        # Skip other get_ipython() calls
        i += 1
        continue
    else:
        output_lines.append(line)
        i += 1

content = ''.join(output_lines)

# Remove IPython cell markers
content = re.sub(r'# In\[\d+\]:\s*\n', '', content)

with open(file_path, 'w') as f:
    f.write(content)
PYTHON_CLEANUP

    # Validate Python syntax first
    echo "  Validating Python syntax..."
    if ! python3 -m py_compile "$PYTHON_FILE" >/dev/null 2>/dev/null; then
        echo "  ✗ Python syntax validation failed"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo ""
        continue
    fi

    # Run the full Python script with timeout (600 seconds = 10 minutes)
    echo "  Running Python script (600 second timeout)..."
    cd "$EXAMPLES_DIR/.."

    # Use timeout with progress reporting
    START_TIME=$(date +%s)
    if timeout 600 python3 "$PYTHON_FILE" | tee /tmp/notebook_output_$$.log; then
        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))
        echo "  ✓ Test passed (completed in ${ELAPSED}s)"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        EXIT_CODE=$?
        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))
        if [ $EXIT_CODE -eq 124 ]; then
            echo "  ✗ Test timed out (exceeded 600 seconds, ran for ${ELAPSED}s)"
            echo "  Last 20 lines of output:"
            tail -20 /tmp/notebook_output_$$.log 2>/dev/null || echo "  (no output captured)"
        else
            echo "  ✗ Test failed with exit code $EXIT_CODE (ran for ${ELAPSED}s)"
            echo "  Last 20 lines of output:"
            tail -20 /tmp/notebook_output_$$.log 2>/dev/null || echo "  (no output captured)"
        fi
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    rm -f /tmp/notebook_output_$$.log

    echo ""
done

echo "========================================="
echo "Test Summary"
echo "========================================="
echo "Passed: $PASSED_TESTS"
echo "Failed: $FAILED_TESTS"
echo "Total:  $((PASSED_TESTS + FAILED_TESTS))"
echo "========================================="

if [ $FAILED_TESTS -gt 0 ]; then
    echo "Some tests failed. Exiting with error code 1."
    exit 1
else
    echo "All tests passed!"
    exit 0
fi
