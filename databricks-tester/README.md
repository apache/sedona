<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Sedona Databricks Testing Framework

Automated testing tool for Apache Sedona on Databricks. Runs smoke tests to validate Sedona functionality.

## Prerequisites

1. **Databricks Workspace** with admin permissions
2. **Environment Variables**:

   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-personal-access-token"
   # Volume path for storing test JARs and init script
   export DATABRICKS_VOLUME_PATH="/Volumes/your-catalog/your-schema/your-volume"
   # Workspace path for storing test scripts
   export DATABRICKS_WORKSPACE_PATH="/Workspace/Shared/your-path"
   ```

## Installation

1. Install uv (Python package manager):

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Clone and install:

   ```bash
   git clone <repository-url>
   cd <repo-name>/databricks-tester
   uv sync
   ```

3. Create `.env` file (optional, only if you haven't exported the environment variables):

   ```bash
   echo "DATABRICKS_HOST=https://your-workspace.cloud.databricks.com" > .env
   echo "DATABRICKS_TOKEN=your-token" >> .env
   echo "DATABRICKS_VOLUME_PATH=/Volumes/your-catalog/your-schema/your-volume" >> .env
   echo "DATABRICKS_WORKSPACE_PATH=/Workspace/Shared/your-path" >> .env
   ```

## Usage

### Run Smoke Tests

```bash
# Run smoke tests (only supported runtime currently)
uv run python -m sedona_databricks_tester smoke-test --runtime 16.4.x-scala2.12 --session-id my-test --jar /path/to/sedona.jar --jar /path/to/geotools.jar

# With cleanup disabled (for debugging)
uv run python -m sedona_databricks_tester smoke-test --runtime 16.4.x-scala2.12 --session-id my-test --jar /path/to/sedona.jar --jar /path/to/geotools.jar --no-cleanup

# Override paths via global options (instead of environment variables)
uv run python -m sedona_databricks_tester --volume-path /Volumes/my/custom/path --workspace-path /Workspace/Shared/my/path smoke-test --runtime 16.4.x-scala2.12 --session-id my-test --jar /path/to/sedona.jar --jar /path/to/geotools.jar

# Multiple JARs can be specified (useful for additional dependencies)
uv run python -m sedona_databricks_tester smoke-test --runtime 16.4.x-scala2.12 --session-id my-test --jar /path/to/sedona.jar --jar /path/to/geotools.jar --jar /path/to/additional.jar
```

### List Available Runtimes

```bash
# Show all supported Databricks runtime versions
uv run python -m sedona_databricks_tester list-runtimes
```

### Create Cluster Only

```bash
# Create/reuse a cluster without running tests (useful for debugging)
uv run python -m sedona_databricks_tester create-cluster --runtime 16.4.x-scala2.12 --session-id my-debug --jar /path/to/sedona.jar --jar /path/to/geotools.jar
```

### Test Connection

```bash
# Test connection to Databricks workspace
uv run python -m sedona_databricks_tester test-connection
```

### Cleanup Resources

```bash
# Clean up resources for a specific session
uv run python -m sedona_databricks_tester cleanup --session-id my-test

# Clean up ALL sedona-test resources (clusters, files, etc.)
uv run python -m sedona_databricks_tester cleanup --all
```

### Common Workflows

#### Full Test Run

```bash
# 1. Test connection
uv run python -m sedona_databricks_tester test-connection

# 2. Run smoke tests
uv run python -m sedona_databricks_tester smoke-test --runtime 16.4.x-scala2.12 --session-id prod-test --jar /path/to/sedona.jar --jar /path/to/geotools.jar

# Resources are automatically cleaned up after test completion
```

#### Development/Debugging Workflow

```bash
# 1. Create cluster for debugging (keeps resources alive)
uv run python -m sedona_databricks_tester create-cluster --runtime 16.4.x-scala2.12 --session-id debug-session --jar /path/to/sedona.jar --jar /path/to/geotools.jar

# 2. Run tests with no cleanup to inspect results
uv run python -m sedona_databricks_tester smoke-test --runtime 16.4.x-scala2.12 --session-id debug-session --jar /path/to/sedona.jar --jar /path/to/geotools.jar --no-cleanup

# 3. Clean up when done debugging
uv run python -m sedona_databricks_tester cleanup --session-id debug-session
```

#### Mass Cleanup

```bash
# Clean up all sedona-test resources across all sessions
uv run python -m sedona_databricks_tester cleanup --all
```

### What the smoke-test command does

1. **Uploads JARs** to Databricks Volumes
2. **Creates/reuses cluster** with Sedona configuration
3. **Uploads test data files** to volume
4. **Uploads smoke test script** to workspace (smoke-tests/data)
5. **Runs tests** (smoke-tests/smoke_test.py)
6. **Shows test output**
7. **Cleans up resources** (unless `--no-cleanup` specified)

### What the cleanup command does

- **Session cleanup** (`--session-id`): Removes clusters, volume files, and workspace files for a specific session
- **Full cleanup** (`--all`): Removes ALL sedona-test clusters and files across all sessions
- **Interactive confirmation**: Asks for confirmation before deleting clusters

## Supported Runtimes

See config/cluster_configs.json for details.
