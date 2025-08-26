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

"""
Sedona Tester CLI
Main command-line interface for local testing and debugging of Databricks Sedona integration.
"""

import logging
import os
import sys
from typing import Optional

import click
from dotenv import load_dotenv

from .databricks_manager import DatabricksManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

CLUSTER_NAME_PREFIX = "sedona-smoke-test-"


def create_databricks_manager(
    ctx,
    session_id: Optional[str] = None,
    test_connection: bool = True,
):
    """Create a DatabricksManager instance with configuration from context and CLI options.

    Args:
        ctx: Click context containing configuration
        session_id: Optional session ID, will be generated if not provided
        test_connection: Whether to test the connection after creating the manager (default: True)

    Returns:
        DatabricksManager instance

    Raises:
        click.ClickException: If connection test fails (when test_connection=True)
    """
    config = ctx.obj.get("databricks_config", {})

    # All values are already validated in the CLI context
    host = config["host"]
    token = config["token"]

    # Generate session ID if not provided
    if not session_id:
        import uuid

        session_id = str(uuid.uuid4())[:8]

    # Create manager and store session info in context for later use
    manager = DatabricksManager(host=host, token=token)

    # Test connection if requested
    if test_connection:
        if not manager.test_connection():
            raise click.ClickException("Failed to connect to Databricks")

    # Store session-specific paths in context
    volume_base_path = config["volume_path"]
    workspace_base_path = config["workspace_path"]
    volume_path = f"{volume_base_path}/{session_id}"
    workspace_path = f"{workspace_base_path}/{session_id}"

    # Store session info in manager context (we'll pass these as parameters)
    ctx.obj["session_info"] = {
        "session_id": session_id,
        "volume_path": volume_path,
        "workspace_path": workspace_path,
    }

    return manager


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.option(
    "--host", help="Databricks workspace host (overrides DATABRICKS_HOST env var)"
)
@click.option(
    "--token", help="Databricks access token (overrides DATABRICKS_TOKEN env var)"
)
@click.option(
    "--volume-path",
    help="Base volume path for storing test assets (overrides DATABRICKS_VOLUME_PATH env var)",
)
@click.option(
    "--workspace-path",
    help="Base workspace path for storing files (overrides DATABRICKS_WORKSPACE_PATH env var)",
)
@click.pass_context
def cli(ctx, debug, host, token, volume_path, workspace_path):
    """Sedona Databricks Testing CLI"""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    # Resolve configuration values from CLI options or environment variables
    resolved_host = host or os.getenv("DATABRICKS_HOST")
    resolved_token = token or os.getenv("DATABRICKS_TOKEN")
    resolved_volume_path = volume_path or os.getenv("DATABRICKS_VOLUME_PATH")
    resolved_workspace_path = workspace_path or os.getenv("DATABRICKS_WORKSPACE_PATH")

    # Validate that all required configuration is available
    if not resolved_host or not resolved_token:
        raise click.ClickException(
            "Databricks host and token must be provided either as CLI options "
            "(--host, --token) or environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)"
        )

    if not resolved_volume_path:
        raise click.ClickException(
            "Volume path must be provided either as CLI option --volume-path or "
            "environment variable DATABRICKS_VOLUME_PATH"
        )

    if not resolved_workspace_path:
        raise click.ClickException(
            "Workspace path must be provided either as CLI option --workspace-path or "
            "environment variable DATABRICKS_WORKSPACE_PATH"
        )

    # Store resolved and validated parameters in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["databricks_config"] = {
        "host": resolved_host,
        "token": resolved_token,
        "volume_path": resolved_volume_path,
        "workspace_path": resolved_workspace_path,
    }


@cli.command()
@click.pass_context
def list_runtimes(ctx):
    """List available Databricks runtime versions."""
    try:
        manager = create_databricks_manager(ctx)
        runtimes = manager.cluster_configs["runtimes"]

        click.echo("Available Databricks Runtime Versions:")
        click.echo("=" * 40)
        for runtime, config in runtimes.items():
            click.echo(f"  {runtime}")
            click.echo(f"    Spark Version: {config['spark_version']}")
            click.echo(f"    Node Type: {config['node_type_id']}")
            click.echo(f"    Workers: {config['num_workers']}")
            click.echo()

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--runtime", required=True, help="Databricks runtime version")
@click.option(
    "--jar",
    multiple=True,
    required=True,
    help="Path to JAR file (can be specified multiple times)",
)
@click.option("--session-id", help="Session ID for this test run")
@click.pass_context
def create_cluster(ctx, runtime, jar, session_id):
    """Create or reuse a Databricks cluster with Sedona configuration."""
    try:
        manager = create_databricks_manager(ctx, session_id)

        click.echo(f"Creating/reusing cluster for runtime: {runtime}")
        click.echo("DatabricksManager created successfully.")
        session_info = ctx.obj.get("session_info", {})
        if session_info:
            click.echo(f"Session ID: {session_info['session_id']}")
            click.echo(f"Workspace path: {session_info['workspace_path']}")
            click.echo(f"Volume path: {session_info['volume_path']}")

        click.echo("JARs to upload:")
        for jar_file in jar:
            click.echo(f"  {jar_file}")

        # Prepare JAR paths using filename as key
        jar_paths = {}
        for jar_path in jar:
            filename = os.path.basename(jar_path)
            jar_paths[filename] = jar_path

        # Get session info for passing parameters
        session_info = ctx.obj.get("session_info", {})
        volume_path = session_info["volume_path"]
        cluster_name = f"{CLUSTER_NAME_PREFIX}{runtime}-{session_info['session_id']}"

        # Create cluster with JARs
        cluster_id = manager.create_cluster_with_local_jars(
            runtime, jar_paths, volume_path, cluster_name
        )
        click.echo(f"Cluster {cluster_id} is ready and will remain alive")
        click.echo(f"Session ID: {session_info['session_id']}")
        click.echo("Remember to clean up manually when done!")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--runtime", required=True, help="Databricks runtime version")
@click.option("--session-id", help="Session ID for this test run")
@click.option(
    "--jar",
    multiple=True,
    required=True,
    help="Path to JAR file (can be specified multiple times)",
)
@click.option(
    "--no-cleanup", is_flag=True, help="Skip cleanup of resources (for testing)"
)
@click.pass_context
def smoke_test(
    ctx,
    runtime,
    session_id,
    jar,
    no_cleanup,
):
    """Run smoke tests on specified runtime."""
    try:
        manager = create_databricks_manager(ctx, session_id)

        session_info = ctx.obj.get("session_info", {})
        click.echo(f"Running smoke tests on runtime: {runtime}")
        click.echo(f"Session ID: {session_info['session_id']}")
        click.echo("JARs to upload:")
        for jar_file in jar:
            click.echo(f"  {jar_file}")

        # Prepare JAR paths using filename as key
        jar_paths = {}
        for jar_path in jar:
            filename = os.path.basename(jar_path)
            jar_paths[filename] = jar_path

        # Get session info for passing parameters
        volume_path = session_info["volume_path"]
        workspace_path = session_info["workspace_path"]
        cluster_name = f"{CLUSTER_NAME_PREFIX}{runtime}-{session_info['session_id']}"
        job_name = f"sedona-smoke-test-{runtime}-{session_info['session_id']}"

        # Create cluster
        click.echo("Creating/reusing cluster...")
        cluster_id = manager.create_cluster_with_local_jars(
            runtime, jar_paths, volume_path, cluster_name
        )
        click.echo(f"Cluster ready: {cluster_id}")

        # Run smoke tests with optional log streaming
        click.echo("Running smoke tests...")
        test_result = manager.run_smoke_test(
            cluster_id,
            workspace_path,
            volume_path,
            job_name,
            session_info["session_id"],
        )

        # Display results
        click.echo("\n" + "=" * 60)
        click.echo("SMOKE TEST RESULTS")
        click.echo("=" * 60)

        if test_result.get("success", False):
            click.echo("✓ All smoke tests PASSED!")
            click.echo(f"Run ID: {test_result.get('run_id', 'Unknown')}")
            if test_result.get("run_page_url"):
                click.echo(f"View details: {test_result['run_page_url']}")
        else:
            click.echo("✗ Smoke tests FAILED!")
            click.echo(f"Status: {test_result.get('result_state', 'Unknown')}")
            click.echo(f"Message: {test_result.get('state_message', 'No message')}")
            if test_result.get("run_page_url"):
                click.echo(f"View details: {test_result['run_page_url']}")
            if test_result.get("error"):
                click.echo(f"Error: {test_result['error']}")

        # Show logs if available
        if test_result.get("logs"):
            click.echo("\n" + "=" * 60)
            click.echo("TEST OUTPUT")
            click.echo("=" * 60)
            # Show last 50 lines of logs to avoid overwhelming output
            log_lines = test_result["logs"].split("\n")
            click.echo("\n".join(log_lines))

        click.echo("=" * 60)

        # Cleanup
        if no_cleanup:
            click.echo("Skipping cleanup (--no-cleanup flag set)")
        else:
            click.echo("Cleaning up resources...")
            manager.terminate_cluster(cluster_id)
            manager.cleanup_volume_files(volume_path)
            manager.cleanup_workspace_files(workspace_path)

        if test_result.get("success", False):
            click.echo("✓ Test completed successfully")
            sys.exit(0)
        else:
            click.echo("✗ Test failed")
            sys.exit(1)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def _cleanup_cluster(manager, cluster):
    """Process a single cluster: terminate and delete it."""
    click.echo(f"Processing cluster: {cluster['name']}")
    try:
        # First terminate the cluster
        if not manager.terminate_cluster(cluster["id"]):
            click.echo(f"  ✗ Failed to terminate {cluster['name']}", err=True)
            return False

        click.echo(f"  ✓ Terminated {cluster['name']}")

        # Then delete it permanently
        if not manager.delete_cluster(cluster["id"]):
            click.echo(f"  ✗ Failed to delete {cluster['name']}", err=True)
            return False

        click.echo(f"  ✓ Permanently deleted {cluster['name']}")
        return True

    except Exception as e:
        click.echo(f"  ✗ Error processing {cluster['name']}: {e}", err=True)
        return False


def _cleanup_clusters(manager, clusters, cluster_type="clusters", force=False):
    """Clean up a list of clusters with user confirmation."""
    if not clusters:
        click.echo(f"No {cluster_type} found")
        return

    click.echo(f"Found {len(clusters)} {cluster_type}:")
    for cluster in clusters:
        click.echo(
            f"  - {cluster['name']} (ID: {cluster['id']}, State: {cluster['state']})"
        )

    if not force and not click.confirm(
        f"Do you want to terminate and delete these {cluster_type}?"
    ):
        click.echo("Cluster cleanup cancelled")
        return

    processed_count = 0
    for cluster in clusters:
        if _cleanup_cluster(manager, cluster):
            processed_count += 1

    click.echo(
        f"Successfully processed {processed_count}/{len(clusters)} {cluster_type}"
    )


def _cleanup_session(manager, session_id, volume_path, workspace_path, force=False):
    """Clean up resources for a specific session."""
    click.echo(f"Cleaning up session: {session_id}")

    # Find and process clusters for this session
    click.echo("Looking for clusters...")
    clusters = manager.find_clusters_by_suffix(session_id)
    _cleanup_clusters(
        manager, clusters, f"cluster(s) for session {session_id}", force=force
    )

    # Clean up files
    click.echo("Cleaning up files...")
    try:
        manager.cleanup_volume_files(volume_path)
        manager.cleanup_workspace_files(workspace_path)
        click.echo("✓ Files cleaned up successfully")
    except Exception as e:
        click.echo(f"✗ Warning: Failed to clean up files: {e}", err=True)

    click.echo(f"Session {session_id} cleanup completed")


def _cleanup_all_resources(manager, volume_path, workspace_path, force=False):
    """Clean up all sedona smoke-test resources."""
    click.echo("Cleaning up all Sedona smoke-test resources...")

    # Find and process all sedona-smoke-test clusters
    click.echo("Looking for all sedona-smoke-test clusters...")
    clusters = manager.find_clusters_by_prefix(CLUSTER_NAME_PREFIX)
    _cleanup_clusters(manager, clusters, "sedona-smoke-test cluster(s)", force=force)

    try:
        manager.cleanup_volume_files(volume_path)
        manager.cleanup_workspace_files(workspace_path)
        click.echo("✓ Volume files cleaned up successfully")
    except Exception as e:
        click.echo(f"✗ Warning: Failed to clean volume files: {e}", err=True)

    click.echo("All resources cleanup completed")


@cli.command()
@click.option("--all", is_flag=True, help="Clean up all resources")
@click.option("--session-id", help="Clean up specific session")
@click.option("--force", is_flag=True, help="Skip confirmation prompts")
@click.pass_context
def cleanup(ctx, all, session_id, force):
    """Clean up Databricks resources."""
    try:
        # Get validated configuration from context
        config = ctx.obj.get("databricks_config", {})
        host = config["host"]
        token = config["token"]
        volume_path = config["volume_path"]
        workspace_path = config["workspace_path"]

        if all:
            manager = DatabricksManager(host=host, token=token)
            if not manager.test_connection():
                raise click.ClickException("Failed to connect to Databricks")
            _cleanup_all_resources(manager, volume_path, workspace_path, force=force)
        else:
            if not session_id:
                click.echo("Specify --all or --session-id for cleanup")
                sys.exit(1)

            manager = DatabricksManager(host=host, token=token)
            if not manager.test_connection():
                raise click.ClickException("Failed to connect to Databricks")

            # Create session-specific paths for cleanup
            session_volume_path = f"{volume_path}/{session_id}"
            session_workspace_path = f"{workspace_path}/{session_id}"
            _cleanup_session(
                manager,
                session_id,
                session_volume_path,
                session_workspace_path,
                force=force,
            )

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def test_connection(ctx):
    """Test connection to Databricks workspace."""
    try:
        manager = create_databricks_manager(ctx)

        # Connection was already tested by create_databricks_manager
        click.echo("✓ Connection to Databricks successful")
        click.echo(f"  Host: {manager.host}")
        user = manager.client.current_user.me()
        click.echo(f"  User: {user.user_name}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
