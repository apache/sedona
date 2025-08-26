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
Databricks Manager
Handles cluster lifecycle management, JAR uploads, and job execution.
"""

import base64
import io
import json
import logging
import time
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    DataSecurityMode,
    InitScriptInfo,
    RuntimeEngine,
    VolumesStorageInfo,
)
from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    RunResultState,
    SparkPythonTask,
    Task,
)
from databricks.sdk.service.workspace import ImportFormat
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

from . import PROJECT_ROOT

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabricksManager:
    """Manages Databricks cluster lifecycle and operations."""

    def __init__(
        self,
        host: str,
        token: str,
    ):
        """Initialize Databricks client and configuration.

        Args:
            host: Databricks workspace host
            token: Databricks access token
        """
        self.host = host
        self.token = token

        # Remove protocol if present and ensure https
        if self.host.startswith("http://") or self.host.startswith("https://"):
            self.host = self.host.split("://", 1)[1]

        # Initialize Databricks client
        self.client = WorkspaceClient(host=f"https://{self.host}", token=self.token)

        # Load cluster configurations
        config_path = PROJECT_ROOT / "config" / "cluster_configs.json"
        with open(config_path) as f:
            self.cluster_configs = json.load(f)

    def test_connection(self) -> bool:
        """Test connection to Databricks workspace."""
        try:
            current_user = self.client.current_user.me()
            logger.info(f"Connected to Databricks as: {current_user.user_name}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def prepare_init_script(
        self, jar_paths: dict[str, str], volume_path: str
    ) -> dict[str, str]:
        """Upload init scripts and JAR files to Databricks Volumes, then generate init script
        and upload it.

        Args:
            runtime_version: The Databricks runtime version
            jar_paths: Dictionary of jar_name -> local_file_path for JARs to upload (required)
            volume_path: Base path in Volumes to store files
        """
        logger.info("Uploading files to Volumes...")

        # Create unique directory for this session to avoid conflicts
        base_path = volume_path
        jars_path = f"{base_path}/jars"

        uploaded_files = {}

        # Note: Volumes directories are created automatically when files are uploaded

        # Upload JAR files to Volumes
        logger.info(f"Uploading {len(jar_paths)} JAR files to Volumes...")
        for jar_name, local_path in jar_paths.items():
            jar_file = Path(local_path)
            volume_jar_path = f"{jars_path}/{jar_name}"

            # Check if JAR already exists in Volumes
            try:
                existing_metadata = self.client.files.get_metadata(
                    file_path=volume_jar_path
                )
                local_size = jar_file.stat().st_size
                volume_size = existing_metadata.content_length

                if volume_size == local_size:
                    logger.info(
                        f"JAR {jar_name} already exists in Volumes with same size "
                        f"({local_size} bytes), skipping upload"
                    )
                    uploaded_files[jar_name] = volume_jar_path
                    continue
                else:
                    logger.info(
                        f"JAR {jar_name} exists but size differs "
                        f"(local: {local_size}, volume: {volume_size}), re-uploading"
                    )
            except Exception:
                # File doesn't exist or error checking, proceed with upload
                logger.info(f"JAR {jar_name} not found in Volumes, uploading...")

            logger.info(f"Uploading {jar_name} to {volume_jar_path}")

            # Use the Files API to upload to Volumes (supports large files)
            with open(jar_file, "rb") as f:
                self.client.files.upload(
                    file_path=volume_jar_path, contents=f, overwrite=True
                )

            uploaded_files[jar_name] = volume_jar_path
            logger.info(
                f"Uploaded {jar_name} to Volumes ({jar_file.stat().st_size} bytes)"
            )

        # Generate init script content
        init_script_content = self._generate_init_script(uploaded_files)

        # Upload init script to Volumes
        init_script_path = f"{base_path}/init_scripts/sedona_init.sh"
        logger.info(f"Uploading init script to Volume: {init_script_path}")

        # Use the Files API to upload init script to Volumes
        self.client.files.upload(
            file_path=init_script_path,
            contents=io.BytesIO(init_script_content.encode()),
            overwrite=True,
        )
        uploaded_files["init_script"] = init_script_path
        return uploaded_files

    def _generate_init_script(self, uploaded_files: dict[str, str]) -> str:
        """Generate init script content using Jinja2 template."""
        jars = []
        for key, volume_path in uploaded_files.items():
            jars.append({"name": key, "volume_path": volume_path})

        # Setup Jinja2 environment
        template_dir = PROJECT_ROOT / "config"
        env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
            autoescape=True,
        )

        # Render the template
        template = env.get_template("init_script.j2")
        context = {"jars": jars, "total_jars": len(jars)}
        return template.render(**context)

    def create_cluster_with_local_jars(
        self,
        runtime_version: str,
        jar_paths: dict[str, str],
        volume_path: str,
        cluster_name: str,
    ) -> str:
        """Create or reuse a cluster with custom JAR files.

        Args:
            runtime_version: Databricks runtime version
            jar_paths: Dictionary of jar_name -> local_file_path for JARs (required)
            volume_path: Path in Volumes to store files
            cluster_name: Full name for the cluster
        """
        # Upload files to Volumes
        uploaded_files = self.prepare_init_script(jar_paths, volume_path)

        # Create or reuse cluster
        return self.reuse_or_create_cluster(
            runtime_version, uploaded_files, cluster_name
        )

    def find_cluster_by_name(self, cluster_name: str) -> Optional[str]:
        """Find cluster ID by name if it exists."""
        clusters = self.client.clusters.list()
        for cluster in clusters:
            if cluster.cluster_name == cluster_name:
                logger.info(
                    f"Found existing cluster: {cluster_name} (ID: {cluster.cluster_id})"
                )
                return cluster.cluster_id
        return None

    def find_clusters_by_suffix(self, suffix: str) -> list[dict[str, str]]:
        """Find all clusters with names ending with the given suffix."""
        clusters = self.client.clusters.list()
        matching_clusters = []

        for cluster in clusters:
            # Check if cluster name ends with the suffix
            if cluster.cluster_name and cluster.cluster_name.endswith(f"-{suffix}"):
                matching_clusters.append(
                    {
                        "id": cluster.cluster_id,
                        "name": cluster.cluster_name,
                        "state": (cluster.state.value if cluster.state else "UNKNOWN"),
                    }
                )
                logger.info(
                    f"Found matching cluster: {cluster.cluster_name} "
                    f"(ID: {cluster.cluster_id}, "
                    f"State: {cluster.state.value if cluster.state else 'UNKNOWN'})"
                )

        return matching_clusters

    def find_clusters_by_prefix(self, prefix: str) -> list[dict[str, str]]:
        """Find all clusters with names starting with the given prefix."""
        clusters = self.client.clusters.list()
        matching_clusters = []

        for cluster in clusters:
            # Check if cluster name starts with the prefix
            if cluster.cluster_name and cluster.cluster_name.startswith(prefix):
                matching_clusters.append(
                    {
                        "id": cluster.cluster_id,
                        "name": cluster.cluster_name,
                        "state": (cluster.state.value if cluster.state else "UNKNOWN"),
                    }
                )
                logger.info(
                    f"Found sedona cluster: {cluster.cluster_name} "
                    f"(ID: {cluster.cluster_id}, "
                    f"State: {cluster.state.value if cluster.state else 'UNKNOWN'})"
                )

        return matching_clusters

    def get_cluster_state(self, cluster_id: str) -> Optional[str]:
        """Get the current state of a cluster."""
        try:
            cluster_info = self.client.clusters.get(cluster_id)
            return cluster_info.state.value if cluster_info.state else "UNKNOWN"
        except Exception as e:
            logger.warning(f"Error getting cluster state: {e}")
            return None

    def start_cluster(self, cluster_id: str) -> bool:
        """Start a cluster that is in TERMINATED state."""
        try:
            logger.info(f"Starting cluster: {cluster_id}")
            self.client.clusters.start(cluster_id)
            return True
        except Exception as e:
            logger.error(f"Failed to start cluster: {e}")
            return False

    def reuse_or_create_cluster(
        self, runtime_version: str, uploaded_files: dict[str, str], cluster_name: str
    ) -> str:
        """Create or reuse a Databricks cluster for the specified runtime."""
        if runtime_version not in self.cluster_configs["runtimes"]:
            raise ValueError(f"Unsupported runtime version: {runtime_version}")

        config = self.cluster_configs["runtimes"][runtime_version].copy()

        # Check if cluster already exists
        existing_cluster_id = self.find_cluster_by_name(cluster_name)

        if existing_cluster_id:
            logger.info(f"Found existing cluster: {cluster_name}")

            # Check cluster state
            current_state = self.get_cluster_state(existing_cluster_id)
            logger.info(f"Cluster state: {current_state}")

            if current_state == "RUNNING":
                logger.info("Cluster is already running, reusing it")
                return existing_cluster_id
            elif current_state == "TERMINATED":
                logger.info("Cluster is terminated, starting it...")
                if self.start_cluster(existing_cluster_id):
                    if self.wait_for_cluster_ready(existing_cluster_id):
                        logger.info("Cluster started successfully")
                        return existing_cluster_id
                    else:
                        logger.warning(
                            "Failed to start existing cluster, will create new one"
                        )
                else:
                    logger.warning(
                        "Failed to start existing cluster, will create new one"
                    )
            elif current_state in ["PENDING", "RESTARTING", "RESIZING"]:
                logger.info(
                    f"Cluster is in {current_state} state, waiting for it to be ready..."
                )
                if self.wait_for_cluster_ready(existing_cluster_id):
                    logger.info("Cluster is now ready")
                    return existing_cluster_id
                else:
                    logger.warning(
                        "Cluster failed to become ready, will create new one"
                    )
            else:
                logger.warning(
                    f"Cluster is in unexpected state {current_state}, will create new one"
                )

        # Create new cluster if no existing cluster found or existing cluster couldn't be used
        logger.info(f"Creating new cluster: {cluster_name}")

        try:
            init_scripts = [
                InitScriptInfo(
                    volumes=VolumesStorageInfo(
                        destination=uploaded_files["init_script"]
                    )
                )
            ]
            data_security_mode = None
            if config.get("data_security_mode"):
                data_security_mode = DataSecurityMode(config["data_security_mode"])

            runtime_engine = None
            if config.get("runtime_engine"):
                runtime_engine = RuntimeEngine(config["runtime_engine"])

            cluster_create_response = self.client.clusters.create(
                spark_version=config["spark_version"],
                cluster_name=cluster_name,
                node_type_id=config["node_type_id"],
                num_workers=config["num_workers"],
                spark_conf=config.get("spark_conf", {}),
                init_scripts=init_scripts,
                autotermination_minutes=config.get("autotermination_minutes", 60),
                enable_elastic_disk=config.get("enable_elastic_disk", True),
                data_security_mode=data_security_mode,
                runtime_engine=runtime_engine,
            )

            cluster_id = cluster_create_response.cluster_id
            if not cluster_id:
                raise ValueError("Failed to get cluster ID from creation response")

            logger.info(f"New cluster created with ID: {cluster_id}")
            logger.info("Waiting for cluster to start...")

            if self.wait_for_cluster_ready(cluster_id):
                logger.info("Cluster is ready!")
                return cluster_id
            else:
                raise RuntimeError("Cluster failed to become ready")

        except Exception as e:
            logger.error(f"Failed to create cluster: {e}")
            raise

    def wait_for_cluster_ready(
        self, cluster_id: str, timeout_minutes: int = 15
    ) -> bool:
        """Wait for cluster to be in RUNNING state."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                cluster_info = self.client.clusters.get(cluster_id)
                state = cluster_info.state.value if cluster_info.state else "UNKNOWN"

                logger.info(f"Cluster {cluster_id} state: {state}")

                if state == "RUNNING":
                    logger.info("Cluster is ready!")
                    return True
                elif state in ["ERROR", "TERMINATED"]:
                    logger.error(f"Cluster failed to start. State: {state}")
                    return False

            except Exception as e:
                logger.warning(f"Error checking cluster state: {e}")

            time.sleep(5)

        logger.error(f"Cluster failed to start within {timeout_minutes} minutes")
        return False

    def terminate_cluster(self, cluster_id: str) -> bool:
        """Terminate a cluster by ID."""
        try:
            self.client.clusters.delete(cluster_id=cluster_id)
            logger.info(f"Terminated cluster {cluster_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to terminate cluster {cluster_id}: {e}")
            return False

    def delete_cluster(self, cluster_id: str) -> bool:
        """Permanently delete a cluster by ID."""
        try:
            self.client.clusters.permanent_delete(cluster_id=cluster_id)
            logger.info(f"Permanently deleted cluster {cluster_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to permanently delete cluster {cluster_id}: {e}")
            return False

    def _delete_directory_recursive(self, directory_path: str):
        """Recursively delete a directory and all its contents."""
        try:
            # List all contents in the directory
            contents = self.client.files.list_directory_contents(
                directory_path=directory_path
            )

            # Delete all files and subdirectories
            for item in contents:
                if item.path:
                    if item.is_directory:
                        self._delete_directory_recursive(item.path)
                    else:
                        self.client.files.delete(file_path=item.path)
                        logger.debug(f"Deleted file: {item.path}")

            # After all contents are deleted, delete the directory itself
            self.client.files.delete_directory(directory_path=directory_path)
            logger.debug(f"Deleted directory: {directory_path}")

        except Exception as e:
            # If directory doesn't exist or is already empty, that's fine
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                logger.debug(f"Directory already cleaned up: {directory_path}")
            else:
                raise e

    def upload_test_data(self, volume_path: str) -> None:
        """Upload test data from smoke-tests/data to the session-specific volume.

        Args:
            volume_path: Base volume path for the session (e.g., /Volumes/catalog/schema/volume/session_id)
        """
        # Define source and destination paths
        local_data_path = PROJECT_ROOT / "smoke-tests" / "data"
        volume_data_path = f"{volume_path}/data"

        if not local_data_path.exists():
            raise RuntimeError(f"Test data directory not found: {local_data_path}")

        logger.info(f"Uploading test data from {local_data_path} to {volume_data_path}")
        self._upload_directory_recursive(local_data_path, volume_data_path)
        logger.info("Test data upload completed")

    def _upload_directory_recursive(self, local_dir: Path, volume_dir: str) -> None:
        """Recursively upload a local directory to Databricks Volumes.

        Args:
            local_dir: Local directory path to upload
            volume_dir: Target directory path in Databricks Volumes
        """
        # Iterate through all items in the local directory
        for item in local_dir.iterdir():
            if item.is_file():
                # Upload file
                volume_file_path = f"{volume_dir}/{item.name}"
                logger.debug(f"Uploading file: {item} -> {volume_file_path}")

                with open(item, "rb") as f:
                    self.client.files.upload(
                        file_path=volume_file_path, contents=f, overwrite=True
                    )

                logger.debug(
                    f"Uploaded file: {item.name} ({item.stat().st_size} bytes)"
                )

            elif item.is_dir():
                # Recursively upload subdirectory
                volume_subdir = f"{volume_dir}/{item.name}"
                logger.debug(f"Uploading directory: {item} -> {volume_subdir}")

                # Recursive call for subdirectory
                self._upload_directory_recursive(item, volume_subdir)

    def cleanup_volume_files(self, volume_path: str):
        """Clean up uploaded volume files for this session."""
        try:
            logger.info(f"Cleaning up volume files at: {volume_path}")

            # Recursively delete all files and directories inside the session directory
            try:
                self._delete_directory_recursive(volume_path)
                logger.info("Volume cleanup completed")
            except Exception as dir_error:
                logger.debug(f"Volume cleanup failed: {dir_error}")

        except Exception as e:
            # Check if the error is just that the directory doesn't exist
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                logger.info("Volume files were already cleaned up")
            else:
                logger.warning(f"Volume cleanup failed: {e}")

    def cleanup_workspace_files(self, workspace_path: str):
        """Clean up uploaded workspace files for this session."""
        try:
            logger.info(f"Cleaning up workspace files at: {workspace_path}")

            # Try to delete the session directory
            try:
                self.client.workspace.delete(path=workspace_path, recursive=True)
                logger.info("Workspace cleanup completed")
            except Exception as workspace_error:
                # Check if the error is just that the directory doesn't exist
                if (
                    "does not exist" in str(workspace_error).lower()
                    or "not found" in str(workspace_error).lower()
                ):
                    logger.info("Workspace files were already cleaned up")
                else:
                    logger.warning(f"Workspace cleanup failed: {workspace_error}")

        except Exception as e:
            logger.warning(f"Workspace cleanup failed: {e}")

    def upload_python_file(self, local_path: str, workspace_path: str) -> str:
        """Upload a Python file to Databricks workspace.

        Args:
            local_path: Local path to the Python file
            workspace_path: Target path in Databricks workspace

        Returns:
            Workspace path where the file was uploaded
        """
        try:
            logger.info(f"Uploading Python file from {local_path} to {workspace_path}")

            # Create parent folder if it doesn't exist
            parent_folder = "/".join(workspace_path.split("/")[:-1])
            if parent_folder:
                self.create_workspace_folder(parent_folder)

            with open(local_path, "rb") as f:
                content = f.read()

            # Encode content as base64 for SOURCE format
            encoded_content = base64.b64encode(content).decode("utf-8")

            # Upload as Python file to workspace
            self.client.workspace.import_(
                path=workspace_path,
                format=ImportFormat.AUTO,
                content=encoded_content,
                overwrite=True,
            )

            logger.info(f"Python file uploaded successfully to {workspace_path}")
            return workspace_path

        except Exception as e:
            logger.error(f"Failed to upload Python file: {e}")
            raise

    def create_workspace_folder(self, folder_path: str) -> None:
        """Create a workspace folder if it doesn't exist.

        Args:
            folder_path: Path of the folder to create in Databricks workspace
        """
        try:
            # Create the folder (this will create parent folders as needed)
            self.client.workspace.mkdirs(path=folder_path)
            logger.info(f"Created workspace folder: {folder_path}")
        except Exception as e:
            # Folder might already exist, which is fine
            logger.debug(f"Workspace folder creation result for {folder_path}: {e}")

    def create_and_run_python_job(
        self,
        cluster_id: str,
        python_file_path: str,
        job_name: str,
        session_id: Optional[str] = None,
    ) -> int:
        """Create and run a job with the specified Python file on the given cluster.

        Args:
            cluster_id: ID of the cluster to run the job on
            python_file_path: Volume path to the Python file
            job_name: Name for the job
            session_id: Optional session ID to pass as parameter to the script

        Returns:
            Run ID of the started job
        """
        try:
            logger.info(
                f"Creating Python job '{job_name}' with file {python_file_path}"
            )

            # Prepare parameters for the Python script
            parameters = []
            if session_id:
                parameters.extend(["--session-id", session_id])

            # Create job using SparkPythonTask
            tasks = [
                Task(
                    task_key="smoke_test",
                    existing_cluster_id=cluster_id,
                    spark_python_task=SparkPythonTask(
                        python_file=python_file_path,
                        parameters=parameters if parameters else None,
                    ),
                    timeout_seconds=1800,  # 30 minutes timeout
                )
            ]

            job = self.client.jobs.create(name=job_name, tasks=tasks)

            logger.info(f"Job created with ID: {job.job_id}")
            if parameters:
                logger.info(f"Job parameters: {parameters}")

            # Ensure job_id is not None
            if job.job_id is None:
                raise ValueError("Job creation failed: job_id is None")

            # Run the job
            run = self.client.jobs.run_now(job_id=job.job_id)

            logger.info(f"Job started with run ID: {run.run_id}")
            return run.run_id

        except Exception as e:
            logger.error(f"Failed to create and run Python job: {e}")
            raise

    def wait_for_job_completion(self, run_id: int, timeout_minutes: int = 30) -> dict:
        """Wait for a job run to complete and return the result.

        Args:
            run_id: ID of the job run to monitor
            timeout_minutes: Maximum time to wait for completion

        Returns:
            Dictionary with job result information
        """
        logger.info(f"Waiting for job run {run_id} to complete...")
        timeout_time = time.time() + (timeout_minutes * 60)
        while time.time() < timeout_time:
            try:
                run = self.client.jobs.get_run(run_id=run_id)

                if (
                    run.state
                    and run.state.life_cycle_state == RunLifeCycleState.TERMINATED
                ):
                    result_state = (
                        run.state.result_state if run.state else RunResultState.FAILED
                    )

                    logger.info(
                        f"Job run {run_id} completed with result: {result_state}"
                    )

                    return {
                        "run_id": run_id,
                        "result_state": result_state,
                        "success": result_state == RunResultState.SUCCESS,
                        "start_time": run.start_time,
                        "end_time": run.end_time,
                        "run_page_url": run.run_page_url,
                        "state_message": run.state.state_message if run.state else "",
                    }
                elif run.state and run.state.life_cycle_state in [
                    RunLifeCycleState.PENDING,
                    RunLifeCycleState.RUNNING,
                ]:
                    logger.info(f"Job run {run_id} is {run.state.life_cycle_state}...")
                    time.sleep(5)
                else:
                    # Job failed or was cancelled
                    life_cycle_state = (
                        run.state.life_cycle_state if run.state else "UNKNOWN"
                    )

                    logger.error(
                        f"Job run {run_id} failed with state: {life_cycle_state}"
                    )
                    return {
                        "run_id": run_id,
                        "result_state": (
                            run.state.result_state
                            if run.state
                            else RunResultState.FAILED
                        ),
                        "success": False,
                        "start_time": run.start_time,
                        "end_time": run.end_time,
                        "run_page_url": run.run_page_url,
                        "state_message": run.state.state_message if run.state else "",
                    }

            except Exception as e:
                logger.error(f"Error checking job status: {e}")
                time.sleep(10)

        logger.error(f"Job run {run_id} timed out after {timeout_minutes} minutes")
        return {
            "run_id": run_id,
            "result_state": "TIMEOUT",
            "success": False,
            "timeout": True,
            "state_message": f"Job timed out after {timeout_minutes} minutes",
        }

    def get_job_logs(self, run_id: int) -> str:
        """Get the logs from a completed job run.

        Args:
            run_id: ID of the job run

        Returns:
            Job output logs as string
        """
        try:
            # Get the run details to check if it's a multi-task job
            run = self.client.jobs.get_run(run_id=run_id)

            if run.tasks and len(run.tasks) > 1:
                # Multi-task job - get logs from each task
                all_logs = []
                for task in run.tasks:
                    if task.run_id:
                        try:
                            task_output = self.client.jobs.get_run_output(
                                run_id=task.run_id
                            )
                            task_logs = (
                                task_output.logs
                                or f"No logs available for task {task.task_key}"
                            )
                            all_logs.append(
                                f"=== Task: {task.task_key} ===\n{task_logs}"
                            )
                        except Exception as task_e:
                            all_logs.append(
                                f"=== Task: {task.task_key} ===\nError getting logs: {task_e}"
                            )
                return "\n\n".join(all_logs)
            elif run.tasks and len(run.tasks) == 1:
                # Single task job - get logs from the task
                task = run.tasks[0]
                if task.run_id:
                    task_output = self.client.jobs.get_run_output(run_id=task.run_id)
                    return task_output.logs or "No logs available"
                else:
                    return "No task run ID available"
            else:
                # Try to get logs directly from the run (legacy approach)
                output = self.client.jobs.get_run_output(run_id=run_id)
                return output.logs or "No logs available"

        except Exception as e:
            logger.error(f"Failed to get job logs: {e}")
            return f"Error retrieving logs: {e}"

    def run_smoke_test(
        self,
        cluster_id: str,
        workspace_path: str,
        volume_path: str,
        job_name: str,
        session_id: Optional[str] = None,
        stream_logs: bool = True,
    ) -> dict:
        """Run the complete smoke test on the specified cluster.

        Args:
            cluster_id: ID of the cluster to run tests on
            workspace_path: Path in workspace to store files
            volume_path: Path in volume to store test data files
            job_name: Full name for the job
            session_id: Optional session ID for test isolation
            stream_logs: Whether to stream logs in real-time

        Returns:
            Dictionary with test results
        """
        try:
            # Define paths
            smoke_test_file = PROJECT_ROOT / "smoke-tests" / "smoke_test.py"
            workspace_python_path = f"{workspace_path}/smoke_test.py"

            # Upload test data to volume before running tests
            logger.info("Uploading test data to volume...")
            self.upload_test_data(volume_path)

            # Upload smoke test Python file to workspace
            self.upload_python_file(str(smoke_test_file), workspace_python_path)

            # Create and run Python job with session_id
            run_id = self.create_and_run_python_job(
                cluster_id, workspace_python_path, job_name, session_id
            )

            # Wait for completion with optional log streaming
            result = self.wait_for_job_completion(run_id)

            # Get logs after job completion
            result["logs"] = self.get_job_logs(run_id)

            return result

        except Exception as e:
            logger.error(f"Smoke test execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "state_message": f"Smoke test execution failed: {e}",
            }
