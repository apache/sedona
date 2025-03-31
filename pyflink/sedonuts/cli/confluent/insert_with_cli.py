import json
import time
from dataclasses import dataclass

import typer

from sedonuts.cli.confluent.functions import list_functions, download_jar
from sedonuts.cli.confluent.template import function_template
import concurrent.futures

import subprocess


@dataclass
class FlinkSQLStatement:
    name: str
    status: str
    full_metadata: str
    status_detail: str | None = None


def describe_flink_sql_statement(name: str, environment: str):
    # The command to run
    command = [
        'confluent', 'flink', 'statement', 'describe',
        name, "--environment", environment, "--output", "json",
        "--cloud", "aws", "--region", "us-east-1"
    ]

    # Run the command using subprocess.run
    result = subprocess.run(command, capture_output=True, text=True)

    # Check the result and print output or error
    if result.returncode == 0:
        result = result.stdout
        metadata = json.loads(result)
        status_detail = metadata.get("statusDetail", None)

        return FlinkSQLStatement(
            name=metadata["name"],
            status=metadata["status"],
            full_metadata=result,
            status_detail=status_detail
        )

    else:
        typer.secho("command failed", fg=typer.colors.RED)
        typer.secho(result.stderr, fg=typer.colors.RED)


def run_flink_sql_statement(sql: str, compute_pool: str, database: str, environment: str, function_name: str):
    typer.secho(f"Creating function for {function_name}", fg=typer.colors.GREEN)
    command = [
        'confluent', 'flink', 'statement', 'create', '--sql',
        sql, '--compute-pool', compute_pool, '--database', database,
        "--environment", environment, "--output", "json"
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        result = result.stdout
        metadata = json.loads(result)

        name = metadata["name"]

        metadata_update = describe_flink_sql_statement(name, environment)

        while metadata_update.status == "RUNNING" or metadata_update.status == "PENDING":
            time.sleep(2)
            metadata_update = describe_flink_sql_statement(name, environment)

        if metadata_update.status == "FAILED":
            status_detail = metadata_update.status_detail if metadata_update.status_detail else ""
            full_metadata = metadata_update.full_metadata
            typer.secho(f"Command failed for {function_name} {status_detail} {full_metadata}", fg=typer.colors.RED)
            return

        if metadata_update.status == "COMPLETED":
            status_detail = metadata_update.status_detail if metadata_update.status_detail else ""
            typer.secho(f"Command succeeded for {function_name} {status_detail}", fg=typer.colors.GREEN)
            return

    else:
        typer.secho("Command failed:", fg=typer.colors.RED)
        typer.secho(result.stderr.strip(), fg=typer.colors.RED)
        typer.secho("", fg=typer.colors.RED)


def apply(
        file: str = typer.Option(None, "--file", "-f", help="Path to the Terraform configuration file"),
        artifact_id: str = typer.Option(..., "--artifact-id", "-a", help="Artifact ID of the JAR file"),
        database: str = typer.Option(..., "--database", "-d", help="Database name"),
        compute_pool: str = typer.Option(..., "--compute-pool", "-c", help="Compute pool name"),
        environment: str = typer.Option(..., "--environment", "-e", help="Environment name"),
        sedona_version: str = typer.Option(None, "--sedona-version", "-s", help="Sedona version"),
        scala_version: str = typer.Option(None, "--scala-version", "-v", help="Scala version")
):
    path = file

    if not path:
        path = download_jar(sedona_version=sedona_version, scala_version=scala_version)

    files = list_functions(
        sedona_version,
        scala_version,
        path
    )

    tasks = []
    for f in files:
        class_name = f.split("/")[-1].replace(".class", "")

        location = "constructors" if "constructors" in f else "functions"

        sql = function_template.format(class_name, location, class_name, artifact_id)

        tasks.append([sql, class_name])

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        task_pool = [executor.submit(run_flink_sql_statement, sql, compute_pool, database, environment, class_name) for sql, class_name in tasks]

        for future in concurrent.futures.as_completed(task_pool):
            task_result = future.result()


def create_confluent_cli_command():
    cli_command = typer.Typer(name="cli")
    cli_command.command(name="apply")(apply)

    return cli_command
