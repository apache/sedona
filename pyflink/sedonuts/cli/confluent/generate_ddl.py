import typer

from sedonuts.cli.confluent.functions import list_functions, download_jar
from sedonuts.cli.confluent.template import function_template


def generate_ddl(
        file: str = typer.Option(None, "--file", "-f", help="Path to the Terraform configuration file"),
        artifact_id: str = typer.Option(..., "--artifact-id", "-a", help="Artifact ID of the JAR file"),
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

    templates = []

    for f in files:
        class_name = f.split("/")[-1].replace(".class", "")

        sql = function_template.format(class_name, class_name, artifact_id)

        templates.append(sql)

    if file is None:
        for template in templates:
            print(template)
            print("")

        return

    with open(file, "w") as f:
        for template in templates:
            f.write(template)
            f.write("\n\n")


def create_ddl_command(file: str | None = None):
    ddl_command = typer.Typer(name="ddl")

    ddl_command.command(name="generate")(generate_ddl)

    return ddl_command
