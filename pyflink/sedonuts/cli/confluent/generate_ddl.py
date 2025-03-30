import typer

from sedonuts.cli.confluent.functions import list_functions
from sedonuts.cli.confluent.template import function_template


def generate_ddl(
        file: str = typer.Option(None, "--file", "-f", help="Path to the Terraform configuration file"),
        artifact_id: str = typer.Option(..., "--artifact-id", "-a", help="Artifact ID of the JAR file")
):
    files = list_functions(
        "1.8.0",
        "2.12",
        "/Users/pawelkocinski/Desktop/projects/sed/sedona/flink-shaded/target"
    )

    templates = []

    for f in files:
        tail = f.split("/")[-1].replace(".class", "")
        class_name = tail.split("$")[1]
        location = tail.replace("$", ".")
        # class_name, location,
        templates.append(function_template.format(artifact_id))

    if file is None:
        for template in templates:
            print(template)
            print("")

        return

    with open(file, "w") as f:
        for template in templates:
            f.write(template)
            f.write("\n\n")


def create_ddl_command():
    ddl_command = typer.Typer(name="ddl")
    ddl_command.command(name="generate")(generate_ddl)

    return ddl_command
