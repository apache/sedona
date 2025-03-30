import typer

from sedonuts.cli.confluent.generate_ddl import create_ddl_command
from sedonuts.cli.confluent.generate_terraform import create_terraform
from sedonuts.cli.confluent.insert_with_cli import create_confluent_cli_command


def main():
    terraform_command = create_terraform()
    ddl_command = create_ddl_command()
    confluent_cli_command = create_confluent_cli_command()

    app = typer.Typer()

    app.add_typer(terraform_command, name="terraform")
    app.add_typer(ddl_command, name="ddl")
    app.add_typer(confluent_cli_command, name="cli")

    app()


if __name__ == "__main__":
    main()
