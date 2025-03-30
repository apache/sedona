import typer


def generate(
        file: str = typer.Option(None, "--file", "-f", help="Path to the Terraform configuration file"),
):
    if file is None:
        pass




def create_terraform():
    terraform_app = typer.Typer(name="terraform")
    terraform_app.command(name="generate")(
        generate
    )


    return terraform_app
