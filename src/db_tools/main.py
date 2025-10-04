import typer
from rich import print

from db_tools.core.translator import t

app = typer.Typer(help="A CLI tool to seed and anonymize development databases.")

@app.command()
def seed():
    """
    Seed the database with sample data based on the config file.
    """
    print(t.get("seeding_started", table_name="users"))

@app.command()
def anonymize():
    """
    Anonymize sensitive data in the database.
    """
    print("Anonymizing data... (Not implemented yet)")


if __name__ == "__main__":
    app()