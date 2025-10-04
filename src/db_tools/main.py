import typer
from rich import print
from typing_extensions import Annotated

from db_tools.core.config_loader import load_config
from db_tools.core.database import get_engine
# --- THAY ĐỔI 1: Import Base từ models ---
from db_tools.core.models import Base
from db_tools.core.processor import process_anonymize, process_seed
from db_tools.core.translator import t
from db_tools.tui import DbToolsApp

app = typer.Typer(
    help="A CLI tool to seed and anonymize development databases.",
    rich_markup_mode="markdown"
)

# Tạo một nhóm lệnh con 'schema'
schema_app = typer.Typer(help="Manage database schema.")
app.add_typer(schema_app, name="schema")


ConnectionOption = Annotated[
    str,
    typer.Option(
        "--connection", "-c",
        help="Database connection string to override the config file.",
    ),
]

# --- Tạo lệnh `schema create` ---
@schema_app.command("create")
def schema_create(connection: ConnectionOption = None):
    """
    Create all tables in the database based on the defined models.
    """
    try:
        config = load_config()
        connection_string = connection or config.get("connection")

        if not connection_string:
            print("[bold red]Error: No connection string provided.[/bold red]")
            raise typer.Exit(code=1)

        print(f"[cyan]Connecting to database...[/cyan]")
        db_engine = get_engine(connection_string)

        print("[cyan]Creating tables...[/cyan]")
        # Dòng lệnh quyền năng của SQLAlchemy:
        # Nó sẽ kiểm tra và tạo các bảng chưa tồn tại.
        Base.metadata.create_all(db_engine)

        print("[bold green]✅ Tables created successfully (if they didn't exist).[/bold green]")

    except Exception as e:
        print(f"[bold red]An unexpected error occurred: {repr(e)}[/bold red]")
        raise typer.Exit(code=1)

@app.command()
def seed(connection: ConnectionOption = None):
    try:
        config = load_config()
        connection_string = connection or config.get("connection")

        if not connection_string:
            print("[bold red]Error: No connection string provided.[/bold red]")
            print("Please provide one via the --connection option or in a `db_tools.yml` file.")
            raise typer.Exit(code=1)

        print(f"[cyan]Connecting to database...[/cyan]")
        db_engine = get_engine(connection_string)
        
        process_seed(config, db_engine)
        
    except ConnectionError as e:
        print(f"[bold red]{t.get('error_db_connection', error=e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(f"[bold red]An unexpected error occurred: {repr(e)}[/bold red]")
        raise typer.Exit(code=1)

@app.command()
def anonymize(connection: ConnectionOption = None):
    try:
        config = load_config()
        connection_string = connection or config.get("connection")

        if not connection_string:
            print("[bold red]Error: No connection string provided.[/bold red]")
            print("Please provide one via the --connection option or in a `db_tools.yml` file.")
            raise typer.Exit(code=1)

        print(f"[cyan]Connecting to database...[/cyan]")
        db_engine = get_engine(connection_string)

        process_anonymize(config, db_engine)

    except ConnectionError as e:
        print(f"[bold red]{t.get('error_db_connection', error=e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(f"[bold red]An unexpected error occurred: {repr(e)}[/bold red]")
        raise typer.Exit(code=1)

@app.command()
def ui():
    app = DbToolsApp()
    app.run()

if __name__ == "__main__":
    app()