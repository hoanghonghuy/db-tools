# src/db_tools/main.py

import typer
from rich import print

from db_tools.core.config_loader import load_config
from db_tools.core.database import get_engine
from db_tools.core.processor import process_seed
from db_tools.core.translator import t
from db_tools.core.processor import process_seed, process_anonymize

app = typer.Typer(help="A CLI tool to seed and anonymize development databases.")


@app.command()
def seed():
    """
    Seed the database with sample data based on the config file.
    """
    try:
        # 1. Load configuration
        print("[bold cyan]Loading configuration...[/bold cyan]")
        config = load_config()
        connection_string = config.get("connection")
        if not connection_string:
            print("[bold red]Error: 'connection' string not found in config file.[/bold red]")
            raise typer.Exit(code=1)
        
        # 2. Create database engine
        print(f"[bold cyan]Connecting to database...[/bold cyan]")
        db_engine = get_engine(connection_string)

        # 3. Call the processor to do the work
        process_seed(config, db_engine)
        
    except FileNotFoundError:
        print(f"[bold red]Error: {t.get('config_file_not_found', filename='db_tools.yml')}[/bold red]")
        raise typer.Exit(code=1)
    except ConnectionError as e:
        print(f"[bold red]{t.get('error_db_connection', error=e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(f"[bold red]An unexpected error occurred: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def anonymize():
    """
    Anonymize sensitive data in the database.
    """
    try:
        # Các bước 1 và 2 giống hệt như hàm seed
        # 1. Load configuration
        print("[bold cyan]Loading configuration...[/bold cyan]")
        config = load_config()
        connection_string = config.get("connection")
        if not connection_string:
            print("[bold red]Error: 'connection' string not found in config file.[/bold red]")
            raise typer.Exit(code=1)
        
        # 2. Create database engine
        print(f"[bold cyan]Connecting to database...[/bold cyan]")
        db_engine = get_engine(connection_string)

        # 3. Call the processor to do the work
        process_anonymize(config, db_engine)
        
    except FileNotFoundError:
        print(f"[bold red]Error: {t.get('config_file_not_found', filename='db_tools.yml')}[/bold red]")
        raise typer.Exit(code=1)
    except ConnectionError as e:
        print(f"[bold red]{t.get('error_db_connection', error=e)}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(f"[bold red]An unexpected error occurred: {e}[/bold red]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()