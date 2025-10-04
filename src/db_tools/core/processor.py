from typing import Any, Dict

from rich import print
from rich.progress import track
from sqlalchemy import MetaData, Table, engine, insert, select, update

# Import module faker_manager mới của chúng ta
from db_tools.core import faker_manager


def process_seed(config: Dict[str, Any], db_engine: engine.Engine):
    """
    Thực thi tác vụ seeding dữ liệu.
    """
    seed_config = config.get("seed")
    if not seed_config:
        print("[yellow]No 'seed' configuration found in db_tools.yml. Skipping.[/yellow]")
        return

    print("\n[bold cyan]🌱 Starting data seeding process...[/bold cyan]")
    metadata = MetaData()

    with db_engine.connect() as connection:
        for table_name, table_config in seed_config.items():
            try:
                print(f"   - Reflecting table structure for [bold magenta]'{table_name}'[/bold magenta]...")
                table = Table(table_name, metadata, autoload_with=db_engine)

                count = table_config.get("count", 10)
                columns_to_fake = table_config.get("columns", {})

                print(f"   - Generating {count} fake records...")
                data_to_insert = []
                for _ in track(range(count), description=f"Generating for '{table_name}'..."):
                    row = faker_manager.generate_fake_row(columns_to_fake)
                    data_to_insert.append(row)

                if data_to_insert:
                    print(f"   - Inserting records into [bold magenta]'{table_name}'[/bold magenta]...")
                    stmt = insert(table)
                    connection.execute(stmt, data_to_insert)
                    connection.commit()
                    print(f"[bold green]✅ Seeded {len(data_to_insert)} records into '{table_name}' successfully![/bold green]")

            except Exception as e:
                print(f"[bold red]❌ An error occurred with table '{table_name}': {repr(e)}[/bold red]")


def process_anonymize(config: Dict[str, Any], db_engine: engine.Engine):
    """
    Thực thi tác vụ ẩn danh hóa dữ liệu (phiên bản sửa lỗi, chạy đúng).
    """
    anonymize_config = config.get("anonymize")
    if not anonymize_config:
        print("[yellow]No 'anonymize' configuration found in db_tools.yml. Skipping.[/yellow]")
        return

    print("\n[bold cyan]🎭 Starting data anonymization process...[/bold cyan]")
    metadata = MetaData()

    with db_engine.connect() as connection:
        for table_name, table_config in anonymize_config.items():
            try:
                print(f"   - Reflecting table structure for [bold magenta]'{table_name}'[/bold magenta]...")
                table = Table(table_name, metadata, autoload_with=db_engine)
                
                primary_key_col = table.primary_key.columns.values()[0]

                print(f"   - Fetching primary keys from '{table_name}'...")
                p_keys = connection.execute(select(primary_key_col)).scalars().all()
                
                if not p_keys:
                    print(f"[yellow]   - No records found in '{table_name}'. Skipping.[/yellow]")
                    continue

                columns_to_anonymize = table_config.get("columns", {})
                
                print(f"   - Anonymizing {len(p_keys)} records...")
                
                for pk_value in track(p_keys, description=f"Anonymizing '{table_name}'..."):
                    # Tạo dữ liệu giả mới CHỈ cho các cột cần ẩn danh
                    new_fake_data = faker_manager.generate_fake_row(columns_to_anonymize)
                    
                    # Tạo và thực thi câu lệnh UPDATE cho TỪNG dòng
                    # Đây là cách làm chính xác để tránh lỗi UNIQUE constraint
                    stmt = (
                        update(table)
                        .where(primary_key_col == pk_value)
                        .values(**new_fake_data)
                    )
                    connection.execute(stmt)
                
                connection.commit()
                print(f"[bold green]✅ Anonymized {len(p_keys)} records in '{table_name}' successfully![/bold green]")

            except Exception as e:
                print(f"[bold red]❌ An error occurred with table '{table_name}': {repr(e)}[/bold red]")