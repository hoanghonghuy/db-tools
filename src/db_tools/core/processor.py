# src/db_tools/core/processor.py

import random
from typing import Any, Dict, List

from rich import print
from rich.progress import track
from sqlalchemy import MetaData, Table, engine, insert, inspect, select, update

from db_tools.core import faker_manager


def _seed_table(
    db_engine: engine.Engine,
    connection, # SQLAlchemy connection object
    table_name: str,
    table_config: Dict[str, Any],
    seeded_pks: Dict[str, List[Any]], # Dictionary chứa các PK đã được seed
) -> None:
    """Hàm nội bộ để seed dữ liệu cho một bảng duy nhất."""
    metadata = MetaData()
    inspector = inspect(db_engine)

    if not inspector.has_table(table_name):
        print(f"[bold red]❌ Error: Table '{table_name}' does not exist.[/bold red]")
        return

    table = Table(table_name, metadata, autoload_with=db_engine)
    count = table_config.get("count", 10)
    columns_to_fake = table_config.get("columns", {})
    relations_config = table_config.get("relations", {})

    print(f"   - Generating {count} records for [bold magenta]'{table_name}'[/bold magenta]...")
    data_to_insert = []
    for _ in track(range(count), description=f"Generating for '{table_name}'..."):
        # Tạo dữ liệu giả từ Faker
        row_data = faker_manager.generate_fake_row(columns_to_fake)

        # Xử lý các mối quan hệ
        for fk_column, rel_info in relations_config.items():
            related_table = rel_info.get("table")
            if related_table in seeded_pks and seeded_pks[related_table]:
                # Lấy ngẫu nhiên một PK từ danh sách các PK của bảng cha
                row_data[fk_column] = random.choice(seeded_pks[related_table])

        data_to_insert.append(row_data)

    if data_to_insert:
        print(f"   - Inserting records into [bold magenta]'{table_name}'[/bold magenta]...")
        # Sử dụng `returning` để lấy lại các giá trị PK vừa được insert
        stmt = insert(table).returning(table.primary_key.columns.values()[0])
        result = connection.execute(stmt, data_to_insert)
        
        # Lưu lại các PK mới được tạo
        new_pks = result.scalars().all()
        seeded_pks[table_name] = new_pks
        
        print(f"[bold green]✅ Seeded {len(new_pks)} records into '{table_name}' successfully![/bold green]")


def process_seed(config: Dict[str, Any], db_engine: engine.Engine):
    """Hàm chính điều phối toàn bộ quá trình seeding."""
    seed_config = config.get("seed")
    if not seed_config:
        print("[yellow]No 'seed' configuration found. Skipping.[/yellow]")
        return

    print("\n[bold cyan]🌱 Starting relational data seeding process...[/bold cyan]")
    
    # Dictionary để lưu trữ các primary key đã được tạo ra
    # Ví dụ: {'users': [1, 2, 3], 'products': [101, 102]}
    seeded_primary_keys: Dict[str, List[Any]] = {}

    with db_engine.connect() as connection:
        # Lặp qua các bảng theo đúng thứ tự trong file config
        for table_name, table_config in seed_config.items():
            try:
                _seed_table(db_engine, connection, table_name, table_config, seeded_primary_keys)
            except Exception as e:
                print(f"[bold red]❌ An error occurred with table '{table_name}': {repr(e)}[/bold red]")
                # Dừng lại nếu có lỗi để đảm bảo toàn vẹn dữ liệu
                connection.rollback()
                return
        
        # Commit transaction cuối cùng nếu tất cả thành công
        connection.commit()
        print("\n[bold green]🎉 All tables seeded successfully![/bold green]")


def process_anonymize(config: Dict[str, Any], db_engine: engine.Engine):
    # Hàm này giữ nguyên không đổi
    anonymize_config = config.get("anonymize")
    if not anonymize_config:
        print("[yellow]No 'anonymize' configuration found. Skipping.[/yellow]")
        return

    print("\n[bold cyan]🎭 Starting data anonymization process...[/bold cyan]")
    metadata = MetaData()
    inspector = inspect(db_engine)

    with db_engine.connect() as connection:
        for table_name, table_config in anonymize_config.items():
            try:
                if not inspector.has_table(table_name):
                    print(f"[bold red]❌ Error: Table '{table_name}' does not exist in the database.[/bold red]")
                    continue

                table = Table(table_name, metadata, autoload_with=db_engine)
                primary_key_col = table.primary_key.columns.values()[0]
                p_keys = connection.execute(select(primary_key_col)).scalars().all()
                
                if not p_keys:
                    print(f"[yellow]   - No records found in '{table_name}'. Skipping.[/yellow]")
                    continue

                columns_to_anonymize = table_config.get("columns", {})
                
                print(f"   - Anonymizing {len(p_keys)} records...")
                
                for pk_value in track(p_keys, description=f"Anonymizing '{table_name}'..."):
                    new_fake_data = faker_manager.generate_fake_row(columns_to_anonymize)
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