# src/db_tools/core/processor.py

from typing import Any, Dict

from faker import Faker
from rich import print
from rich.progress import track
from sqlalchemy import MetaData, Table, engine, insert


# Khởi tạo một đối tượng Faker duy nhất để tái sử dụng
faker = Faker()

def _generate_fake_data(columns: Dict[str, str]) -> Dict[str, Any]:
    """Tạo một dòng dữ liệu giả dựa trên cấu hình cột."""
    row_data = {}
    for col_name, faker_provider in columns.items():
        # getattr(faker, 'name')() sẽ tương đương với faker.name()
        # Đây là cách gọi hàm của Faker một cách linh động từ tên provider (string)
        if hasattr(faker, faker_provider):
            row_data[col_name] = getattr(faker, faker_provider)()
        else:
            # Bỏ qua nếu provider không hợp lệ, hoặc có thể in cảnh báo
            print(f"[yellow]Warning: Faker provider '{faker_provider}' not found for column '{col_name}'. Skipping.[/yellow]")
    return row_data


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
                # 1. Dùng SQLAlchemy để "soi" cấu trúc của bảng từ database
                print(f"   - Reflecting table structure for [bold magenta]'{table_name}'[/bold magenta]...")
                table = Table(table_name, metadata, autoload_with=db_engine)

                count = table_config.get("count", 10)
                columns_to_fake = table_config.get("columns", {})
                
                # 2. Tạo danh sách dữ liệu giả
                print(f"   - Generating {count} fake records...")
                data_to_insert = []
                # Dùng track của Rich để có progress bar đẹp mắt
                for _ in track(range(count), description=f"Generating for '{table_name}'..."):
                    fake_row = _generate_fake_data(columns_to_fake)
                    data_to_insert.append(fake_row)

                # 3. Insert toàn bộ dữ liệu vào bảng trong một lần
                if data_to_insert:
                    print(f"   - Inserting records into [bold magenta]'{table_name}'[/bold magenta]...")
                    stmt = insert(table)
                    connection.execute(stmt, data_to_insert)
                    connection.commit() # Commit transaction
                    print(f"[bold green]✅ Seeded {len(data_to_insert)} records into '{table_name}' successfully![/bold green]")

            except Exception as e:
                print(f"[bold red]❌ An error occurred with table '{table_name}': {e}[/bold red]")