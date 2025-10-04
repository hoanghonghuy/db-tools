# src/db_tools/core/faker_manager.py

from typing import Any, Dict, List
from faker import Faker
from rich import print

# Khởi tạo một đối tượng Faker duy nhất để toàn bộ module sử dụng
_faker = Faker()

def generate_fake_row(columns_config: Dict[str, str]) -> Dict[str, Any]:
    """
    Tạo một dòng (dictionary) dữ liệu giả dựa trên cấu hình các cột.

    Args:
        columns_config: Dictionary định nghĩa các cột và faker provider tương ứng.

    Returns:
        Một dictionary đại diện cho một dòng dữ liệu.
    """
    row_data = {}
    for col_name, faker_provider in columns_config.items():
        # getattr(_faker, 'name')() sẽ tương đương với _faker.name()
        if hasattr(_faker, faker_provider):
            row_data[col_name] = getattr(_faker, faker_provider)()
        else:
            print(f"[yellow]Warning: Faker provider '{faker_provider}' not found for column '{col_name}'. Skipping.[/yellow]")
    return row_data

def generate_bulk_data(count: int, columns_config: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Tạo một danh sách các dòng dữ liệu giả.

    Args:
        count: Số lượng dòng cần tạo.
        columns_config: Cấu hình cho các cột.

    Returns:
        Một list các dictionary, mỗi dictionary là một dòng dữ liệu.
    """
    return [generate_fake_row(columns_config) for _ in range(count)]