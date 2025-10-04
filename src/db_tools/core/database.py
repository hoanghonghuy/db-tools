# src/db_tools/core/database.py

from typing import Any, Dict, Optional
from sqlalchemy import create_engine, engine, inspect
from rich import print

def get_engine(connection_string: str) -> Optional[engine.Engine]:
    """
    Creates a SQLAlchemy engine from a connection string.

    Args:
        connection_string: The database connection string.

    Returns:
        A SQLAlchemy Engine object, or None if connection fails.
    """
    try:
        engine = create_engine(connection_string)
        # The 'connect' method will raise an error if the connection string is invalid
        engine.connect()
        return engine
    except Exception as e:
        # Lỗi sẽ được bắt và hiển thị ở tầng gọi hàm (main.py)
        # Chúng ta sẽ re-raise (ném lại) lỗi để tầng trên xử lý
        raise ConnectionError(e)
    
def inspect_db_schema(db_engine: engine.Engine) -> Dict[str, Any]:
    """
    Sử dụng Inspector để lấy cấu trúc của database.

    Returns:
        Một dictionary chứa tên các bảng và danh sách các cột của chúng.
        Ví dụ: {'users': ['id', 'name', 'email'], 'orders': ['id', 'customer_name']}
    """
    inspector = inspect(db_engine)
    schema = {}
    table_names = inspector.get_table_names()
    for table_name in table_names:
        columns = inspector.get_columns(table_name)
        schema[table_name] = [col['name'] for col in columns]
    return schema