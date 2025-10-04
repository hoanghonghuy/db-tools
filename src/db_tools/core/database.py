# src/db_tools/core/database.py

from typing import Optional
from sqlalchemy import create_engine, engine
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