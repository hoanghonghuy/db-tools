from typing import Any, Dict, List, Optional, Tuple

from rich import print
# --- THAY ĐỔI: Thêm MetaData, Table, func vào đây ---
from sqlalchemy import (MetaData, Table, create_engine, engine, func, inspect,
                        select)


def get_engine(connection_string: str) -> Optional[engine.Engine]:
    """Tạo một SQLAlchemy engine từ chuỗi kết nối."""
    try:
        engine = create_engine(connection_string)
        connection = engine.connect()
        connection.close()
        return engine
    except Exception as e:
        raise ConnectionError(e)


def ping_database(db_engine: engine.Engine) -> str:
    """Kiểm tra kết nối và trả về loại database."""
    with db_engine.connect() as connection:
        connection.execute(select(1))
    return db_engine.dialect.name


def inspect_db_schema(db_engine: engine.Engine) -> Dict[str, Any]:
    """Sử dụng Inspector để lấy cấu trúc của database."""
    inspector = inspect(db_engine)
    schema = {}
    table_names = inspector.get_table_names()
    for table_name in table_names:
        columns = inspector.get_columns(table_name)
        schema[table_name] = [col['name'] for col in columns]
    return schema


def get_table_row_count(db_engine: engine.Engine, table_name: str) -> int:
    """Đếm số lượng dòng trong một bảng cụ thể."""
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=db_engine)
    
    with db_engine.connect() as connection:
        stmt = select(func.count()).select_from(table)
        row_count = connection.execute(stmt).scalar_one()
        return row_count


def get_table_preview_data(
    db_engine: engine.Engine, table_name: str, limit: int = 20
) -> Tuple[List[str], List[tuple]]:
    """Lấy một vài dòng dữ liệu đầu tiên từ bảng để xem trước."""
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=db_engine)
    
    with db_engine.connect() as connection:
        stmt = select(table).limit(limit)
        result = connection.execute(stmt)
        column_names = list(result.keys())
        rows = result.fetchall()
        return column_names, rows