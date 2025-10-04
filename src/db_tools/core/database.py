from typing import Any, Dict, Optional

from rich import print
from sqlalchemy import create_engine, engine, inspect, select


def get_engine(connection_string: str) -> Optional[engine.Engine]:
    """Tạo một SQLAlchemy engine từ chuỗi kết nối."""
    try:
        engine = create_engine(connection_string)
        # Lệnh connect() sẽ tự kiểm tra tính hợp lệ của chuỗi kết nối
        connection = engine.connect()
        connection.close() # Đóng kết nối ngay sau khi kiểm tra
        return engine
    except Exception as e:
        raise ConnectionError(e)


def ping_database(db_engine: engine.Engine) -> str:
    """
    Kiểm tra kết nối và trả về loại database.
    Hoạt động trên mọi CSDL được SQLAlchemy hỗ trợ.
    """
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
    from sqlalchemy import MetaData, Table, func

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=db_engine)
    
    with db_engine.connect() as connection:
        stmt = select(func.count()).select_from(table)
        row_count = connection.execute(stmt).scalar_one()
        return row_count