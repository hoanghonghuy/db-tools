# tests/test_faker_manager.py

# Dòng này quan trọng để Python tìm thấy module trong thư mục src
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from db_tools.core import faker_manager

def test_generate_fake_row_returns_dict():
    """
    Kiểm tra xem hàm có trả về một dictionary hay không.
    """
    config = {"name": "name"}
    result = faker_manager.generate_fake_row(config)
    assert isinstance(result, dict)

def test_generate_fake_row_has_correct_keys():
    """
    Kiểm tra xem dictionary trả về có đủ các key như trong config không.
    """
    config = {"name": "name", "email": "email"}
    result = faker_manager.generate_fake_row(config)
    assert "name" in result
    assert "email" in result

def test_generate_fake_row_handles_invalid_provider():
    """
    Kiểm tra xem hàm có bỏ qua một cách an toàn khi provider không tồn tại.
    """
    # 'non_existent_provider' không có trong Faker
    config = {"name": "name", "invalid_field": "non_existent_provider"}
    result = faker_manager.generate_fake_row(config)
    assert "name" in result
    assert "invalid_field" not in result # Phải không có key này trong kết quả

def test_generate_bulk_data():
    """
    Kiểm tra hàm tạo nhiều dòng dữ liệu.
    """
    count = 5
    config = {"address": "address"}
    result = faker_manager.generate_bulk_data(count, config)
    assert isinstance(result, list)
    assert len(result) == count
    assert isinstance(result[0], dict)
    assert "address" in result[0]