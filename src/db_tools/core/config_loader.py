from pathlib import Path
from typing import Any, Dict

import yaml
from rich import print

DEFAULT_CONFIG_FILE = "db_tools.yml"

def load_config() -> Dict[str, Any]:
    """
    Tải file cấu hình db_tools.yml.
    Trả về một dictionary rỗng nếu file không tồn tại.
    """
    config_path = Path(DEFAULT_CONFIG_FILE)
    if not config_path.is_file():
        return {} # Trả về rỗng thay vì báo lỗi

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
            return config_data if config_data else {}
    except yaml.YAMLError as e:
        print(f"[bold red]Error parsing YAML file: {e}[/bold red]")
        return {}