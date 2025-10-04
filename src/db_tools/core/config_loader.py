# src/db_tools/core/config_loader.py

from pathlib import Path
from typing import Any, Dict

import yaml
from rich import print

from db_tools.core.translator import t

DEFAULT_CONFIG_FILE = "db_tools.yml"

def load_config() -> Dict[str, Any]:
    """
    Finds, loads, and validates the db_tools.yml configuration file.

    Returns:
        A dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the configuration file cannot be found.
        yaml.YAMLError: If the configuration file is malformed.
    """
    config_path = Path(DEFAULT_CONFIG_FILE)
    if not config_path.is_file():
        # Lỗi sẽ được xử lý ở tầng gọi hàm (main.py)
        raise FileNotFoundError(f"Configuration file not found at {config_path.resolve()}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        # Xử lý trường hợp file YAML bị lỗi cú pháp
        print(f"[bold red]Error parsing YAML file: {e}[/bold red]")
        raise