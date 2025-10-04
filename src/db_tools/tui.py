# src/db_tools/tui.py

import io
import sys
from contextlib import redirect_stdout

from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, Log

# Import các logic cốt lõi đã có của chúng ta
from db_tools.core.config_loader import load_config
from db_tools.core.database import get_engine
from db_tools.core.processor import process_anonymize, process_seed


class DbToolsApp(App):
    """Một ứng dụng TUI để quản lý các tác vụ database."""

    TITLE = "DB Tools Interactive"
    CSS_PATH = None 

    # Định nghĩa các phím tắt
    BINDINGS = [
        ("s", "seed", "🌱 Seed Data"),
        ("a", "anonymize", "🎭 Anonymize Data"),
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        """Tạo ra các thành phần giao diện."""
        yield Header()
        yield Log(id="log_viewer", auto_scroll=True)
        yield Footer()

    def on_mount(self) -> None:
        """Được gọi khi ứng dụng khởi động."""
        log_viewer = self.query_one(Log)
        log_viewer.write_line("🚀 Welcome to DB Tools Interactive Mode!")
        log_viewer.write_line("Press 's' to seed or 'a' to anonymize data.")

    def _run_task(self, task_function, task_name: str):
        """Hàm helper để chạy một tác vụ và hiển thị output ra Log."""
        log_viewer = self.query_one(Log)
        log_viewer.write_line("-" * 50)
        log_viewer.write_line(f"▶️  Starting task: [bold]{task_name}[/bold]...")
        
        try:
            # Tải config và tạo engine
            config = load_config()
            connection_string = config.get("connection")
            if not connection_string:
                log_viewer.write_line("[bold red]Error: 'connection' string not found.[/bold red]")
                return

            db_engine = get_engine(connection_string)

            # Chuyển hướng stdout để bắt output từ các hàm print()
            string_io = io.StringIO()
            with redirect_stdout(string_io):
                task_function(config, db_engine)
            
            output = string_io.getvalue()
            log_viewer.write(output)
            log_viewer.write_line(f"✅ Task [bold]{task_name}[/bold] finished.")

        except Exception as e:
            log_viewer.write_line(f"❌ An error occurred during task [bold]{task_name}[/bold]:")
            log_viewer.write_line(f"[bold red]{repr(e)}[/bold red]")

    # Các hàm action tương ứng với BINDINGS
    def action_seed(self) -> None:
        """Chạy tác vụ seeding."""
        self._run_task(process_seed, "Seed")

    def action_anonymize(self) -> None:
        """Chạy tác vụ ẩn danh hóa."""
        self._run_task(process_anonymize, "Anonymize")

    def action_quit(self) -> None:
        """Thoát ứng dụng."""
        self.exit()