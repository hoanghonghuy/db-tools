import io
import sys
from contextlib import redirect_stdout
from typing import Dict

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Log, Tree

# Import các logic cốt lõi
from db_tools.core.config_loader import load_config
from db_tools.core.database import get_engine, inspect_db_schema
from db_tools.core.processor import process_anonymize, process_seed


class DbToolsApp(App):
    """Một ứng dụng TUI để quản lý các tác vụ database."""

    TITLE = "DB Tools Interactive"
    CSS_PATH = None

    BINDINGS = [
        ("s", "seed", "🌱 Seed Data"),
        ("a", "anonymize", "🎭 Anonymize Data"),
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        """Tạo ra các thành phần giao diện chính."""
        yield Header()
        with Horizontal():
            yield Tree("🗂️ Database Schema", id="schema_tree", classes="sidebar")
            yield Log(id="log_viewer", auto_scroll=True)
        yield Footer()

    def on_mount(self) -> None:
        """Được gọi một lần khi ứng dụng khởi động."""
        self.log_viewer = self.query_one(Log)
        self.write_log("🚀 Welcome to DB Tools Interactive Mode!")
        self.write_log("Press 's' to seed or 'a' to anonymize data.")
        self.load_schema()

    def write_log(self, message: str) -> None:
        """Ghi log an toàn vào Log widget."""
        self.log_viewer.write_line(message)

    # --- Tác vụ nền (Workers) ---
    @work(exclusive=True, group="db_work", thread=True)
    def load_schema(self) -> None:
        """Kết nối tới DB và lấy schema trong một luồng nền."""
        self.write_log("🔄 Connecting to database and loading schema...")
        try:
            config = load_config()
            connection_string = config.get("connection")
            if not connection_string:
                self.write_log("[bold red]Error: 'connection' string not found.[/bold red]")
                return

            db_engine = get_engine(connection_string)
            schema = inspect_db_schema(db_engine)
            
            self.call_from_thread(self.update_schema_tree, schema)
            
        except Exception as e:
            self.write_log("[bold red]❌ Failed to connect or inspect database:[/bold red]")
            self.write_log(f"[red]{repr(e)}[/red]")

    def update_schema_tree(self, schema: dict) -> None:
        """Cập nhật Tree widget với thông tin schema (chạy trong luồng chính)."""
        tree = self.query_one(Tree)
        tree.clear()
        root = tree.root
        root.expand()
        
        for table_name, columns in schema.items():
            table_node = root.add(f"📄 [b]{table_name}[/b]")
            for col_name in columns:
                table_node.add_leaf(f"• {col_name}")
        
        self.write_log("[bold green]✅ Schema loaded successfully.[/bold green]")

    # --- Chạy tác vụ chính ---
    @work(exclusive=True, group="db_work", thread=True)
    def run_task(self, task_function, task_name: str) -> None:
        """Hàm chạy tác vụ seed/anonymize trong một luồng nền."""
        self.write_log("-" * 50)
        self.write_log(f"▶️  Starting task: [bold]{task_name}[/bold]...")
        
        try:
            config = load_config()
            connection_string = config.get("connection")
            db_engine = get_engine(connection_string)

            string_io = io.StringIO()
            with redirect_stdout(string_io):
                task_function(config, db_engine)
            
            output = string_io.getvalue()
            self.write_log(output) # Dùng self.write_log
            self.write_log(f"✅ Task [bold]{task_name}[/bold] finished.")

        except Exception as e:
            self.write_log(f"❌ An error occurred during task [bold]{task_name}[/bold]:")
            self.write_log(f"[bold red]{repr(e)}[/bold red]")
            
    # --- Xử lý các phím tắt ---
    def action_seed(self) -> None:
        """Chạy tác vụ seeding."""
        self.run_task(process_seed, "Seed")

    def action_anonymize(self) -> None:
        """Chạy tác vụ ẩn danh hóa."""
        self.run_task(process_anonymize, "Anonymize")

    def action_quit(self) -> None:
        """Thoát ứng dụng."""
        self.exit()