# src/db_tools/tui.py (Phiên bản hoàn thiện)

import io
import sys
from contextlib import redirect_stdout
from typing import Any, Dict

# --- THAY ĐỔI 1: Import thêm Text ---
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.widgets import Footer, Header, RichLog, Tree

# Import các logic cốt lõi
from db_tools.core.config_loader import load_config
from db_tools.core.database import get_engine, inspect_db_schema
from db_tools.core.processor import process_anonymize, process_seed


class LogMessage(Message):
    def __init__(self, content: Any) -> None:
        self.content = content
        super().__init__()


class LogRedirect(io.StringIO):
    def __init__(self, app_ref: App):
        self.app_ref = app_ref
        super().__init__()

    def write(self, s: str) -> int:
        self.app_ref.post_message(LogMessage(s))
        return len(s)


class DbToolsApp(App):
    TITLE = "DB Tools Interactive"
    CSS_PATH = None

    BINDINGS = [
        ("s", "seed", "🌱 Seed Data"),
        ("a", "anonymize", "🎭 Anonymize Data"),
        ("d", "toggle_dark", "Toggle dark mode"),
        ("q", "quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            yield Tree("🗂️ Database Schema", id="schema_tree", classes="sidebar")
            yield RichLog(id="log_viewer", auto_scroll=True, wrap=True, highlight=True)
        yield Footer()

    def on_mount(self) -> None:
        self.log_viewer = self.query_one(RichLog)
        self.write_log("🚀 Welcome to DB Tools Interactive Mode!")
        self.write_log("Press 's' to seed or 'a' to anonymize data.")
        self.load_schema()

    # --- THAY ĐỔI 2: Sửa lại hàm write_log ---
    def write_log(self, message: Any) -> None:
        """Ghi log vào RichLog, có diễn giải markup cho string."""
        if isinstance(message, str):
            # Dùng Text.from_markup để Rich diễn giải các thẻ [bold]...[/]
            self.log_viewer.write(Text.from_markup(message))
        else:
            # Nếu là đối tượng Rich khác (VD: Table), ghi trực tiếp
            self.log_viewer.write(message)

    def on_log_message(self, message: LogMessage) -> None:
        self.write_log(message.content)

    @work(exclusive=True, group="db_work", thread=True)
    def load_schema(self) -> None:
        # ... (phần còn lại của file giữ nguyên)
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
        tree = self.query_one(Tree)
        tree.clear()
        root = tree.root
        root.expand()
        for table_name, columns in schema.items():
            table_node = root.add(f"📄 [b]{table_name}[/b]")
            for col_name in columns:
                table_node.add_leaf(f"• {col_name}")
        self.write_log("[bold green]✅ Schema loaded successfully.[/bold green]")

    @work(exclusive=True, group="db_work", thread=True)
    def run_task(self, task_function, task_name: str) -> None:
        self.write_log("-" * 50)
        self.write_log(f"▶️  Starting task: [bold]{task_name}[/bold]...")
        try:
            config = load_config()
            connection_string = config.get("connection")
            db_engine = get_engine(connection_string)

            log_redirector = LogRedirect(self)
            with redirect_stdout(log_redirector):
                task_function(config, db_engine)
            
            self.write_log(f"✅ Task [bold]{task_name}[/bold] finished.")
        except Exception as e:
            self.write_log(f"❌ An error occurred during task [bold]{task_name}[/bold]:")
            self.write_log(f"[bold red]{repr(e)}[/bold red]")
            
    def action_seed(self) -> None:
        self.run_task(process_seed, "Seed")

    def action_anonymize(self) -> None:
        self.run_task(process_anonymize, "Anonymize")
        
    def action_quit(self) -> None:
        self.exit()