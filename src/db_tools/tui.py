import io
from contextlib import redirect_stdout
from typing import Any, Dict

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.widgets import Footer, Header, RichLog, Tree

from db_tools.core.config_loader import load_config
from db_tools.core.database import get_engine, inspect_db_schema
from db_tools.core.processor import process_anonymize, process_seed
from db_tools.core.translator import Translator


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
    # --- THAY ĐỔI 1: Khởi tạo BINDINGS với placeholder tiếng Anh ---
    TITLE = "DB Tools"
    BINDINGS = [
        ("s", "seed", "Seed Data"),
        ("a", "anonymize", "Anonymize Data"),
        ("d", "toggle_dark", "Toggle Dark Mode"),
        ("q", "quit", "Quit"),
    ]
    # ----------------------------------------------------------------

    CSS_PATH = "tui.css"

    def __init__(self) -> None:
        super().__init__()
        self.t = Translator() 

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            yield Tree(self.t.get("tree_root_label"), id="schema_tree", classes="sidebar")
            yield RichLog(id="log_viewer", auto_scroll=True, wrap=True, highlight=True)
        yield Footer()

    def on_mount(self) -> None:
        # --- THAY ĐỔI 2: Cập nhật động TITLE và BINDINGS trong on_mount ---
        self.title = self.t.get("app_title") 
        
        footer = self.query_one(Footer)
        # Gán lại toàn bộ bindings đã được dịch cho Footer
        footer.bindings = [
            ("s", "seed", self.t.get("binding_seed")),
            ("a", "anonymize", self.t.get("binding_anonymize")),
            ("d", "toggle_dark", self.t.get("binding_toggle_dark")),
            ("q", "quit", self.t.get("binding_quit")),
        ]
        # -------------------------------------------------------------

        self.log_viewer = self.query_one(RichLog)
        self.write_log(f"[bold]{self.t.get('welcome_message')}[/bold]")
        self.write_log(self.t.get('key_prompt'))
        self.load_schema()

    def write_log(self, message: Any) -> None:
        if isinstance(message, str):
            self.log_viewer.write(Text.from_markup(message))
        else:
            self.log_viewer.write(message)

    def on_log_message(self, message: LogMessage) -> None:
        self.write_log(message.content)

    @work(exclusive=True, group="db_work", thread=True)
    def load_schema(self) -> None:
        self.write_log(f"-> {self.t.get('schema_loading')}")
        try:
            config = load_config()
            connection_string = config.get("connection")
            if not connection_string:
                self.write_log(f"[bold red]{self.t.get('config_not_found')}[/bold red]")
                return

            db_engine = get_engine(connection_string)
            schema = inspect_db_schema(db_engine)
            self.call_from_thread(self.update_schema_tree, schema)
        except Exception as e:
            self.write_log(f"[bold red]{self.t.get('schema_load_error')}[/bold red]")
            self.write_log(f"[red]{repr(e)}[/red]")

    def update_schema_tree(self, schema: dict) -> None:
        tree = self.query_one(Tree)
        tree.clear()
        root = tree.root
        root.expand()
        for table_name, columns in schema.items():
            table_node = root.add(f"[bold]{table_name}[/bold]")
            for col_name in columns:
                table_node.add_leaf(col_name)
        self.write_log(f"[bold green]{self.t.get('schema_loaded_success')}[/bold green]")

    @work(exclusive=True, group="db_work", thread=True)
    def run_task(self, task_function, task_name: str) -> None:
        self.write_log("-" * 50)
        self.write_log(f"-> {self.t.get('task_starting', task_name=task_name)}")
        try:
            config = load_config()
            connection_string = config.get("connection")
            db_engine = get_engine(connection_string)

            log_redirector = LogRedirect(self)
            with redirect_stdout(log_redirector):
                task_function(config, db_engine)
            
            self.write_log(f"[bold green]{self.t.get('task_finished', task_name=task_name)}[/bold green]")
        except Exception as e:
            self.write_log(f"[bold red]{self.t.get('task_error', task_name=task_name)}[/bold red]")
            self.write_log(f"[red]{repr(e)}[/red]")
            
    def action_seed(self) -> None:
        self.run_task(process_seed, self.t.get("binding_seed"))

    def action_anonymize(self) -> None:
        self.run_task(process_anonymize, self.t.get("binding_anonymize"))
        
    def action_quit(self) -> None:
        self.exit()