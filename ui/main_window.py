# sql_client/ui/main_window.py

import os
import time
import sqlite3 as sqlite
import psycopg2
from functools import partial
from PyQt6.QtWidgets import (
    QMainWindow, QTreeView, QTabWidget, QSplitter, QTextEdit, QComboBox,
    QTableView, QVBoxLayout, QWidget, QStatusBar, QToolBar, QSizePolicy,
    QPushButton, QInputDialog, QMessageBox, QMenu, QAbstractItemView,
    QDialog, QStackedWidget, QLabel, QHBoxLayout
)
from PyQt6.QtGui import QAction, QIcon, QStandardItemModel, QStandardItem, QFont, QMovie
from PyQt6.QtCore import Qt, QModelIndex, QSize, QThreadPool, QTimer

from database import RunnableQuery, QuerySignals
from ui.dialogs import PostgresConnectionDialog, SQLiteConnectionDialog


class MainWindow(QMainWindow):
    QUERY_TIMEOUT = 60000

    def __init__(self):
        super().__init__()
        self.setWindowTitle("SQL Client")
        self.setGeometry(100, 100, 1200, 800)

        self.thread_pool = QThreadPool.globalInstance()
        self.tab_timers = {}
        self.running_queries = {}

        self._create_actions()
        self._create_menu()
        self._create_centered_toolbar()

        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.setCentralWidget(main_splitter)

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status_message_label = QLabel("Ready")
        self.status.addWidget(self.status_message_label, 1)

        self.thread_status_label = QLabel()
        self.status.addPermanentWidget(self.thread_status_label)

        self.thread_check_timer = QTimer(self)
        self.thread_check_timer.timeout.connect(self._update_thread_status)
        self.thread_check_timer.start(500)
        self._update_thread_status()

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.tree = QTreeView()
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.show_context_menu)
        self.tree.clicked.connect(self.item_clicked)
        self.tree.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(['Object Explorer'])
        self.tree.setModel(self.model)

        vertical_splitter = QSplitter(Qt.Orientation.Vertical)
        vertical_splitter.addWidget(self.tree)

        self.schema_tree = QTreeView()
        self.schema_model = QStandardItemModel()
        self.schema_model.setHorizontalHeaderLabels(["Database Schema"])
        self.schema_tree.setModel(self.schema_model)
        self.schema_tree.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self.schema_tree.customContextMenuRequested.connect(
            self.show_schema_context_menu)
        vertical_splitter.addWidget(self.schema_tree)

        vertical_splitter.setSizes([240, 360])
        left_layout.addWidget(vertical_splitter)
        main_splitter.addWidget(left_panel)

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        add_tab_btn = QPushButton("New")
        add_tab_btn.clicked.connect(self.add_tab)
        self.tab_widget.setCornerWidget(add_tab_btn)
        main_splitter.addWidget(self.tab_widget)

        self.load_data()
        self.add_tab()
        main_splitter.setSizes([280, 920])
        self._apply_styles()

    def _update_thread_status(self):
        active_threads = self.thread_pool.activeThreadCount()
        max_threads = self.thread_pool.maxThreadCount()
        self.thread_status_label.setText(
            f"Threads: {active_threads} / {max_threads}")

    def _create_actions(self):
        self.exit_action = QAction(QIcon("assets/exit_icon.png"), "Exit", self)
        self.exit_action.triggered.connect(self.close)
        self.execute_action = QAction(
            QIcon("assets/execute_icon.png"), "Execute", self)
        self.execute_action.triggered.connect(self.execute_query)
        self.cancel_action = QAction(
            QIcon("assets/cancel_icon.png"), "Cancel", self)
        self.cancel_action.triggered.connect(self.cancel_current_query)
        self.cancel_action.setEnabled(False)

    def _create_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("&File")
        file_menu.addAction(self.exit_action)
        actions_menu = menubar.addMenu("&Actions")
        actions_menu.addAction(self.execute_action)
        actions_menu.addAction(self.cancel_action)

    def _create_centered_toolbar(self):
        toolbar = QToolBar("Main Toolbar")
        left_spacer = QWidget()
        left_spacer.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        right_spacer = QWidget()
        right_spacer.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(left_spacer)
        toolbar.addAction(self.exit_action)
        toolbar.addAction(self.execute_action)
        toolbar.addAction(self.cancel_action)
        toolbar.addWidget(right_spacer)
        self.addToolBar(toolbar)

    def _apply_styles(self):
        style_sheet = """
            QTableView {
                alternate-background-color: #f5f5f5;
                background-color: white;
                gridline-color: #d0d0d0;
                border: 1px solid #c0c0c0;
                font-family: Arial, sans-serif;
                font-size: 9pt;
            }
            QTableView::item { padding: 4px; }
            QTableView::item:selected { background-color: #5698d4; color: white; }
            QHeaderView::section {
                background-color: #34557C;
                color: white;
                padding: 6px;
                border: 1px solid #2a436e;
                font-weight: bold;
                font-size: 9pt;
            }
            QTableView QTableCornerButton::section {
                background-color: #34557C;
                border: 1px solid #2a436e;
            }
        """
        self.setStyleSheet(style_sheet)

    def add_tab(self):
        tab_content = QWidget(self.tab_widget)
        layout = QVBoxLayout(tab_content)
        layout.setContentsMargins(0, 0, 0, 0)
        db_combo_box = QComboBox()
        db_combo_box.setObjectName("db_combo_box")
        layout.addWidget(db_combo_box)
        self.load_joined_items(db_combo_box)

        vertical_splitter = QSplitter(Qt.Orientation.Vertical)
        text_edit = QTextEdit()
        text_edit.setPlaceholderText("Write your SQL query here...")
        text_edit.setObjectName("query_editor")
        vertical_splitter.addWidget(text_edit)

        results_container = QWidget()
        results_layout = QVBoxLayout(results_container)
        results_layout.setContentsMargins(0, 5, 0, 0)

        stacked_widget = QStackedWidget()
        stacked_widget.setObjectName("results_stacked_widget")

        table_view = QTableView()
        table_view.setObjectName("result_table")
        table_view.setAlternatingRowColors(True)
        stacked_widget.addWidget(table_view)

        spinner_overlay_widget = QWidget()
        spinner_layout = QHBoxLayout(spinner_overlay_widget)
        spinner_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        spinner_movie = QMovie("assets/spinner.gif")
        spinner_label = QLabel()
        if not spinner_movie.isValid():
            spinner_label.setText("Loading...")
        else:
            spinner_label.setMovie(spinner_movie)
        spinner_movie.setScaledSize(QSize(32, 32))
        spinner_label.setObjectName("spinner_label")
        loading_text_label = QLabel("Waiting for query to complete...")
        font = QFont()
        font.setPointSize(10)
        loading_text_label.setFont(font)
        loading_text_label.setStyleSheet("color: #555;")
        spinner_layout.addWidget(spinner_label)
        spinner_layout.addWidget(loading_text_label)
        stacked_widget.addWidget(spinner_overlay_widget)
        stacked_widget.setCurrentIndex(0)
        results_layout.addWidget(stacked_widget)

        tab_status_label = QLabel("")
        tab_status_label.setObjectName("tab_status_label")
        tab_status_label.setContentsMargins(5, 0, 5, 0)
        results_layout.addWidget(tab_status_label)

        vertical_splitter.addWidget(results_container)
        vertical_splitter.setSizes([300, 300])
        layout.addWidget(vertical_splitter)
        tab_content.setLayout(layout)

        index = self.tab_widget.addTab(
            tab_content, f"Worksheet {self.tab_widget.count() + 1}")
        self.tab_widget.setCurrentIndex(index)
        self.renumber_tabs()
        return tab_content

    def close_tab(self, index):
        tab = self.tab_widget.widget(index)
        if tab in self.running_queries:
            self.running_queries[tab].cancel()
            del self.running_queries[tab]
            if not self.running_queries:
                self.cancel_action.setEnabled(False)
        if tab in self.tab_timers:
            self.tab_timers[tab]["timer"].stop()
            if "timeout_timer" in self.tab_timers[tab]:
                self.tab_timers[tab]["timeout_timer"].stop()
            del self.tab_timers[tab]
        if self.tab_widget.count() > 1:
            self.tab_widget.removeTab(index)
            self.renumber_tabs()
        else:
            self.status.showMessage("Must keep at least one tab", 3000)

    def renumber_tabs(self):
        for i in range(self.tab_widget.count()):
            self.tab_widget.setTabText(i, f"Worksheet {i + 1}")

    def load_data(self):
        self.model.clear()
        self.model.setHorizontalHeaderLabels(["Object Explorer"])
        conn = sqlite.connect('hierarchy.db')
        c = conn.cursor()
        c.execute("SELECT id, name FROM categories")
        categories = c.fetchall()
        for cat_id, cat_name in categories:
            cat_item = QStandardItem(cat_name)
            cat_item.setData(cat_id, Qt.ItemDataRole.UserRole + 1)
            c.execute(
                "SELECT id, name FROM subcategories WHERE category_id=?", (cat_id,))
            subcats = c.fetchall()
            for subcat_id, subcat_name in subcats:
                subcat_item = QStandardItem(subcat_name)
                subcat_item.setData(subcat_id, Qt.ItemDataRole.UserRole + 1)
                c.execute("""
                    SELECT id, name, host, "database", "user", password, port, db_path
                    FROM items WHERE subcategory_id=?
                """, (subcat_id,))
                items = c.fetchall()
                for item_row in items:
                    item_id, name, host, db, user, pwd, port, db_path = item_row
                    item_item = QStandardItem(name)
                    conn_data = {
                        "id": item_id, "name": name, "host": host,
                        "database": db, "user": user, "password": pwd,
                        "port": port, "db_path": db_path
                    }
                    item_item.setData(conn_data, Qt.ItemDataRole.UserRole)
                    subcat_item.appendRow(item_item)
                cat_item.appendRow(subcat_item)
            self.model.appendRow(cat_item)
        conn.close()

    def item_clicked(self, index):
        item = self.model.itemFromIndex(index)
        depth = self.get_item_depth(item)
        self.schema_model.clear()
        self.schema_model.setHorizontalHeaderLabels(["Database Schema"])
        if depth == 3:
            conn_data = item.data(Qt.ItemDataRole.UserRole)
            if conn_data:
                if conn_data.get("host"):
                    self.status.showMessage(
                        f"Loading schema for {conn_data.get('name')}...", 3000)
                    self.load_postgres_schema(conn_data)
                elif conn_data.get("db_path"):
                    self.status.showMessage(
                        f"Loading schema for {conn_data.get('name')}...", 3000)
                    self.load_sqlite_schema(conn_data)

    def get_item_depth(self, item):
        depth = 0
        parent = item.parent()
        while parent is not None:
            depth += 1
            parent = parent.parent()
        return depth + 1

    def show_context_menu(self, pos):
        index = self.tree.indexAt(pos)
        if not index.isValid():
            return
        item = self.model.itemFromIndex(index)
        depth = self.get_item_depth(item)
        menu = QMenu()
        if depth == 1:
            add_subcat = QAction("Add Group", self)
            add_subcat.triggered.connect(lambda: self.add_subcategory(item))
            menu.addAction(add_subcat)
        elif depth == 2:
            parent_category_item = item.parent()
            if parent_category_item:
                category_name = parent_category_item.text()
                if "postgres" in category_name.lower():
                    add_pg_action = QAction(
                        "Add New PostgreSQL Connection", self)
                    add_pg_action.triggered.connect(
                        lambda: self.add_postgres_connection(item))
                    menu.addAction(add_pg_action)
                elif "sqlite" in category_name.lower():
                    add_sqlite_action = QAction(
                        "Add New SQLite Connection", self)
                    add_sqlite_action.triggered.connect(
                        lambda: self.add_sqlite_connection(item))
                    menu.addAction(add_sqlite_action)
        elif depth == 3:
            conn_data = item.data(Qt.ItemDataRole.UserRole)
            if conn_data:
                if conn_data.get("db_path"):
                    edit_action = QAction("Edit Connection", self)
                    edit_action.triggered.connect(lambda: self.edit_item(item))
                    menu.addAction(edit_action)
                elif conn_data.get("host"):
                    edit_action = QAction("Edit Connection", self)
                    edit_action.triggered.connect(
                        lambda: self.edit_pg_item(item))
                    menu.addAction(edit_action)
                delete_action = QAction("Delete Connection", self)
                delete_action.triggered.connect(lambda: self.delete_item(item))
                menu.addAction(delete_action)
        menu.exec(self.tree.viewport().mapToGlobal(pos))

    def add_subcategory(self, parent_item):
        name, ok = QInputDialog.getText(self, "New Group", "Group name:")
        if ok and name:
            conn = sqlite.connect('hierarchy.db')
            c = conn.cursor()
            parent_id = parent_item.data(Qt.ItemDataRole.UserRole+1)
            c.execute(
                "INSERT INTO subcategories (name, category_id) VALUES (?, ?)", (name, parent_id))
            conn.commit()
            conn.close()
            self.load_data()

    def add_postgres_connection(self, parent_item):
        subcat_id = parent_item.data(Qt.ItemDataRole.UserRole + 1)
        dialog = PostgresConnectionDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            try:
                conn = sqlite.connect("hierarchy.db")
                c = conn.cursor()
                c.execute("""
                    INSERT INTO items (name, subcategory_id, host, "database", "user", password, port)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (data["name"], subcat_id, data["host"], data["database"], data["user"], data["password"], data["port"]))
                conn.commit()
                conn.close()
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to save PostgreSQL connection:\n{e}")

    def add_sqlite_connection(self, parent_item):
        subcat_id = parent_item.data(Qt.ItemDataRole.UserRole + 1)
        dialog = SQLiteConnectionDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            try:
                conn = sqlite.connect("hierarchy.db")
                c = conn.cursor()
                c.execute("INSERT INTO items (name, subcategory_id, db_path) VALUES (?, ?, ?)",
                          (data["name"], subcat_id, data["db_path"]))
                conn.commit()
                conn.close()
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to save SQLite connection:\n{e}")

    def edit_item(self, item):
        conn_data = item.data(Qt.ItemDataRole.UserRole)
        if conn_data and conn_data.get("db_path"):
            dialog = SQLiteConnectionDialog(self, conn_data=conn_data)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                new_data = dialog.get_data()
                try:
                    conn = sqlite.connect("hierarchy.db")
                    c = conn.cursor()
                    c.execute("UPDATE items SET name = ?, db_path = ? WHERE id = ?",
                              (new_data["name"], new_data["db_path"], new_data["id"]))
                    conn.commit()
                    conn.close()
                    self.load_data()
                    self.refresh_all_comboboxes()
                except Exception as e:
                    QMessageBox.critical(
                        self, "Error", f"Failed to update SQLite connection:\n{e}")

    def edit_pg_item(self, item):
        conn_data = item.data(Qt.ItemDataRole.UserRole)
        if not conn_data:
            return
        dialog = PostgresConnectionDialog(self, is_editing=True)
        dialog.name_input.setText(conn_data.get("name", ""))
        dialog.host_input.setText(conn_data.get("host", ""))
        dialog.port_input.setText(str(conn_data.get("port", "")))
        dialog.db_input.setText(conn_data.get("database", ""))
        dialog.user_input.setText(conn_data.get("user", ""))
        dialog.password_input.setText(conn_data.get("password", ""))
        if dialog.exec() == QDialog.DialogCode.Accepted:
            new_data = dialog.get_data()
            try:
                conn = sqlite.connect("hierarchy.db")
                c = conn.cursor()
                c.execute("""
                UPDATE items SET name = ?, host = ?, database = ?, user = ?, password = ?, port = ? WHERE id = ?
                """, (new_data["name"], new_data["host"], new_data["database"], new_data["user"], new_data["password"], new_data["port"], conn_data["id"]))
                conn.commit()
                conn.close()
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to update PostgreSQL connection:\n{e}")

    def delete_item(self, item):
        conn_data = item.data(Qt.ItemDataRole.UserRole)
        item_id = conn_data.get("id")
        reply = QMessageBox.question(self, "Delete Connection", "Are you sure you want to delete this connection?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                conn = sqlite.connect("hierarchy.db")
                c = conn.cursor()
                c.execute("DELETE FROM items WHERE id = ?", (item_id,))
                conn.commit()
                conn.close()
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to delete item:\n{e}")

    def refresh_all_comboboxes(self):
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            combo_box = tab.findChild(QComboBox, "db_combo_box")
            if combo_box:
                current_text = combo_box.currentText()
                self.load_joined_items(combo_box)
                new_index = combo_box.findText(current_text)
                if new_index != -1:
                    combo_box.setCurrentIndex(new_index)

    def load_joined_items(self, combo_box):
        try:
            conn = sqlite.connect("hierarchy.db")
            cursor = conn.cursor()
            combo_box.clear()
            cursor.execute("""
            SELECT c.name, sc.name, i.name, i.host, i.database, i.user, i.password, i.port, i.db_path, i.id, i.usage_count
            FROM categories c
            JOIN subcategories sc ON sc.category_id = c.id
            JOIN items i ON i.subcategory_id = sc.id
            ORDER BY i.usage_count DESC, c.name, sc.name, i.name
            """)
            all_items = cursor.fetchall()
            conn.close()
            for row in all_items:
                cat_name, subcat_name, item_name, host, db, user, pwd, port, db_path, item_id, usage_count = row
                visible_text = f"{cat_name} -> {subcat_name} -> {item_name}"
                conn_data = {"id": item_id, "host": host, "database": db,
                             "user": user, "password": pwd, "port": port, "db_path": db_path}
                combo_box.addItem(visible_text, conn_data)
        except Exception as e:
            self.status.showMessage(f"Error loading connections: {e}", 4000)

    def execute_query(self):
        current_tab = self.tab_widget.currentWidget()
        if not current_tab:
            return
        if current_tab in self.running_queries:
            QMessageBox.warning(self, "Query in Progress",
                                "A query is already running in this tab.")
            return

        query_editor = current_tab.findChild(QTextEdit, "query_editor")
        db_combo_box = current_tab.findChild(QComboBox, "db_combo_box")
        tab_status_label = current_tab.findChild(QLabel, "tab_status_label")
        query = query_editor.toPlainText().strip()

        if not db_combo_box.currentText() or not query:
            self.status.showMessage("Database or query is empty", 3000)
            return

        self.status.showMessage("New query thread started...", 2000)

        progress_timer = QTimer(self)
        start_time = time.time()
        timeout_timer = QTimer(self)
        timeout_timer.setSingleShot(True)
        self.tab_timers[current_tab] = {
            "timer": progress_timer, "start_time": start_time, "timeout_timer": timeout_timer}
        progress_timer.timeout.connect(
            partial(self.update_timer_label, tab_status_label, current_tab))
        progress_timer.start(100)

        stacked_widget = current_tab.findChild(
            QStackedWidget, "results_stacked_widget")
        spinner_label = stacked_widget.findChild(QLabel, "spinner_label")
        stacked_widget.setCurrentIndex(1)
        if spinner_label and spinner_label.movie():
            spinner_label.movie().start()

        index = db_combo_box.currentIndex()
        conn_data = db_combo_box.itemData(index)
        signals = QuerySignals()
        runnable = RunnableQuery(conn_data, query, signals)
        signals.finished.connect(
            partial(self.handle_query_result, current_tab))
        signals.error.connect(partial(self.handle_query_error, current_tab))
        timeout_timer.timeout.connect(
            partial(self.handle_query_timeout, current_tab, runnable))

        self.running_queries[current_tab] = runnable
        self.cancel_action.setEnabled(True)
        self.thread_pool.start(runnable)
        timeout_timer.start(self.QUERY_TIMEOUT)

        self.status_message_label.setText("Executing query...")
        if tab_status_label:
            tab_status_label.setText("Waiting for query to complete...")

    def update_timer_label(self, label, tab):
        if not label or tab not in self.tab_timers:
            return
        elapsed = time.time() - self.tab_timers[tab]["start_time"]
        label.setText(
            f"Waiting for query to complete... Running: {elapsed:.1f} sec")

    def handle_query_result(self, target_tab, results, columns, row_count, elapsed_time, is_select_query):
        if target_tab in self.tab_timers:
            self.tab_timers[target_tab]["timer"].stop()
            self.tab_timers[target_tab]["timeout_timer"].stop()
            del self.tab_timers[target_tab]

        table_view = target_tab.findChild(QTableView, "result_table")
        tab_status_label = target_tab.findChild(QLabel, "tab_status_label")

        if is_select_query:
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(columns)
            for row in results:
                model.appendRow([QStandardItem(str(cell)) for cell in row])
            table_view.setModel(model)
            message = f"Query executed successfully | Total rows: {row_count} | Time: {elapsed_time:.2f} sec"
        else:
            table_view.setModel(QStandardItemModel())
            message = f"Command executed successfully | Rows affected: {row_count} | Time: {elapsed_time:.2f} sec"

        if tab_status_label:
            tab_status_label.setText(message)
        self.status_message_label.setText("Ready")
        self.stop_spinner(target_tab)
        self.refresh_all_comboboxes()

        if target_tab in self.running_queries:
            del self.running_queries[target_tab]
        if not self.running_queries:
            self.cancel_action.setEnabled(False)

    def handle_query_error(self, target_tab, error_message):
        if target_tab in self.tab_timers:
            self.tab_timers[target_tab]["timer"].stop()
            self.tab_timers[target_tab]["timeout_timer"].stop()
            del self.tab_timers[target_tab]

        tab_status_label = target_tab.findChild(QLabel, "tab_status_label")
        if tab_status_label:
            tab_status_label.setText(f"Error: {error_message}")
        self.status_message_label.setText("Error occurred")
        QMessageBox.critical(self, "Query Execution Error",
                             f"An error occurred:\n\n{error_message}")
        self.stop_spinner(target_tab)

        if target_tab in self.running_queries:
            del self.running_queries[target_tab]
        if not self.running_queries:
            self.cancel_action.setEnabled(False)

    def stop_spinner(self, target_tab):
        if not target_tab:
            return
        stacked_widget = target_tab.findChild(
            QStackedWidget, "results_stacked_widget")
        if stacked_widget:
            spinner_label = stacked_widget.findChild(QLabel, "spinner_label")
            if spinner_label and spinner_label.movie():
                spinner_label.movie().stop()
            stacked_widget.setCurrentIndex(0)

    def handle_query_timeout(self, tab, runnable):
        if self.running_queries.get(tab) is runnable:
            runnable.cancel()
            self.stop_spinner(tab)
            if tab in self.tab_timers:
                self.tab_timers[tab]["timer"].stop()
                del self.tab_timers[tab]
            if tab in self.running_queries:
                del self.running_queries[tab]
            if not self.running_queries:
                self.cancel_action.setEnabled(False)
            tab_status_label = tab.findChild(QLabel, "tab_status_label")
            if tab_status_label:
                tab_status_label.setText(
                    f"Error: Query Timed Out after {self.QUERY_TIMEOUT / 1000} seconds.")
            self.status_message_label.setText("Error occurred")
            QMessageBox.warning(
                self, "Query Timeout", f"The query was stopped because it ran for more than {self.QUERY_TIMEOUT / 1000} seconds.")

    def cancel_current_query(self):
        current_tab = self.tab_widget.currentWidget()
        runnable = self.running_queries.get(current_tab)
        if runnable:
            runnable.cancel()
            if current_tab in self.tab_timers:
                self.tab_timers[current_tab]["timer"].stop()
                self.tab_timers[current_tab]["timeout_timer"].stop()
                del self.tab_timers[current_tab]
            self.stop_spinner(current_tab)
            tab_status_label = current_tab.findChild(
                QLabel, "tab_status_label")
            if tab_status_label:
                tab_status_label.setText("Query cancelled by user.")
            self.status_message_label.setText("Query Cancelled")
            del self.running_queries[current_tab]
            if not self.running_queries:
                self.cancel_action.setEnabled(False)

    def load_sqlite_schema(self, conn_data):
        self.schema_model.clear()
        self.schema_model.setHorizontalHeaderLabels(["Tables & Views"])
        db_path = conn_data.get("db_path")
        if not db_path or not os.path.exists(db_path):
            self.status.showMessage(
                f"Error: SQLite DB path not found: {db_path}", 5000)
            return

        try:
            conn = sqlite.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name;")
            tables = cursor.fetchall()
            conn.close()

            for name, type in tables:
                icon = QIcon(
                    "assets/table_icon.png") if type == 'table' else QIcon("assets/view_icon.png")
                item = QStandardItem(icon, name)
                item.setEditable(False)
                item_data = {'db_type': 'sqlite', 'conn_data': conn_data}
                item.setData(item_data, Qt.ItemDataRole.UserRole)
                self.schema_model.appendRow(item)

            try:
                self.schema_tree.expanded.disconnect()
            except TypeError:
                pass

        except Exception as e:
            self.status.showMessage(f"Error loading SQLite schema: {e}", 5000)

    def load_postgres_schema(self, conn_data):
        try:
            self.schema_model.clear()
            self.schema_model.setHorizontalHeaderLabels(["Schemas"])
            self.pg_conn = psycopg2.connect(
                host=conn_data["host"], database=conn_data["database"],
                user=conn_data["user"], password=conn_data["password"],
                port=int(conn_data["port"])
            )
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY schema_name;
            """)
            schemas = cursor.fetchall()
            for (schema_name,) in schemas:
                schema_item = QStandardItem(
                    QIcon("assets/schema_icon.png"), schema_name)
                schema_item.setEditable(False)
                item_data = {'db_type': 'postgres',
                             'schema_name': schema_name, 'conn_data': conn_data}
                schema_item.setData(item_data, Qt.ItemDataRole.UserRole)
                schema_item.appendRow(QStandardItem("Loading..."))
                self.schema_model.appendRow(schema_item)

            try:
                self.schema_tree.expanded.disconnect()
            except TypeError:
                pass
            self.schema_tree.expanded.connect(self.load_tables_on_expand)
        except Exception as e:
            self.status.showMessage(f"Error loading schemas: {e}", 5000)

    def show_schema_context_menu(self, position):
        index = self.schema_tree.indexAt(position)
        if not index.isValid():
            return

        item = self.schema_model.itemFromIndex(index)
        item_data = item.data(Qt.ItemDataRole.UserRole)

        is_sqlite_table = item_data and item_data.get('db_type') == 'sqlite'
        is_postgres_table = item_data and item.parent(
        ) and item_data.get('db_type') == 'postgres'

        if not (is_sqlite_table or is_postgres_table):
            return

        table_name = item.text()
        menu = QMenu()
        view_menu = menu.addMenu("View/Edit Data")

        query_all_action = QAction("Query all rows from Table", self)
        query_all_action.triggered.connect(lambda: self.query_table_rows(
            item_data, table_name, limit=None, execute_now=True))
        view_menu.addAction(query_all_action)

        preview_100_action = QAction("Preview first 100 rows", self)
        preview_100_action.triggered.connect(lambda: self.query_table_rows(
            item_data, table_name, limit=100, execute_now=True))
        view_menu.addAction(preview_100_action)

        last_100_action = QAction("Show last 100 rows", self)
        last_100_action.triggered.connect(lambda: self.query_table_rows(
            item_data, table_name, limit=100, order='desc', execute_now=True))
        view_menu.addAction(last_100_action)

        query_tool_action = QAction("Query Tool", self)
        query_tool_action.triggered.connect(
            lambda: self.open_query_tool_for_table(item_data, table_name))
        menu.addAction(query_tool_action)

        menu.exec(self.schema_tree.viewport().mapToGlobal(position))

    def open_query_tool_for_table(self, item_data, table_name):
        self.query_table_rows(item_data, table_name,
                              limit=None, execute_now=False)

    def query_table_rows(self, item_data, table_name, limit=None, order='asc', execute_now=True):
        if not item_data:
            return

        conn_data = item_data.get('conn_data')
        new_tab = self.add_tab()
        query_editor = new_tab.findChild(QTextEdit, "query_editor")
        db_combo_box = new_tab.findChild(QComboBox, "db_combo_box")

        if not query_editor or not db_combo_box:
            return

        required_conn_id = conn_data.get('id')
        for i in range(db_combo_box.count()):
            data = db_combo_box.itemData(i)
            if data and data.get('id') == required_conn_id:
                db_combo_box.setCurrentIndex(i)
                break

        if not execute_now:
            query_editor.setPlainText("")
            return

        db_type = item_data.get('db_type')
        full_query = ""

        if db_type == 'postgres':
            schema_name = item_data.get('schema_name')
            base_query = f'SELECT * FROM "{schema_name}"."{table_name}"'
            order_col = '1'
        elif db_type == 'sqlite':
            base_query = f'SELECT * FROM "{table_name}"'
            order_col = 'rowid'
            try:
                conn = sqlite.connect(conn_data["db_path"])
                cursor = conn.cursor()
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                table_info = cursor.fetchall()
                conn.close()
                pk_cols = [info[1] for info in table_info if info[5] > 0]
                if pk_cols:
                    order_col = f'"{pk_cols[0]}"'
            except Exception as e:
                print(
                    f"Could not determine PK for table '{table_name}': {e}. Defaulting to 'rowid'.")
        else:
            return

        if limit and order == 'desc':
            full_query = f"SELECT * FROM ({base_query} ORDER BY {order_col} DESC LIMIT {limit}) AS last_100_subquery ORDER BY {order_col} ASC"
        elif limit:
            full_query = f"{base_query} LIMIT {limit}"
        else:
            full_query = base_query

        query_editor.setPlainText(full_query)
        self.execute_query()

    def load_tables_on_expand(self, index: QModelIndex):
        item = self.schema_model.itemFromIndex(index)
        if not item or (item.rowCount() > 0 and item.child(0).text() != "Loading..."):
            return

        item.removeRows(0, item.rowCount())
        item_data = item.data(Qt.ItemDataRole.UserRole)
        schema_name = item_data.get('schema_name')

        try:
            cursor = self.pg_conn.cursor()
            cursor.execute("""
                SELECT table_name, table_type FROM information_schema.tables
                WHERE table_schema = %s ORDER BY table_type, table_name;
            """, (schema_name,))
            tables = cursor.fetchall()
            for (table_name, table_type) in tables:
                icon_path = "assets/table_icon.png" if "TABLE" in table_type else "assets/view_icon.png"
                table_item = QStandardItem(QIcon(icon_path), table_name)
                table_item.setEditable(False)
                table_item.setData(item_data, Qt.ItemDataRole.UserRole)
                item.appendRow(table_item)
        except Exception as e:
            self.status.showMessage(f"Error expanding schema: {e}", 5000)
