# sql_client/ui/dialogs.py

import psycopg2
import sqlite3 as sqlite
from PyQt6.QtWidgets import (
    QDialog, QLineEdit, QFormLayout, QPushButton, QHBoxLayout,
    QVBoxLayout, QMessageBox, QFileDialog
)


class PostgresConnectionDialog(QDialog):
    def __init__(self, parent=None, is_editing=False):
        super().__init__(parent)
        self.setWindowTitle("New PostgreSQL Connection")

        self.name_input = QLineEdit()
        self.host_input = QLineEdit()
        self.port_input = QLineEdit()
        self.db_input = QLineEdit()
        self.user_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)

        form = QFormLayout()
        form.addRow("Connection Name:", self.name_input)
        form.addRow("Host:", self.host_input)
        form.addRow("Port:", self.port_input)
        form.addRow("Database:", self.db_input)
        form.addRow("User:", self.user_input)
        form.addRow("Password:", self.password_input)

        self.test_btn = QPushButton("Test Connection")
        self.test_btn.clicked.connect(self.test_connection)
        self.save_btn = QPushButton("Update" if is_editing else "Save")
        self.save_btn.clicked.connect(self.save_connection)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)

        button_layout = QHBoxLayout()
        button_layout.addWidget(self.test_btn)
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_btn)
        button_layout.addWidget(self.save_btn)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addLayout(button_layout)
        self.setLayout(layout)

    def test_connection(self):
        try:
            conn = psycopg2.connect(
                host=self.host_input.text(),
                port=int(self.port_input.text()),
                database=self.db_input.text(),
                user=self.user_input.text(),
                password=self.password_input.text()
            )
            conn.close()
            QMessageBox.information(self, "Success", "Connection successful!")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to connect:\n{e}")

    def save_connection(self):
        if not self.name_input.text().strip():
            QMessageBox.warning(self, "Missing Info",
                                "Connection name is required.")
            return
        self.accept()

    def get_data(self):
        return {
            "name": self.name_input.text(),
            "host": self.host_input.text(),
            "port": self.port_input.text(),
            "database": self.db_input.text(),
            "user": self.user_input.text(),
            "password": self.password_input.text()
        }


class SQLiteConnectionDialog(QDialog):
    def __init__(self, parent=None, conn_data=None):
        super().__init__(parent)
        self.conn_data = conn_data
        is_editing = self.conn_data is not None

        self.setWindowTitle(
            "Edit SQLite Connection" if is_editing else "New SQLite Connection")
        self.name_input = QLineEdit()
        self.path_input = QLineEdit()

        form = QFormLayout()
        form.addRow("Connection Name:", self.name_input)
        form.addRow("Database Path:", self.path_input)

        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self.browse_file)
        self.create_btn = QPushButton("Create New DB")
        self.create_btn.clicked.connect(self.create_new_db)

        path_layout = QHBoxLayout()
        path_layout.addWidget(self.browse_btn)
        path_layout.addWidget(self.create_btn)
        form.addRow("", path_layout)

        if is_editing:
            self.name_input.setText(self.conn_data.get("name", ""))
            self.path_input.setText(self.conn_data.get("db_path", ""))

        self.save_btn = QPushButton("Update" if is_editing else "Save")
        self.save_btn.clicked.connect(self.save_connection)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)

        button_layout = QHBoxLayout()
        button_layout.addWidget(self.cancel_btn)
        button_layout.addStretch()
        button_layout.addWidget(self.save_btn)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addLayout(button_layout)
        self.setLayout(layout)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select SQLite DB", "", "SQLite Database (*.db *.sqlite *.sqlite3)")
        if file_path:
            self.path_input.setText(file_path)

    def create_new_db(self):
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Create New SQLite DB", "", "SQLite Database (*.db *.sqlite *.sqlite3)")
        if file_path:
            try:
                conn = sqlite.connect(file_path)
                conn.close()
                self.path_input.setText(file_path)
                QMessageBox.information(
                    self, "Success", f"Database created successfully at:\n{file_path}")
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Could not create database:\n{e}")

    def save_connection(self):
        if not self.name_input.text().strip() or not self.path_input.text().strip():
            QMessageBox.warning(self, "Missing Info",
                                "Both fields are required.")
            return
        self.accept()

    def get_data(self):
        return {
            "name": self.name_input.text(),
            "db_path": self.path_input.text(),
            "id": self.conn_data.get("id") if self.conn_data else None
        }
