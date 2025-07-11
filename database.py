# sql_client/database.py

import os
import time
import psycopg2
import sqlite3 as sqlite
from PyQt6.QtCore import QObject, pyqtSignal, QRunnable
import threading  # threading ইম্পোর্ট করা হয়েছে


class QuerySignals(QObject):
    """
    কোয়েরি এক্সিকিউশনের জন্য সিগন্যাল ক্লাস।
    """
    finished = pyqtSignal(list, list, int, float, bool)
    error = pyqtSignal(str)


class RunnableQuery(QRunnable):
    """
    থ্রেডে ডাটাবেস কোয়েরি চালানোর জন্য Runnable worker।
    """

    def __init__(self, conn_data, query, signals):
        super().__init__()
        self.conn_data = conn_data
        self.query = query
        self.signals = signals
        self._is_cancelled = False
        self.conn = None
        self.lock = threading.Lock()  # থ্রেড-সেফ অ্যাক্সেসের জন্য লক

    def cancel(self):
        """
        থ্রেড-সেফভাবে কোয়েরি বাতিল করার জন্য মেথড।
        """
        with self.lock:
            self._is_cancelled = True
            if self.conn:
                try:
                    # psycopg2 কানেকশনের জন্য .cancel() মেথড আছে
                    if hasattr(self.conn, 'cancel'):
                        self.conn.cancel()
                    # sqlite3 কানেকশনের জন্য .interrupt() মেথড আছে
                    elif hasattr(self.conn, 'interrupt'):
                        self.conn.interrupt()
                except Exception:
                    # বাতিল করার সময় ত্রুটি উপেক্ষা করা হবে
                    pass

    def run(self):
        local_conn = None
        try:
            with self.lock:
                if self._is_cancelled:
                    return

            start_time = time.time()
            if not self.conn_data:
                raise ConnectionError("Incomplete connection information.")

            db_type = "sqlite" if "db_path" in self.conn_data and self.conn_data[
                "db_path"] else "postgres"

            if db_type == "sqlite":
                local_conn = sqlite.connect(self.conn_data["db_path"])
            else:
                local_conn = psycopg2.connect(
                    host=self.conn_data["host"], database=self.conn_data["database"],
                    user=self.conn_data["user"], password=self.conn_data["password"],
                    port=int(self.conn_data["port"])
                )

            # ক্যানসেলেশন মেথডের জন্য কানেকশন অবজেক্টটি শেয়ার করা
            with self.lock:
                if self._is_cancelled:
                    local_conn.close()
                    return
                self.conn = local_conn

            cursor = local_conn.cursor()
            cursor.execute(self.query)

            with self.lock:
                if self._is_cancelled:
                    return

            row_count = 0
            is_select_query = self.query.lower().strip().startswith("select")
            results = []
            columns = []

            if is_select_query:
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    if not self._is_cancelled:
                        results = cursor.fetchall()
                        row_count = len(results)
            else:
                local_conn.commit()
                row_count = cursor.rowcount if cursor.rowcount != -1 else 0

            with self.lock:
                if self._is_cancelled:
                    return

            elapsed_time = time.time() - start_time
            self.signals.finished.emit(
                results, columns, row_count, elapsed_time, is_select_query)

        except (sqlite.OperationalError, psycopg2.errors.QueryCanceled) as e:
            # কোয়েরি বাতিল হলে যে exception আসে, তা এখানে ধরা হবে
            if "interrupted" in str(e).lower() or "canceled" in str(e).lower():
                pass
            else:
                if not self._is_cancelled:
                    self.signals.error.emit(str(e))
        except Exception as e:
            if not self._is_cancelled:
                self.signals.error.emit(str(e))
        finally:
            with self.lock:
                self.conn = None
            if local_conn:
                local_conn.close()


def setup_database():
    """
    অ্যাপ্লিকেশনের জন্য প্রয়োজনীয় ডাটাবেস এবং টেবিল তৈরি করে।
    """
    db_file = 'hierarchy.db'
    db_exists = os.path.exists(db_file)
    conn = sqlite.connect(db_file)
    c = conn.cursor()

    if not db_exists:
        c.execute(
            'CREATE TABLE categories (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE)')
        c.execute('CREATE TABLE subcategories (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, category_id INTEGER, FOREIGN KEY (category_id) REFERENCES categories (id))')
        c.execute('CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, subcategory_id INTEGER, host TEXT, "database" TEXT, "user" TEXT, password TEXT, port INTEGER, db_path TEXT, FOREIGN KEY (subcategory_id) REFERENCES subcategories (id))')
        c.execute("INSERT INTO categories (name) VALUES ('PostgreSQL Connections')")
        c.execute("INSERT INTO categories (name) VALUES ('SQLite Connections')")
        conn.commit()

    c.execute("PRAGMA table_info(items)")
    columns = [info[1] for info in c.fetchall()]
    if 'usage_count' not in columns:
        c.execute(
            "ALTER TABLE items ADD COLUMN usage_count INTEGER NOT NULL DEFAULT 0")
        conn.commit()

    conn.close()
