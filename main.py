# sql_client/main.py

import sys
import os
from PyQt6.QtWidgets import QApplication

from ui.main_window import MainWindow
from database import setup_database


def main():
    """
    অ্যাপ্লিকেশন চালু করার মূল ফাংশন।
    """
    if not os.path.exists("assets"):
        os.makedirs("assets")

    setup_database()

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
