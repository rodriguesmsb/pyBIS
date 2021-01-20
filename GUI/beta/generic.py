import sys
from PyQt5.QtWidgets import QApplication, QWidget
from PyQt5.QtGui import QIcon


class BaseWindow(QWidget):
    def __init__(self, title, icon_file):
        super().__init__()

        self.title = title
        self.icon_file = icon_file

        self.setupUI()

    def setupUI(self):
        screen = QApplication.primaryScreen()
        screen = screen.size()

        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)
        self.setWindowTitle(self.title)
        self.setWindowIcon(QIcon(self.icon_file))
