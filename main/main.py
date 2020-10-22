#!/usr/bin/env python3

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QPushButton,
                             QTabWidget)
import qdarkstyle


class Pydbsus_gui(QMainWindow):

    def __init__(self, *abas):
        super().__init__()

        self.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt5'))
        self.tabs = QTabWidget()

        for aba in abas:
            self.tabs.addTab(aba, aba.windowTitle())

        self.setCentralWidget(self.tabs)

        self.show()


if __name__ == '__main__':
    import sys

    from tabs.download import Download
    from tabs.etl import Etl
    from tabs.merge import Merge

    app = QApplication(sys.argv)
    download = Download()
    etl = Etl()
    merge = Merge()

    main = Pydbsus_gui(download, etl, merge)
    sys.exit(app.exec_())
