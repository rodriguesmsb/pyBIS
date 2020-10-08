#!/usr/bin/env python3

from sys import argv, exit
from threading import Thread
from time import sleep
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton,
                             QProgressBar)

from PyQt5.QtCore import QThread, pyqtSlot, pyqtSignal

from pydatasus import PyDatasus


class MyThread(QThread):

    change_value = pyqtSignal(int)

    def __init__(self, func):
        super().__init__()
        self.func = func

    def run(self):
        cnt = 0
        while self.func.is_alive():
            cnt += 1
            sleep(0.3)
            if cnt == 100:
                cnt = 0
            self.change_value.emit(cnt)
        self.terminate()


class Window(QMainWindow):

    def __init__(self):
        super().__init__()

        self.setFixedSize(400, 200)

        self.botao = QPushButton('botao', self)
        self.botao.clicked.connect(self._thread_get_csv)

        self.botao.move(140, 40)

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setGeometry(50, 100, 300, 30)

        self.show()

    def start_bar(self, func):
        self.thread = MyThread(func)
        self.thread.change_value.connect(self.updateProgressBar)
        self.thread.start()

    def updateProgressBar(self, val):
        self.progress_bar.setValue(val)

    def _thread_get_csv(self):
        thread = Thread(target=PyDatasus().get_csv, args=('SIM',), daemon=True)
        thread.start()

        self.start_bar(thread)


if __name__ == '__main__':
    app = QApplication(argv)
    wind = Window()
    exit(app.exec_())
