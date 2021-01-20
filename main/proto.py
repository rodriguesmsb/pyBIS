import sys
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton,
                             QGroupBox, QGridLayout, QVBoxLayout)
from PyQt5.QtCore import Qt, pyqtSlot, pyqtSignal

from tabs.download import Download
from tabs.etl import Etl
from tabs.merge import Merge


class Main(QMainWindow):
    def __init__(self):
        super().__init__()

        group_functions = QGroupBox('Funções')
        vbox_functions = QVBoxLayout()
        self.main_grid = QGridLayout()

        button_draw_download = QPushButton('Download')
        button_draw_download.clicked.connect(self.button_signal)
        button_draw_etl = QPushButton('E.T.L')
        button_draw_etl.clicked.connect(self.button_signal)
        button_draw_merge = QPushButton('Merge')
        button_draw_merge.clicked.connect(self.button_signal)

        vbox_functions.addWidget(button_draw_download)
        vbox_functions.addWidget(button_draw_etl)
        vbox_functions.addWidget(button_draw_merge)

        self.main_grid.addLayout(vbox_functions, 0, 0,
                                 Qt.AlignLeft | Qt.AlignTop)
        group_functions.setLayout(self.main_grid)

        self.setCentralWidget(group_functions)

    def drawWindow(self, text):
        print(text)

    @pyqtSlot()
    def button_signal(self):
        self.signal = pyqtSignal()

        self.signal.connect(self.drawWindow)
        self.signal.emit()


if __name__ == '__main__':
    app = QApplication([])
    main = Main()
    main.show()
    sys.exit(app.exec_())
