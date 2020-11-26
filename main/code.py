import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QPushButton, QGridLayout,
                             QVBoxLayout)
from PyQt5.QtCore import Qt
import qdarkgraystyle
from tabs.download import Download
from tabs.etl import Etl
from tabs.merge import Merge


class Main(QWidget):
    def __init__(self):
        super().__init__()

        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)
        self.widget_function = None

        self.setStyleSheet(qdarkgraystyle.load_stylesheet())

        self.grid = QGridLayout()

        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignLeft)
        layout.setSpacing(90)

        button_download = QPushButton('Download')
        button_download.setMinimumWidth(100)
        button_download.setMaximumWidth(200)
        button_download.clicked.connect(self.drawWindow)
        etl = QPushButton('Etl')
        etl.clicked.connect(self.drawWindow)
        etl.setMinimumWidth(100)
        etl.setMaximumWidth(200)
        merge = QPushButton('Merge')
        merge.clicked.connect(self.drawWindow)
        merge.setMinimumWidth(100)
        merge.setMaximumWidth(200)

        layout.addWidget(button_download)
        layout.addWidget(etl)
        layout.addWidget(merge)

        self.widget_function = Download()
        self.grid.addLayout(layout, 0, 0)
        self.grid.addWidget(self.widget_function, 0, 1)

        self.setLayout(self.grid)

    def drawWindow(self):
        self.widget_function.deleteLater()
        if self.sender().text() == 'Download':
            self.widget_function = Download()
        elif self.sender().text() == 'Etl':
            self.widget_function = Etl()
        elif self.sender().text() == 'Merge':
            self.widget_function = Merge()

        self.grid.addWidget(self.widget_function, 0, 1)


if __name__ == '__main__':
    app = QApplication([])
    main = Main()
    main.show()
    sys.exit(app.exec_())
