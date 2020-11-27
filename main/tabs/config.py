#!/usr/bin/env python3

import sys
from os import path
from PyQt5.QtWidgets import (QApplication, QWidget, QComboBox, QGroupBox,
                             QVBoxLayout)
from PyQt5.QtGui import QIcon

img_folder = path.dirname(__file__)
img_config = path.join(img_folder, 'imgs/config/')


class Config(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowIcon(QIcon(img_config + 'new_config.png'))
        self.settings = QGroupBox('Configurações')
        vbox = QVBoxLayout()
        self.setWindowTitle('Configuração')
        self.select_layout = QComboBox()
        self.select_layout.addItems([
            'Fusion', 'Windows', 'Dark', 'DarkGray'
        ])

        vbox.addWidget(self.select_layout)
        self.setLayout(vbox)


def main():
    app = QApplication([])
    config = Config()
    config.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
