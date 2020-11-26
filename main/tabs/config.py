#!/usr/bin/env python3

import sys
from PyQt5.QtWidgets import QApplication, QWidget, QComboBox


class Config(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle('Configuração')
        self.select_layout = QComboBox(self)
        self.select_layout.setGeometry(50, 50, 150, 50)
        self.select_layout.addItems([
            'Fusion', 'Windows', 'Dark', 'DarkGray'
        ])


def main():
    app = QApplication([])
    config = Config()
    config.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
