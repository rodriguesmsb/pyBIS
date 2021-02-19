import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import download_funct as dd

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class Download(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(layout + 'download.ui', self)
        self.database.addItem('SELECIONAR SISTEMA')
        self.database.addItems(dd.load_system())
        self.database.currentTextChanged.connect(
            lambda: dd.load_base(self.base, self.database.currentText())
        )

        self.locale.addItem('SELECIONAR LOCAL')
        self.locale.addItems(['BRASIL', 'REGIÃO', 'ESTADO'])
        self.locale.currentTextChanged.connect(
            lambda: dd.load_locales_choice(self.locale_,
                                           self.locale.currentText())
        )

        self.locale_.currentTextChanged.connect(dd.set_spatial_conf)

        self.load_data.clicked.connect(
            lambda: dd.gen_csv(self.database, self.base, self.locale_,
                               self.year, self.year_, self)
        )
