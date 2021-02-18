import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow, QApplication

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import analysis_funct as an

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class AnalysisUI(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(layout + 'analysis.ui', self)
        self.analysis = None

        self.pushButton_2.clicked.connect(
            lambda: an.start_server(self)
        )

        self.radioButton_6.clicked.connect(
            lambda: an.activate(self.radioButton_6)
        )

        self.radioButton_7.clicked.connect(
            lambda: an.activate(self.radioButton_7)
        )
