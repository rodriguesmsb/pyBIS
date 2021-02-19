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
        self.nav = None

        self.pushButton_2.clicked.connect(
            lambda: an.start_server(self)
        )

        self.radioButton.toggled.connect(
            lambda: an.activate('spatial', self)
        )

        self.radioButton_2.toggled.connect(
            lambda: an.activate('spatio_temporal', self)
        )

        self.comboBox_7.currentTextChanged.connect(an.write_conf_cat)
        self.comboBox_8.currentTextChanged.connect(an.write_conf_cat_1)
        self.comboBox_9.currentTextChanged.connect(an.write_conf_num)
        self.comboBox_10.currentTextChanged.connect(an.write_conf_num_1)
