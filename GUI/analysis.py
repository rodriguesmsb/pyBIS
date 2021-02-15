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
        self.spatial_analysis = None

        self.pushButton.clicked.connect(
            lambda: an.get_shapefile(self.pushButton)
        )
        self.pushButton_3.clicked.connect(
            lambda: an.get_shapefile(self.pushButton_3)
        )

        self.pushButton_2.clicked.connect(
            lambda: an.start_server(self)
        )
