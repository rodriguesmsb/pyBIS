import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QWidget, QApplication, QHBoxLayout, QButtonGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import analysis_funct as an

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class AnalysisUI(QWidget):
    def __init__(self):
        super().__init__()

        layout_box = QHBoxLayout()

        self.frame_map = uic.loadUi(layout + 'frame_map.ui')

        self.frame_map.pushButton.clicked.connect(
            lambda: an.trade_frame(layout_box, self.frame_map,
                                     self.frame_data)
        )

        self.frame_map.pushButton_2.clicked.connect(
            lambda: an.trade_frame(layout_box, self.frame_map,
                                     self.frame_analysis)
        )

        self.frame_map.pushButton_3.clicked.connect(
            lambda: an.get_shapefile(self.frame_map.pushButton_3,
                                     self.frame_map.lineEdit)
        )

        self.frame_data = uic.loadUi(layout + 'frame_data.ui')

        self.frame_data.pushButton.clicked.connect(
            lambda: an.get_csv(self.frame_data.pushButton,
                               self.frame_data.lineEdit)
        )

        self.frame_data.pushButton_2.clicked.connect(
            lambda: an.trade_frame(layout_box, self.frame_data,
                                     self.frame_map)
        )

        self.frame_data.pushButton_3.clicked.connect(
            lambda: an.trade_frame(layout_box, self.frame_data,
                                     self.frame_analysis)
        )

        self.frame_analysis = uic.loadUi(layout + 'frame_analysis.ui')

        self.frame_analysis.pushButton_2.clicked.connect(
            lambda: an.trade_frame(layout_box, self.frame_analysis,
                                     self.frame_map)
        )

        self.frame_analysis.pushButton_3.clicked.connect(
            lambda: an.trade_frame(layout_box, self.frame_analysis,
                                     self.frame_data)
        )

        self.frame_data.setHidden(True)
        self.frame_analysis.setHidden(True)
        # self.frame_data.setHidden()

        layout_box.addWidget(self.frame_map)
        layout_box.addWidget(self.frame_data)
        layout_box.addWidget(self.frame_analysis)

        self.setLayout(layout_box)
