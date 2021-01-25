import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtGui import QStandardItemModel

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import merge_funct as mf

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class Merge(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(layout + 'merge.ui', self)

        self.select_file1.clicked.connect(
            lambda: mf.load_data(self.select_file1, self.table1,
                                 self.add_line)
        )

        self.select_file2.clicked.connect(
            lambda: mf.load_data(self.select_file2, self.table2,
                                 self.add_line)
        )

        model = QStandardItemModel()
        self.table3.setModel(model)

        self.button_add.clicked.connect(
            lambda: mf.add_column_same(self.add_line, self.table3)
        )
