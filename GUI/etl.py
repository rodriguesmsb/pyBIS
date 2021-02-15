import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtGui import QStandardItemModel


sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

import etl_funct as et

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class Etl(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(layout + 'etl.ui', self)

        self.button_add.clicked.connect(
            lambda: et.add_column(self.column_add, self.column_apply)
        )

        self.button_rm.clicked.connect(
            lambda: et.rm_column(self.column_apply)
        )

        buttons = [
            self.button_lt, self.button_gt, self.button_lte, self.button_gte,
            self.button_dif, self.button_equal, self.button_and,
            self.button_or, self.button_not, self.button_in
        ]

        for button in buttons:
            button.clicked.connect(
                lambda: et.operator_line_edit(self, self.line_edit)
            )

        model = QStandardItemModel()
        self.column_ext.setModel(model)

        self.apply_filter.clicked.connect(
            lambda: et.apply_filter(self.line_select, self.line_edit, self)
        )

        self.gen_filter.clicked.connect(
            lambda: et.add_list_filter(
                self.line_select, self.line_edit, model)
        )

        self.rm_filter.clicked.connect(
            lambda: et.rm_column(self.column_ext)
        )

        self.button_export.clicked.connect(
            lambda: et.export_file_csv(self.button_export, self)
        )
