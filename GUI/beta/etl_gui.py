import os
import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QGridLayout, QComboBox,
                             QPushButton, QGroupBox, QHBoxLayout, QTableWidget,
                             QVBoxLayout, QLineEdit, QTableWidgetItem)
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import Qt

from generic import BaseWindow


class Etl(BaseWindow):
    def __init__(self):
        super().__init__('E.T.L', 'imgs/etl.png')

        self.drawWindow()

    def drawWindow(self):
        main_layout = QGridLayout()

        grid_ext = QGridLayout()
        grid_ext.setSpacing(20)
        group_ext = QGroupBox('Extrair')
        group_ext.setLayout(grid_ext)
        box_table = QHBoxLayout()
        self.table_add = QTableWidget(150, 1)
        self.table_add.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)
        self.table_add.setHorizontalHeaderItem(
            0, QTableWidgetItem('Selecionar Coluna'))
        self.table_add.setColumnWidth(0, 210)
        self.table_app = QTableWidget(150, 1)
        self.table_app.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)
        self.table_app.setHorizontalHeaderItem(
            0, QTableWidgetItem('Colunas Selecionadas'))
        self.table_app.setColumnWidth(0, 210)
        box_table.addWidget(self.table_add)
        box_table.addWidget(self.table_app)

        self.button_add = QPushButton('Adicionar')
        self.button_apply = QPushButton('Aplicar')
        self.button_remove = QPushButton('Remover')
        grid_ext.addLayout(box_table, 0, 0, 1, 3)
        grid_ext.addWidget(self.button_add, 1, 0)
        grid_ext.addWidget(self.button_apply, 1, 1)
        grid_ext.addWidget(self.button_remove, 1, 2)

        grid_trans = QGridLayout()
        group_trans = QGroupBox('Transformar')
        group_trans.setLayout(grid_trans)
        vbox_buttons1 = QVBoxLayout()
        vbox_buttons2 = QVBoxLayout()
        self.column = QComboBox()
        self.column.addItem('Selecionar Coluna')
        self.column.setEditable(True)
        self.column.setMinimumWidth(200)
        self.button_gt = QPushButton('>')
        self.button_lt = QPushButton('<')
        self.button_gte = QPushButton('>=')
        self.button_lte = QPushButton('<=')
        self.button_dif = QPushButton('!=')
        self.button_equal = QPushButton('==')
        self.button_and = QPushButton('and')
        self.button_or = QPushButton('or')
        self.button_in = QPushButton('in')
        self.button_not = QPushButton('not')
        self.expression = QLineEdit()
        self.button_apply_expression = QPushButton('Aplicar Filtro')
        self.table_trans = QTableWidget(150, 1)
        self.table_trans.setHorizontalHeaderItem(
            0, QTableWidgetItem('Histórico de Filtros'))
        self.table_trans.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)
        self.table_trans.setColumnWidth(0, 510)

        vbox_buttons1.addWidget(self.button_lt)
        vbox_buttons1.addWidget(self.button_lte)
        vbox_buttons1.addWidget(self.button_dif)
        vbox_buttons1.addWidget(self.button_not)
        vbox_buttons1.addWidget(self.button_and)

        vbox_buttons2.addWidget(self.button_gt)
        vbox_buttons2.addWidget(self.button_gte)
        vbox_buttons2.addWidget(self.button_equal)
        vbox_buttons2.addWidget(self.button_in)
        vbox_buttons2.addWidget(self.button_or)

        hbox_button = QHBoxLayout()
        hbox_button.addWidget(self.column)
        hbox_button.addWidget(self.expression)
        hbox_button.addWidget(self.button_apply_expression)

        grid_trans.addLayout(vbox_buttons1, 0, 0)
        grid_trans.addLayout(vbox_buttons2, 0, 1)
        grid_trans.addWidget(self.table_trans, 0, 2, 1, 1)
        grid_trans.addLayout(hbox_button, 1, 0, 1, 3)

        grid_exp = QGridLayout()
        group_exp = QGroupBox('Exportar')
        group_exp.setLayout(grid_exp)
        self.button_export = QPushButton('Exportar')
        self.button_export.setMinimumWidth(200)
        self.table_exp = QTableWidget(10, 150)
        grid_exp.addWidget(self.table_exp, 0, 0)
        grid_exp.addWidget(self.button_export, 1, 0, Qt.AlignRight)

        main_layout.addWidget(group_ext, 0, 0)
        main_layout.addWidget(group_trans, 0, 1)
        main_layout.addWidget(group_exp, 1, 0, 1, 2)

        self.setLayout(main_layout)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setApplicationName('ETL')
    main = Etl()
    main.show()
    sys.exit(app.exec_())
