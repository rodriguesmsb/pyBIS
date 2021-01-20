import os
import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QGridLayout, QComboBox,
                             QSpinBox, QPushButton, QGroupBox,
                             QProgressBar, QLabel, QTableWidget)
from PyQt5.QtCore import Qt

from generic import BaseWindow


class Merge(BaseWindow):
    def __init__(self):
        super().__init__('Mesclar', 'imgs/merge.png')

        self.drawWindow()

    def drawWindow(self):
        main_layout = QGridLayout()

        group_data1 = QGroupBox('Data 1')
        grid_data1 = QGridLayout()
        group_data1.setLayout(grid_data1)

        group_data2 = QGroupBox('Data 2')
        grid_data2 = QGridLayout()
        group_data2.setLayout(grid_data2)

        group_union = QGroupBox('Agregar')
        grid_union = QGridLayout()
        group_union.setLayout(grid_union)

        group_export = QGroupBox('Exportar')
        grid_export = QGridLayout()
        group_export.setLayout(grid_export)

        self.select_file1 = QPushButton('Escolher Arquivo')
        self.tableData1 = QTableWidget(10, 150)
        grid_data1.addWidget(self.select_file1, 0, 0, Qt.AlignLeft)
        grid_data1.addWidget(self.tableData1, 1, 0)
        
        self.select_file2 = QPushButton('Escolher Arquivo')
        self.tableData2 = QTableWidget(10, 150)
        grid_data2.addWidget(self.select_file2, 0, 0, Qt.AlignLeft)
        grid_data2.addWidget(self.tableData2, 1, 0)

        self.column_select = QComboBox()
        self.button_add = QPushButton('Adicionar')
        self.table_agregate = QTableWidget(150, 1)
        self.table_agregate.setColumnWidth(0, 500)
        grid_union.addWidget(self.column_select, 0, 0)
        grid_union.addWidget(self.button_add, 0, 1)
        grid_union.addWidget(self.table_agregate, 1, 0, 1, 1)

        self.button_apply = QPushButton('Aplicar')
        self.button_export = QPushButton('Exportar')
        self.table_export = QTableWidget(10, 150)
        grid_export.addWidget(self.button_apply, 0, 0)
        grid_export.addWidget(self.button_export, 1, 0)
        grid_export.addWidget(self.table_export, 0, 1, 3, 1)

        main_layout.addWidget(group_data1, 0, 0)
        main_layout.addWidget(group_data2, 0, 1)
        main_layout.addWidget(group_union, 1, 0)
        main_layout.addWidget(group_export, 1, 1)

        self.setLayout(main_layout)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setApplicationName('Mesclar')
    main = Merge()
    main.show()
    sys.exit(app.exec_())
