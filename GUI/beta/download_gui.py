import os
import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QGridLayout, QComboBox,
                             QSpinBox, QPushButton, QGroupBox,
                             QProgressBar, QLabel, QTableWidget)

from generic import BaseWindow


class Download(BaseWindow):
    def __init__(self):
        super().__init__('Download', 'imgs/download.png')

        self.drawWindow()

    def drawWindow(self):
        main_layout = QGridLayout()
        main_layout.setSpacing(10)

        group_system = QGroupBox('Sistemas')
        grid_system = QGridLayout()
        grid_system.setSpacing(50)
        group_system.setLayout(grid_system)
        self.system = QComboBox()
        self.system_ = QComboBox()
        self.local = QComboBox()
        self.local_ = QComboBox()
        grid_system.addWidget(self.system, 0, 0)
        grid_system.addWidget(self.system_, 1, 0)
        grid_system.addWidget(self.local, 0, 1)
        grid_system.addWidget(self.local_, 1, 1)

        group_config = QGroupBox('Configurações')
        grid_config = QGridLayout()
        grid_config.setSpacing(50)
        group_config.setLayout(grid_config)
        label_year = QLabel('Ano inicial:')
        label_year_ = QLabel('Ano final:')
        cores = QLabel('Processadores:')
        mem = QLabel('Memoria:')
        self.year = QSpinBox()
        self.year.setMinimum(2010)
        self.year.setMaximum(2019)
        self.year_ = QSpinBox()
        self.year_.setMinimum(2010)
        self.year_.setMaximum(2019)
        self.cores = QSpinBox()
        self.mem = QSpinBox()
        grid_config.addWidget(label_year, 0, 0)
        grid_config.addWidget(label_year_, 0, 2)
        grid_config.addWidget(self.year, 0, 1)
        grid_config.addWidget(self.year_, 0, 3)
        grid_config.addWidget(cores, 1, 0)
        grid_config.addWidget(mem, 1, 2)
        grid_config.addWidget(self.cores, 1, 1)
        grid_config.addWidget(self.mem, 1, 3)

        group_run = QGroupBox('Executar')
        grid_run = QGridLayout()
        group_run.setLayout(grid_run)
        self.load_table = QPushButton('CARREGAR BANCO')
        self.view = QPushButton('VISUALIZAR BANCO')
        grid_run.addWidget(self.load_table, 0, 0)
        grid_run.addWidget(self.view, 0, 1)

        group_proc = QGroupBox('Progresso da Tarefa')
        grid_proc = QGridLayout()
        group_proc.setLayout(grid_proc)
        self.progress = QProgressBar()
        self.progress.setValue(0)
        grid_system.addWidget(self.progress, 2, 0, 2, 2)

        group_sample = QGroupBox('Amostra dos dados')
        grid_sample = QGridLayout()
        group_sample.setLayout(grid_sample)
        self.table_sample = QTableWidget(10, 150)
        grid_sample.addWidget(self.table_sample, 0, 0)

        main_layout.addWidget(group_system, 0, 0, 2, 1)
        main_layout.addWidget(group_config, 0, 1)
        main_layout.addWidget(group_run, 1, 1)
        main_layout.addWidget(group_sample, 3, 0, 3, 2)

        self.setLayout(main_layout)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setApplicationName('Download')
    main = Download()
    main.show()
    sys.exit(app.exec_())
