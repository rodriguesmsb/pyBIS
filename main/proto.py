import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QPushButton, QVBoxLayout,
                             QGridLayout, QGroupBox, QComboBox, QHBoxLayout,
                             QProgressBar, QSpinBox, QLabel, QTableWidget)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon, QFont


class Main(QWidget):
    def __init__(self):
        super().__init__()

        self.setFont(QFont('Noto Sans', 12))
        self.grid = QGridLayout()
        self.group = QGroupBox('Funções')
        self.vbox = QVBoxLayout()
        self.vbox.setSpacing(30)

        # self.button_download = QPushButton('Download')
        self.button_download = QPushButton(
            QIcon('/home/fabio/Imagens/download.png'), '')
        self.button_download.clicked.connect(self.drawDownload)
        # self.button_etl = QPushButton('E.T.L')
        self.button_etl = QPushButton(
            QIcon('/home/fabio/Imagens/preprocessamento.png'), '')
        self.button_merge = QPushButton('Merge')

        self.vbox.addWidget(self.button_download)
        self.vbox.addWidget(self.button_etl)
        self.vbox.addWidget(self.button_merge)

        self.group.setLayout(self.vbox)

        self.grid.addWidget(self.group, 0, 0, Qt.AlignLeft | Qt.AlignTop)
        self.setLayout(self.grid)

    def drawDownload(self):
        grid_download = QGridLayout()
        grid_download.setSpacing(50)

        group_system = QGroupBox('Sistemas')

        system_locale = QHBoxLayout()
        system_locale.setSpacing(60)
        base_locale = QHBoxLayout()
        base_locale.setSpacing(60)
        system_locale_progress = QVBoxLayout()

        self.system = QComboBox()
        self.locale = QComboBox()
        self.base = QComboBox()
        self.state_locale = QComboBox()
        self.progress = QProgressBar()
        self.progress.setValue(0)

        system_locale.addWidget(self.system)
        system_locale.addWidget(self.locale)
        base_locale.addWidget(self.base)
        base_locale.addWidget(self.state_locale)
        system_locale_progress.addLayout(system_locale)
        system_locale_progress.addLayout(base_locale)
        system_locale_progress.addWidget(self.progress)
        group_system.setLayout(system_locale_progress)

        grid_download.addWidget(group_system)

        group_op = QGroupBox('Opções')
        vbox_op = QVBoxLayout()
        vbox_op.setSpacing(40)
        hbox_init = QHBoxLayout()
        hbox_init.setSpacing(20)
        hbox_finale = QHBoxLayout()
        hbox_finale.setSpacing(20)

        self.date_init = QSpinBox()
        self.date_init.setMinimum(2010)
        self.date_init.setMaximum(2019)

        self.date_finale = QSpinBox()
        self.date_finale.setMinimum(2010)
        self.date_finale.setMaximum(2019)

        self.set_cpu = QSpinBox()
        self.set_memory = QSpinBox()

        group_buttons = QGroupBox()
        group_hbox_buttons = QHBoxLayout()
        self.load_base = QPushButton('CARREGAR BANCO')
        self.view_base = QPushButton('VISUALIZAR BANCO')
        group_hbox_buttons.addWidget(self.load_base)
        group_hbox_buttons.addWidget(self.view_base)
        group_buttons.setLayout(group_hbox_buttons)

        hbox_init.addWidget(QLabel('ANO INICIAL:'))
        hbox_init.addWidget(self.date_init)
        hbox_init.addWidget(QLabel('SETAR CORES:'))
        hbox_init.addWidget(self.set_cpu)

        hbox_finale.addWidget(QLabel('ANO FINAL:'))
        hbox_finale.addWidget(self.date_finale)
        hbox_finale.addWidget(QLabel('SETAR MEMORIA:'))
        hbox_finale.addWidget(self.set_memory)

        vbox_op.addLayout(hbox_init)
        vbox_op.addLayout(hbox_finale)
        vbox_op.addWidget(group_buttons)
        group_op.setLayout(vbox_op)

        group_tabela = QGroupBox('Tabela')
        self.tabela_download = QTableWidget(10, 150)
        tabela_vbox = QVBoxLayout()
        tabela_vbox.addWidget(self.tabela_download)
        group_tabela.setLayout(tabela_vbox)

        grid_download.addWidget(group_op, 0, 1)
        grid_download.addWidget(group_tabela, 1, 0, 1, 2)

        self.grid.addLayout(grid_download, 0, 1)


if __name__ == '__main__':
    app = QApplication([])
    main = Main()
    main.show()
    sys.exit(app.exec_())
