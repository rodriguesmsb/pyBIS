from sys import argv, exit
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QPushButton,
                             QProgressBar, QComboBox, QGroupBox, QGridLayout,
                             QLabel, QSpinBox, QTableWidget, QTableWidgetItem)
from PyQt5.QtCore import Qt


class Merge(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('Mesclar')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.main_layout = QGridLayout()

        self.grupo_esquerda = QGroupBox('Data 1')
        self.grid_esquerda = QGridLayout()

        self.escolher_csv_esquerda = QPushButton('Escolher arquivo')
        self.tabela_esquerda = QTableWidget(10, 70)

        self.grid_esquerda.addWidget(self.escolher_csv_esquerda, 0, 0,
                                     Qt.AlignLeft)

        self.grid_esquerda.addWidget(self.tabela_esquerda, 1, 0)

        self.grupo_direita = QGroupBox('Data 2')
        self.grid_direita = QGridLayout()

        self.escolher_csv_direita = QPushButton('Escolher arquivo')
        self.tabela_direita = QTableWidget(10, 70)

        self.grid_direita.addWidget(self.escolher_csv_direita, 0, 0,
                                    Qt.AlignLeft)

        self.grid_direita.addWidget(self.tabela_direita, 1, 0)

        self.grupo_esquerda.setLayout(self.grid_esquerda)
        self.grupo_direita.setLayout(self.grid_direita)

        self.grupo_juncao = QGroupBox('Exportar')
        self.grid_juncao = QGridLayout()

        self.grupo_planeta = QGroupBox('Agregar')
        self.grid_planeta = QGridLayout()

        self.tabela_juncao = QTableWidget(10, 70)

        self.selecionar_coluna = QComboBox()
        self.adicionar_coluna = QPushButton('Adicionar')
        self.colunas_selecionadas = QTableWidget(10, 1)
        self.colunas_selecionadas.setColumnWidth(0, 500)
        self.botao_aplicar = QPushButton('Aplicar')
        self.botao_exportar = QPushButton('Exportar')

        self.grid_planeta.addWidget(self.selecionar_coluna, 0, 0)
        self.grid_planeta.addWidget(self.adicionar_coluna, 0, 1)
        self.grid_planeta.addWidget(self.colunas_selecionadas, 1, 0)

        self.grupo_planeta.setLayout(self.grid_planeta)

        self.grid_juncao.addWidget(self.botao_aplicar, 0, 0, Qt.AlignTop)
        self.grid_juncao.addWidget(self.botao_exportar, 1, 0, Qt.AlignTop)
        self.grid_juncao.addWidget(self.tabela_juncao, 0, 1, 2, 1)

        self.grupo_juncao.setLayout(self.grid_juncao)

        self.main_layout.addWidget(self.grupo_esquerda, 0, 0)
        self.main_layout.addWidget(self.grupo_direita, 0, 1)
        self.main_layout.addWidget(self.grupo_juncao, 1, 1)
        self.main_layout.addWidget(self.grupo_planeta, 1, 0)

        self.setLayout(self.main_layout)

        self.show()


if __name__ == '__main__':
    app = QApplication(argv)
    merge = Merge()
    exit(app.exec_())
