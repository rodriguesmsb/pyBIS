import sys
from os import path
from PyQt5.QtWidgets import (QApplication, QWidget, QHBoxLayout, QVBoxLayout,
                             QTableWidget, QPushButton, QGridLayout, QComboBox,
                             QFormLayout, QGroupBox, QLineEdit,
                             QTableWidgetItem)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon

img_folder = path.dirname(__file__)
img_etl = path.join(img_folder, 'imgs/etl/')


class Etl(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowIcon(QIcon(img_etl + 'etl.png'))
        grid = QGridLayout()
        grid.addWidget(self.extracao(), 0, 0)
        grid.addWidget(self.transformacao(), 0, 1)
        grid.addWidget(self.exportacao(), 1, 0, 1, 2, Qt.AlignTop)

        self.setLayout(grid)
        self.setWindowTitle('Etl')

    def adicionar_linha(self):
        selecionado = self.tabela_adicionar.currentRow()
        local_aplicar = self.tabela_aplicar.currentRow()
        coluna = self.tabela_adicionar.item(selecionado, 0).text()
        self.tabela_aplicar.setItem(local_aplicar, 0, QTableWidgetItem(coluna))

    def remover_linha(self):
        selecionado = self.tabela_aplicar.currentRow()
        self.tabela_aplicar.removeRow(selecionado)

    def extracao(self):
        hbox_tabela = QHBoxLayout()
        hbox_botoes = QHBoxLayout()
        layout = QFormLayout()
        layout_group = QGroupBox('Extração')

        self.tabela_aplicar = QTableWidget(120, 1)
        self.tabela_adicionar = QTableWidget(120, 1)
        self.tabela_adicionar.setHorizontalHeaderItem(
            0, QTableWidgetItem('Selecione suas colunas'))
        self.tabela_adicionar.horizontalHeader().setDefaultAlignment(
            Qt.AlignLeft)
        self.tabela_adicionar.setColumnWidth(0, 520)
        self.tabela_aplicar.setColumnWidth(0, 520)
        self.tabela_aplicar.setHorizontalHeaderItem(
            0, QTableWidgetItem('Colunas Selecionadas'))
        self.tabela_aplicar.horizontalHeader().setDefaultAlignment(
            Qt.AlignLeft)

        self.botao_adicionar = QPushButton('Adicionar')
        self.botao_adicionar.clicked.connect(self.adicionar_linha)
        self.botao_remover = QPushButton('Remover')
        self.botao_remover.clicked.connect(self.remover_linha)
        self.botao_aplicar_extracao = QPushButton('Aplicar')

        botoes = [
            self.botao_adicionar, self.botao_remover,
            self.botao_aplicar_extracao
        ]

        hbox_tabela.addWidget(self.tabela_adicionar)
        hbox_tabela.addWidget(self.tabela_aplicar)

        for widget in botoes:
            hbox_botoes.addWidget(widget)

        layout.addRow(hbox_tabela)
        layout.addRow(hbox_botoes)

        layout_group.setLayout(layout)

        return layout_group

    def transformacao(self):
        vbox = QVBoxLayout()
        vbox_tabela = QVBoxLayout()
        hbox = QHBoxLayout()
        hbox_botoes = QHBoxLayout()
        layout = QFormLayout()
        layout_group = QGroupBox('Transformação')

        self.botao_linha = QComboBox()
        # self.botao_linha.setEditable(True)
        self.botao_maior = QPushButton('>')
        self.botao_menor = QPushButton('<')
        self.botao_maior_igual = QPushButton('>=')
        self.botao_menor_igual = QPushButton('<=')
        self.botao_igual = QPushButton('equal')
        self.botao_diferente = QPushButton('!=')
        self.botao_and = QPushButton('and')
        self.botao_or = QPushButton('or')
        self.botao_in = QPushButton('in')
        self.botao_not = QPushButton('not')

        self.tabela_transformar = QTableWidget(150, 1)
        self.tabela_transformar.horizontalHeader().setDefaultAlignment(
            Qt.AlignLeft)
        self.tabela_transformar.setHorizontalHeaderItem(
            0, QTableWidgetItem('Histórico de Filtros'))
        # self.tabela_transformar.setHeader
        self.tabela_transformar.setColumnWidth(0, 500)
        self.query = QLineEdit()
        self.botao_aplicar_transformacao = QPushButton('Aplicar')

        vbox.addWidget(self.botao_linha)
        vbox.addWidget(self.botao_maior)
        vbox.addWidget(self.botao_menor)
        vbox.addWidget(self.botao_maior_igual)
        vbox.addWidget(self.botao_menor_igual)
        vbox.addWidget(self.botao_igual)
        vbox.addWidget(self.botao_diferente)
        vbox.addWidget(self.botao_and)
        vbox.addWidget(self.botao_or)
        vbox.addWidget(self.botao_in)
        vbox.addWidget(self.botao_not)

        hbox.addLayout(vbox)

        hbox_botoes.addWidget(self.query)
        hbox_botoes.addWidget(self.botao_aplicar_transformacao)
        vbox_tabela.addWidget(self.tabela_transformar)
        vbox_tabela.addLayout(hbox_botoes)

        hbox.addLayout(vbox_tabela)

        layout.addRow(hbox)
        layout_group.setLayout(layout)

        return layout_group

    def exportacao(self):
        grid = QGridLayout()
        self.tabela_exportar = QTableWidget(150, 150)
        self.botao_exportar = QPushButton('Exportar ".csv"')
        layout_group = QGroupBox('Exportação')

        grid.addWidget(self.tabela_exportar, 0, 0)
        grid.addWidget(self.botao_exportar, 1, 0, Qt.AlignRight)

        layout_group.setLayout(grid)

        return layout_group


if __name__ == '__main__':
    app = QApplication([])
    window = Etl()
    window.show()
    sys.exit(app.exec_())
