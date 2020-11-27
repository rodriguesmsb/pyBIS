from sys import argv, exit
from os import path
from PyQt5.QtWidgets import (QApplication, QWidget, QPushButton, QComboBox,
                             QGroupBox, QGridLayout, QTableWidget,
                             QTableWidgetItem, QFileDialog)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
import pandas as pd


img_folder = path.dirname(__file__)
img_merge = path.join(img_folder, 'imgs/mesclar/')


class Merge(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('Mesclar')
        self.setWindowIcon(QIcon(img_merge + 'merge.png'))
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.main_layout = QGridLayout()

        self.grupo_esquerda = QGroupBox('Data 1')
        self.grid_esquerda = QGridLayout()

        self.escolher_csv_esquerda = QPushButton('Escolher arquivo')
        self.escolher_csv_esquerda.clicked.connect(self.b1_)
        self.tabela_esquerda = QTableWidget(10, 70)

        self.grid_esquerda.addWidget(self.escolher_csv_esquerda, 0, 0,
                                     Qt.AlignLeft)

        self.grid_esquerda.addWidget(self.tabela_esquerda, 1, 0)

        self.grupo_direita = QGroupBox('Data 2')
        self.grid_direita = QGridLayout()

        self.escolher_csv_direita = QPushButton('Escolher arquivo')
        self.escolher_csv_direita.clicked.connect(self.b2_)
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
        self.selecionar_coluna.setEditable(True)
        self.adicionar_coluna = QPushButton('Adicionar')
        self.adicionar_coluna.clicked.connect(self.add_column_same)
        self.colunas_selecionadas = QTableWidget(10, 1)
        self.colunas_selecionadas.setColumnWidth(0, 500)
        self.botao_aplicar = QPushButton('Aplicar')
        self.botao_aplicar.clicked.connect(self.get_merge_data)
        self.botao_exportar = QPushButton('Exportar')
        self.botao_exportar.clicked.connect(self.exportar_reduzido)

        self.grid_planeta.addWidget(self.selecionar_coluna, 0, 0)
        self.grid_planeta.addWidget(self.adicionar_coluna, 0, 1)
        self.grid_planeta.addWidget(self.colunas_selecionadas, 1, 0)

        self.grupo_planeta.setLayout(self.grid_planeta)

        self.grid_juncao.addWidget(self.botao_aplicar, 0, 0)
        self.grid_juncao.addWidget(self.botao_exportar, 1, 0)
        self.grid_juncao.addWidget(self.tabela_juncao, 0, 1, 3, 1)

        self.grupo_juncao.setLayout(self.grid_juncao)

        self.main_layout.addWidget(self.grupo_esquerda, 0, 0)
        self.main_layout.addWidget(self.grupo_direita, 0, 1)
        self.main_layout.addWidget(self.grupo_juncao, 1, 1)
        self.main_layout.addWidget(self.grupo_planeta, 1, 0)

        self.setLayout(self.main_layout)

        self.show()

    def b1_(self):
        filename, _ = QFileDialog.getOpenFileName(self,
                                                  'Open File',
                                                  'Arquivo csv (*.csv)')
        self.b1 = pd.read_csv(filename, low_memory=False)
        if 'TIPOBITO' in self.b1.columns:
            year, month = self.year_month(self.b1['DTOBITO'])
            self.b1["YEAR"] = year
            self.b1["MONTH"] = month
        elif 'DTNASC' in self.b1.columns and 'TPOBITO' not in self.b1.columns:
            year, month = self.year_month(self.b1['DTNASC'])
            self.b1["YEAR"] = year
            self.b1["MONTH"] = month

        i = 0
        lista = self.b1.columns[1:]
        for column in lista:
            self.tabela_esquerda.setItem(0, i, QTableWidgetItem(column))
            i += 1

        column_n = 0
        row = 1
        for column in lista:
            for i in range(1, 11):
                self.tabela_esquerda.setItem(row, column_n,
                                             QTableWidgetItem(
                                                    str(self.b1[column][i])))
                row += 1
            row = 1
            column_n += 1

    def b2_(self):
        filename, _ = QFileDialog.getOpenFileName(self,
                                                  'Open File',
                                                  'Arquivo csv (*.csv)')
        self.b2 = pd.read_csv(filename, low_memory=False)
        if 'TIPOBITO' in self.b2.columns:
            year, month = self.year_month(self.b2['DTOBITO'])
            self.b2["YEAR"] = year
            self.b2["MONTH"] = month
        elif 'DTNASC' in self.b2.columns and 'TPOBITO' not in self.b2.columns:
            year, month = self.year_month(self.b2['DTNASC'])
            self.b2["YEAR"] = year
            self.b2["MONTH"] = month

        i = 0
        lista = self.b2.columns[1:]
        for column in lista:
            self.tabela_direita.setItem(0, i, QTableWidgetItem(column))
            i += 1

        column_n = 0
        row = 1
        for column in lista:
            for i in range(1, 11):
                self.tabela_direita.setItem(row, column_n,
                                            QTableWidgetItem(
                                                    str(self.b2[column][i])))
                row += 1
            row = 1
            column_n += 1

        self.get_same_columns(self.b1, self.b2)

    def get_merge_data(self):
        self.merge_data(self.b1, self.b2, [x.text() for x in
                        self.colunas_selecionadas.selectedItems()])

    def get_same_columns(self, df1, df2):
        result_index = []
        for column in df1.columns:
            if column in df2.columns:
                result_index.append(column)
        self.selecionar_coluna.addItems(result_index)

        # return result_index

    def merge_data(self, b1, b2, columns):
        b1 = b1[columns].groupby(columns).size().reset_index(name="b1_count")
        b2 = b2[columns].groupby(columns).size().reset_index(name="b2_count")
        self.df_reduzido = b1.merge(b2, on=columns)

        i = 0
        lista = self.df_reduzido.columns
        for column in lista:
            self.tabela_juncao.setItem(0, i, QTableWidgetItem(column))
            i += 1

        column_n = 0
        row = 1
        for column in lista:
            for i in range(1, 11):
                self.tabela_juncao.setItem(
                    row, column_n, QTableWidgetItem(
                        str(self.df_reduzido[column][i])))
                row += 1
            row = 1
            column_n += 1

    def year_month(self, date):
        def correct_date(x):
            x = str(x)
            if len(x) < 8:
                x = "0" + x
            return x
        date = date.apply(lambda x: correct_date(x))
        date = pd.to_datetime(date.astype(str), format="%d%m%Y")
        year, month = date.dt.year, date.dt.month
        return (year, month)

    def exportar_reduzido(self):
        try:
            options = QFileDialog.Options()
            options |= QFileDialog.DontUseNativeDialog
            filename, _ = QFileDialog.getSaveFileName(self,
                                                      'Save File', '',
                                                      'Arquivo csv(*.csv)')
            if filename:
                self.df_reduzido.to_csv(filename, index=False)
        except NameError:
            ...

    def add_column_same(self):
        text = self.selecionar_coluna.currentText()
        self.colunas_selecionadas.setItem(
            self.colunas_selecionadas.currentRow(), 0,
            QTableWidgetItem(text))


if __name__ == '__main__':
    app = QApplication(argv)
    merge = Merge()
    exit(app.exec_())
