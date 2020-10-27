#!/usr/bin/env python3

from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget,
                             QTableWidgetItem)
import qdarkstyle


class Pydbsus_gui(QMainWindow):

    def __init__(self, *abas):
        super().__init__()

        self.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt5'))
        self.tabs = QTabWidget()

        for aba in abas:
            self.tabs.addTab(aba, aba.windowTitle())

        self.setCentralWidget(self.tabs)

        self.show()


def escreve_tabela_download():
    download.visualizar_dados()
    download.tabela.clear()

    coluna_ordenada = {}

    if download.df.columns[0] == '_c0':
        for coluna in download.df.columns[1:]:
            coluna_ordenada[coluna] = []
    else:
        for coluna in download.df.columns:
            coluna_ordenada[coluna] = []

    colunas = list(coluna_ordenada.keys())
    colunas.sort()

    for i, column in enumerate(colunas):
        download.tabela.setItem(0, i, QTableWidgetItem(column))
        i += 1

    etl.tabela_adicionar.clear()
    for e, c in enumerate(colunas):
        etl.tabela_adicionar.setItem(e, 0, QTableWidgetItem(c))

    column_n = 0
    row = 1

    for column in colunas:
        for i in range(1, 11):
            download.tabela.setItem(
                row, column_n, QTableWidgetItem(
                    str(
                        download.df.select(
                            download.df[column]).take(i)[i - 1][0])))
            row += 1
        row = 1
        column_n += 1


def aplicar_itens_etl():
    etl.tabela_exportar.clear()
    colunas_selecionadas = []

    for coluna in etl.tabela_aplicar.selectedItems():
        colunas_selecionadas.append(coluna.text())

    remocao = []
    for coluna in download.df.columns:
        if coluna not in colunas_selecionadas:
            remocao.append(coluna)

    new_df = download.df.drop(*remocao)

    for i, coluna in enumerate(new_df.columns):
        etl.tabela_exportar.setItem(0, i, QTableWidgetItem(coluna))

    column_n = 0
    row = 1
    for coluna in new_df.columns:
        for i in range(1, 11):
            etl.tabela_exportar.setItem(
                row, column_n, QTableWidgetItem(
                    str(new_df.select(new_df[coluna]).take(i)[i - 1][0])))
            row += 1
        row = 1
        column_n += 1


def thread_visualizar_dados():
    download.thread_executar_visualizar = _Thread(escreve_tabela_download)
    download.thread_executar_visualizar.start()


if __name__ == '__main__':
    import sys

    from tabs.download import Download, _Thread
    from tabs.etl import Etl
    from tabs.merge import Merge

    app = QApplication(sys.argv)
    download = Download()
    download.visualizar_banco.clicked.connect(thread_visualizar_dados)

    etl = Etl()
    etl.botao_aplicar_aplicar.clicked.connect(aplicar_itens_etl)

    merge = Merge()

    main = Pydbsus_gui(download, etl, merge)
    sys.exit(app.exec_())
