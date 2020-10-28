from sys import argv, exit
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QPushButton,
                             QProgressBar, QComboBox, QGroupBox, QGridLayout,
                             QLabel, QSpinBox, QTableWidget, QTableWidgetItem,
                             QLineEdit)
from PyQt5.QtCore import Qt


class Etl(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('E.T.L')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.main_layout = QGridLayout()

        self.tabela_adicionar = QTableWidget(150, 1)
        self.tabela_adicionar.setColumnWidth(0, 520)

        self.tabela_aplicar = QTableWidget(150, 1)
        self.tabela_aplicar.setColumnWidth(0, 520)
        self.botao_adicionar = QPushButton('Adicionar')
        self.botao_adicionar.clicked.connect(self.adicionar_linha)
        self.botao_remover = QPushButton('Remover')
        self.botao_remover.clicked.connect(self.remover_item)
        self.botao_aplicar_aplicar = QPushButton('Aplicar')

        self.grupo_tabelas = QGroupBox()
        self.grid_tabelas = QGridLayout()

        self.grupo_botoes = QGroupBox()
        self.grid_botoes = QGridLayout()

        self.grupo_tabelas.setLayout(self.grid_tabelas)

        self.grupo_extracao = QGroupBox('Extração')
        self.grid_extracao = QGridLayout()

        self.grid_extracao.addWidget(self.tabela_adicionar, 0, 0)
        self.grid_extracao.addWidget(self.tabela_aplicar, 0, 1)

        self.grid_extracao.addWidget(self.botao_adicionar, 1, 1, Qt.AlignRight)
        self.grid_extracao.addWidget(self.botao_remover, 2, 1, Qt.AlignRight)
        self.grid_extracao.addWidget(self.botao_aplicar_aplicar, 3, 1,
                                     Qt.AlignRight)

        self.grupo_transformar = QGroupBox('Transformação')
        self.grid_transformar = QGridLayout()

        self.tabela_transformar = QTableWidget(50, 150)
        self.condicao_coluna = QComboBox()
        self.condicao_maior = QPushButton('>')
        self.condicao_menor = QPushButton('<')
        self.condicao_maior_igual = QPushButton('>=')
        self.condicao_menor_igual = QPushButton('<=')
        self.condicao_diferente = QPushButton('!=')
        self.condicao_and = QPushButton('and')
        self.condicao_or = QPushButton('or')
        self.condicao_in = QPushButton('in')
        self.condicao_not = QPushButton('not')
        self.linha_editar = QLineEdit()
        self.botao_aplicar = QPushButton('Aplicar')

        self.grupo_botoes_transformar = QGroupBox()
        self.grid_botoes_transformar = QGridLayout()

        self.grid_transformar.addWidget(self.tabela_transformar, 0, 1, 0, 2)

        self.grid_botoes_transformar.addWidget(self.condicao_coluna, 0, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_maior, 1, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_menor, 2, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_maior_igual, 3, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_menor_igual, 4, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_diferente, 5, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_and, 6, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_or, 7, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_in, 8, 0)

        self.grid_botoes_transformar.addWidget(self.condicao_not, 9, 0)

        self.grupo_botoes_transformar.setLayout(self.grid_botoes_transformar)

        self.grid_transformar.addWidget(self.grupo_botoes_transformar, 0, 0,
                                        Qt.AlignTop)

        self.grid_transformar.addWidget(self.linha_editar, 1, 1,
                                        Qt.AlignBottom)
        self.grid_transformar.addWidget(self.botao_aplicar, 1, 2)

        self.grupo_exportar = QGroupBox('Exportação')
        self.grid_exportar = QGridLayout()

        self.tabela_exportar = QTableWidget(10, 90)
        self.botao_exportar = QPushButton('Exportar .csv')

        self.grid_exportar.addWidget(self.tabela_exportar, 0, 0, 1, 0,
                                     Qt.AlignTop)
        self.grid_exportar.addWidget(self.botao_exportar, 1, 1)

        self.label_grafico = QLabel('')
        self.botao_salvar_html = QPushButton('Gerar Profile')

        self.grupo_profile = QGroupBox('Profile')
        self.grid_profile = QGridLayout()

        self.grid_profile.addWidget(self.botao_salvar_html, 0, 1,
                                    Qt.AlignJustify)
        self.grid_profile.addWidget(self.label_grafico, 0, 0)
        # self.grid_profile.addWidget(self.barra_salvar_html, 0, 0)

        self.grupo_extracao.setLayout(self.grid_extracao)
        self.grupo_transformar.setLayout(self.grid_transformar)
        self.grupo_exportar.setLayout(self.grid_exportar)
        self.grupo_profile.setLayout(self.grid_profile)

        self.main_layout.addWidget(self.grupo_extracao, 0, 0)
        self.main_layout.addWidget(self.grupo_transformar, 0, 1)
        self.main_layout.addWidget(self.grupo_exportar, 1, 0)
        self.main_layout.addWidget(self.grupo_profile, 1, 1)

        self.setLayout(self.main_layout)

        self.show()


    def adicionar_linha(self):
        selecionado = self.tabela_adicionar.currentRow()
        local_aplicar = self.tabela_aplicar.currentRow()
        coluna = self.tabela_adicionar.item(selecionado, 0).text()
        self.tabela_aplicar.setItem(local_aplicar, 0, QTableWidgetItem(coluna))

    def remover_item(self):
        selecionado = self.tabela_aplicar.currentRow()
        self.tabela_aplicar.removeRow(selecionado)


if __name__ == '__main__':
    app = QApplication(argv)
    etl = Etl()
    exit(app.exec_())
