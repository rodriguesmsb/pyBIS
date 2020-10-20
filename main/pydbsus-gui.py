#!/usr/bin/env python3


from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget,
                             QProgressBar, QComboBox, QSlider, QSpinBox,
                             QGridLayout, QComboBox, QSizePolicy, QLineEdit,
                             QProgressBar, QGroupBox, QLabel, QTableWidget,
                             QPushButton, QTableWidgetItem, QFileDialog,
                             QHeaderView)
from PyQt5.QtGui import QFont
from PyQt5.QtCore import (QObject, QThread, QSize, QRect, Qt, pyqtSignal,
                          pyqtSlot, QFile)
import qdarkstyle
from pandas_profiling import ProfileReport
import pandas as pd


class _Loop(QThread):
    sinal = pyqtSignal(int)

    def __init__(self, thread):
        super().__init__()
        self.thread = thread

    def run(self):
        n = 0
        [botao.setEnabled(False) for botao in download.lista_botoes]
        while self.thread.isRunning():
            n += 1
            if n == 100:
                n = 0
            sleep(0.3)
            self.sinal.emit(n)
        self.sinal.emit(100)
        [botao.setEnabled(True) for botao in download.lista_botoes]


class _Thread(QThread):

    def __init__(self, fn, *arg, **kw):
        super().__init__()
        self.fn = fn
        self.arg = arg
        self.kw = kw

    @pyqtSlot()
    def run(self):
        self.fn(*self.arg, **self.kw)


class Download(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle('Iniciar')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)
        self.spark = ''
        self.estados = {
            'Acre': ['Rio Branco', 'AC', 'Norte'],
            'Alagoas': ['Maceió', 'AL', 'Nordeste'],
            'Amapá': ['Macapá', 'AP', 'Norte'],
            'Amazonas': ['Manaus', 'AM', 'Norte'],
            'Bahia': ['Salvador', 'BA', 'Nordeste'],
            'Ceará': ['Fortaleza', 'CE', 'Nordeste'],
            'Distrito Federal': ['Brasília', 'DF',
                                 'Centro-Oeste'],
            'Espírito Santo': ['Vitória', 'ES', 'Sudeste'],
            'Goiás': ['Goiânia', 'GO', 'Centro-Oeste'],
            'Maranhão': ['São Luís', 'MA', 'Nordeste'],
            'Mato Grosso': ['Cuiabá', 'MT', 'Centro-Oeste'],
            'Mato Grosso do Sul': ['Campo Grande', 'MS',
                                   'Centro-Oeste'],
            'Minas Gerais': ['Belo Horizonte', 'MG', 'Sudeste'],
            'Pará': ['Belém', 'PA', 'Norte'],
            'Paraíba': ['João Pessoa', 'PB', 'Nordeste'],
            'Paraná': ['Curitiba', 'PR', 'Sul'],
            'Pernambuco': ['Recife', 'PE', 'Nordeste'],
            'Piauí': ['Teresina', 'PI', 'Nordeste'],
            'Rio de Janeiro': ['Rio de Janeiro', 'RJ',
                               'Sudeste'],
            'Rio Grande do Norte': ['Natal', 'RN', 'Nordeste'],
            'Rio Grande do Sul': ['Porto Alegre', 'RS', 'Sul'],
            'Rondônia': ['Porto Velho', 'RO', 'Norte'],
            'Roraima': ['Boa Vista', 'RR', 'Norte'],
            'Santa Catarina': ['Florianópolis', 'SC', 'Sul'],
            'São Paulo': ['São Paulo', 'SP', 'Sudeste'],
            'Sergipe': ['Aracaju', 'SE', 'Nordeste'],
            'Tocantins': ['Palmas', 'TO', 'Norte']
        }

        self.group_system = QGroupBox('Sistemas')
        self.grid_sys = QGridLayout()
        self.grid_sys.setSpacing(50)

        self.sistema = QComboBox()
        self.bases = QComboBox()
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.locais = QComboBox()
        self.estados_regioes = QComboBox()

        self.ano_inicial_label = QLabel('ANO INICIAL:')
        self.ano_final_label = QLabel('ANO FINAL:')
        self.ano_inicial = QSpinBox()
        self.ano_inicial.setRange(2010, 2019)
        self.ano_final = QSpinBox()
        self.ano_final.setRange(2010, 2019)
        self.spin_cores_label = QLabel('SETAR CORES:')
        self.spin_cores = QSpinBox()
        self.spin_memoria_label = QLabel('SETAR MEMORIA:')
        self.spin_memoria = QSpinBox()
        self.carregar_banco = QPushButton('CARREGAR BANCO')
        self.visualizar_banco = QPushButton('VISUALIZAR BANCO')

        self.lista_botoes = [
            self.sistema, self.bases, self.locais, self.estados_regioes,
            self.ano_inicial, self.ano_final, self.spin_cores,
            self.spin_memoria, self.carregar_banco, self.visualizar_banco
        ]

        self.grid_sys.addWidget(self.sistema, 0, 0)
        self.grid_sys.addWidget(self.bases, 1, 0)
        self.grid_sys.addWidget(self.progress_bar, 2, 0, 2, 2)
        self.grid_sys.addWidget(self.locais, 0, 1)
        self.grid_sys.addWidget(self.estados_regioes, 1, 1)

        self.group_system.setLayout(self.grid_sys)

        self.group_funct = QGroupBox('Opções')
        self.grid_funct = QGridLayout()

        self.grid_funct.addWidget(self.ano_inicial_label, 0, 0)
        self.grid_funct.addWidget(self.ano_final_label, 1, 0)
        self.grid_funct.addWidget(self.ano_inicial, 0, 1, Qt.AlignLeft)
        self.grid_funct.addWidget(self.ano_final, 1, 1, Qt.AlignLeft)

        self.grid_funct.addWidget(self.spin_cores_label, 0, 2)
        self.grid_funct.addWidget(self.spin_cores, 0, 3, Qt.AlignLeft)
        self.grid_funct.addWidget(self.spin_memoria_label, 1, 2)
        self.grid_funct.addWidget(self.spin_memoria, 1, 3, Qt.AlignLeft)

        self.group_funct.setLayout(self.grid_funct)

        self.group_botoes = QGroupBox()
        self.grid_buttons = QGridLayout()

        self.grid_buttons.addWidget(self.carregar_banco, 0, 0)
        self.grid_buttons.addWidget(self.visualizar_banco, 0, 1)

        self.group_botoes.setLayout(self.grid_buttons)

        self.tabela = QTableWidget(10, 150)
        self.tabela.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.group_table = QGroupBox('Tabela')
        self.grid_table = QGridLayout()
        self.grid_table.addWidget(self.tabela)
        self.group_table.setLayout(self.grid_table)

        self.main_layout = QGridLayout()
        self.main_layout.setSpacing(10)

        self.main_layout.addWidget(self.group_system, 0, 0)
        self.main_layout.addWidget(self.group_funct, 0, 1)
        self.main_layout.addWidget(self.group_botoes, 1, 1)
        self.main_layout.addWidget(self.group_table, 2, 0, 1, 0)

        self.setLayout(self.main_layout)

        self.show()


class Transform(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('Transform')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.show()


class Profile(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('Profile')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.show()


class Exportar(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('Exportar')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.show()


class Ajuda(QWidget):

    def __init__(self):
        super().__init__()

        self.setWindowTitle('Ajuda')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.show()


class Configuracoes(QWidget):

    def __init__(self):
        super().__init__()
        # Em desenvolvimento
        self.setWindowTitle('Configurações')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)

        self.grupo_configuracoes = QGroupBox()
        self.grid_configuracoes = QGridLayout()

        self.layout_principal = QGridLayout()

        self.show()


class Pydbsus_gui(QMainWindow):

    def __init__(self, *abas):
        super().__init__()

        self.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt5'))
        self.tabs = QTabWidget()

        for aba in abas:
            self.tabs.addTab(aba, aba.windowTitle())

        self.setCentralWidget(self.tabs)

        self.show()


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
        self.botao_remover = QPushButton('Remover')
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
        self.grid_extracao.addWidget(self.botao_aplicar_aplicar, 3, 1, Qt.AlignRight)

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


def exportar_reduzido():
    try:
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        filename, _ = QFileDialog.getSaveFileName(etl,
                                                  'Save File', '',
                                                  'Arquivo csv(*.csv)')
        if filename:
            df_reduzido.to_csv(filename, index=False)
    except NameError:
        ...


def get_merge_data():
    merge_data(b1, b2, [x.text() for x in mesclar.colunas_selecionadas.selectedItems()])


def merge_data(b1, b2, columns):
    global df_reduzido
    b1 = b1[columns].groupby(columns).size().reset_index(name="b1_count")
    b2 = b2[columns].groupby(columns).size().reset_index(name="b2_count")
    df_reduzido = b1.merge(b2, on=columns)

    i = 0
    lista = df_reduzido.columns
    for column in lista:
        mesclar.tabela_juncao.setItem(0, i, QTableWidgetItem(column))
        i += 1

    column_n = 0
    row = 1
    for column in lista:
        for i in range(1, 11):
            mesclar.tabela_juncao.setItem(row, column_n,
                                          QTableWidgetItem(
                                              str(df_reduzido[column][i])))
            row += 1
        row = 1
        column_n += 1
    # return df_reduzido


def get_same_columns(df1, df2):
    result_index = []
    for column in df1.columns:
        if column in df2.columns:
            result_index.append(column)
    mesclar.selecionar_coluna.addItems(result_index)

    return result_index


def year_month(date):
    def correct_date(x):
        x = str(x)
        if len(x) < 8:
            x = "0" + x
        return x
    date = date.apply(lambda x: correct_date(x))
    date = pd.to_datetime(date.astype(str), format="%d%m%Y")
    year, month = date.dt.year, date.dt.month
    return (year, month)


def b1_():
    global b1
    filename, _ = QFileDialog.getOpenFileName(mesclar,
                                              'Open File',
                                              'Arquivo csv (*.csv)')
    b1 = pd.read_csv(filename, low_memory=False)
    if 'TIPOBITO' in b1.columns:
        year, month = year_month(b1['DTOBITO'])
        b1["YEAR"] = year
        b1["MONTH"] = month
    elif 'DTNASC' in b1.columns and 'TPOBITO' not in b1.columns:
        year, month = year_month(b1['DTNASC'])
        b1["YEAR"] = year
        b1["MONTH"] = month

    i = 0
    lista = b1.columns[1:]
    for column in lista:
        mesclar.tabela_esquerda.setItem(0, i, QTableWidgetItem(column))
        i += 1

    column_n = 0
    row = 1
    for column in lista:
        for i in range(1, 11):
            mesclar.tabela_esquerda.setItem(row, column_n,
                                            QTableWidgetItem(
                                                str(b1[column][i])))
            row += 1
        row = 1
        column_n += 1


def b2_():
    global b2
    filename, _ = QFileDialog.getOpenFileName(mesclar,
                                              'Open File',
                                              'Arquivo csv (*.csv)')
    b2 = pd.read_csv(filename, low_memory=False)
    if 'TIPOBITO' in b2.columns:
        year, month = year_month(b2['DTOBITO'])
        b2["YEAR"] = year
        b2["MONTH"] = month
    elif 'DTNASC' in b2.columns and 'TPOBITO' not in b2.columns:
        year, month = year_month(b2['DTNASC'])
        b2["YEAR"] = year
        b2["MONTH"] = month

    i = 0
    lista = b2.columns[1:]
    for column in lista:
        mesclar.tabela_direita.setItem(0, i, QTableWidgetItem(column))
        i += 1

    column_n = 0
    row = 1
    for column in lista:
        for i in range(1, 11):
            mesclar.tabela_direita.setItem(row, column_n,
                                           QTableWidgetItem(
                                               str(b2[column][i])))
            row += 1
        row = 1
        column_n += 1

    get_same_columns(b1, b2)


def add_column_same():
    text = mesclar.selecionar_coluna.currentText()
    mesclar.colunas_selecionadas.setItem(
        mesclar.colunas_selecionadas.currentRow(), 0,  QTableWidgetItem(text))


def sistema_bases(text):
    download.bases.clear()
    download.bases.setEnabled(True)

    if text == 'SIH':
        download.bases.addItem('Autorizações Hospitalares')
    elif text == 'SIM':
        download.bases.addItems(['Óbito', 'Óbito Fetal'])
    elif text == 'SINAN':
        download.bases.addItems([
            'Animais Peçonhentos', 'Botulismo', 'Chagas', 'Cólera',
            'Coqueluche', 'Difteria', 'Esquistossomose', 'Febre Maculosa',
            'Febre Tifóide', 'Hanseníase', 'Leptospirose', 'Meningite',
            'Raiva', 'Tétano', 'Tuberculose'
        ])
    elif text == 'SINASC':
        download.bases.addItem('Nascidos Vivos')
    else:
        download.bases.setEnabled(False)


def estado_regiao(text):
    download.estados_regioes.clear()
    download.estados_regioes.setEnabled(True)
    if text == 'ESTADO':
        for estado in download.estados.keys():
            download.estados_regioes.addItem(estado)
    elif text == 'REGIÃO':
        download.estados_regioes.addItems([
            'Norte', 'Nordeste', 'Sul', 'Sudeste', 'Centro-Oeste'
        ])
    else:
        download.estados_regioes.setEnabled(False)


def memoria():
    if platform.system().lower() == 'linux':
        with open('/proc/meminfo', 'r') as mem:
            ret = {}
            tmp = 0
            for i in mem:
                sline = i.split()
                if str(sline[0]) == 'MemTotal:':
                    ret['total'] = int(sline[1])
                elif str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                    tmp += int(sline[1])
            ret['free'] = tmp
            ret['used'] = int(ret['total']) - int(ret['free'])
        return round(int(ret['total'] / 1024) / 1000)

    elif platform.system().lower() == 'windows':
        return 4


def pega_datas():
    if download.ano_inicial.value() == download.ano_final.value():
        return download.ano_inicial.value()

    elif download.ano_inicial.value() < download.ano_final.value():
        return [data for data in range(download.ano_inicial.value(),
                                       download.ano_final.value() + 1)]
    elif download.ano_inicial.value() > download.ano_final.value():
        return [data for data in range(download.ano_final.value(),
                                       download.ano_inicial.value() + 1)]


def confere_regiao():
    if download.locais.currentText() == 'REGIÃO':
        return ([value[1] for value in list(download.estados.values())
                 if value[2] == download.estados_regioes.currentText()])

    elif download.locais.currentText() == 'ESTADO':
        return download.estados.get(
            download.estados_regioes.currentText())[1]

    elif download.locais.currentText() == 'TODOS':
        ufs = []
        for value in download.estados.values():
            ufs.append(value[1])

        return ufs


@pyqtSlot(int)
def update_progressbar(val):
    download.progress_bar.setValue(val)


def baixar_banco():
    condicao = [
        download.sistema.currentText() != 'SELECIONAR SISTEMA',
        download.locais.currentText() != 'SELECIONAR LOCAL'
    ]

    if all(condicao):
        sistema = download.sistema.currentText()
        base = download.bases.currentText()
        estados = confere_regiao()
        datas = pega_datas()

        argumentos = [sistema, base, estados, datas, download.estados]

        download.csv = _Thread(PyDatasus().get_csv_db_complete, *argumentos)
        download.loop = _Loop(download.csv)
        # download.loop.moveToThread(download.main_thread)
        download.loop.sinal.connect(update_progressbar)
        # download.csv.moveToThread(download.main_thread)
        download.csv.start()
        download.loop.start()


def ligar_spark():

    try:
        download.spark.stop()
    except AttributeError:
        ...

    cores = download.spin_cores.value()
    memoria = download.spin_memoria.value()

    conf = spark_conf('AggData', cores, memoria, 20)
    download.spark = start_spark(conf)
    return download.spark


def seleciona_banco():
    dicionario_doencas = {
        'Óbito': 'DO', 'Óbito Fetal': 'DOFE',
        'Animais Peçonhentos': 'ANIM', 'Botulismo': 'BOTU',
        'Chagas': 'CHAG', 'Cólera': 'COLE',
        'Coqueluche': 'COQU', 'Difteria': 'DIFT',
        'Esquistossomose': 'ESQU', 'Febre Maculosa': 'FMAC',
        'Febre Tifóide': 'FTIF', 'Hanseníase': 'HANS',
        'Leptospirose': 'LEPT', 'Meningite': 'MENI',
        'Raiva': 'RAIV', 'Tétano': 'TETA',
        'Tuberculose': 'TUBE', 'Nascidos Vivos': 'DN'
    }
    if download.bases.currentText() in dicionario_doencas:
        return dicionario_doencas.get(download.bases.currentText())


def visualizar_banco():
    global df
    spark = ligar_spark()

    sistema = download.sistema.currentText()
    datas = pega_datas()

    locais = confere_regiao()

    base = seleciona_banco()

    banco = []
    if sistema != 'SINAN':
        if isinstance(datas, list) and isinstance(locais, list):
            banco = [base + local + str(data) + '.csv'
                     for local in locais
                     for data in datas]

        elif isinstance(datas, int) and isinstance(locais, list):
            banco = [base + str(local) + str(datas) + '.csv'
                     for local in locais]

        elif isinstance(datas, list) and isinstance(locais, str):
            banco = [base + str(locais) + str(data) + '.csv'
                     for data in datas]

        elif isinstance(datas, int) and isinstance(locais, str):
            banco = [base + str(locais) + str(datas) + '.csv']
    elif sistema == 'SINAN':
        if isinstance(datas, list) and isinstance(locais, list):
            banco = [base + local + str(data)[2:4] + '.csv'
                     for local in locais
                     for data in datas]

        elif isinstance(datas, int) and isinstance(locais, list):
            banco = [base + str(local) + str(datas)[2:4] + '.csv'
                     for local in locais]

        elif isinstance(datas, list) and isinstance(locais, str):
            banco = [base + str(locais) + str(data)[2:4] + '.csv'
                     for data in datas]

        elif isinstance(datas, int) and isinstance(locais, str):
            banco = [base + str(locais) + str(datas)[2:4] + '.csv']

    bases = re.compile('|'.join(banco), re.IGNORECASE)
    caminho_sistema = path.expanduser(f'~/Documentos/files_db/{sistema}/')

    arquivos = []
    for arquivo_csv in listdir(caminho_sistema):
        if re.search(bases, arquivo_csv):
            arquivos.append(caminho_sistema + arquivo_csv)

    df = spark.read.csv(arquivos, header=True, inferSchema=True)

    return df


def insere_na_tabela():
    df = visualizar_banco()
    download.tabela.clear()
    etl.tabela_adicionar.clear()

    rows = {}
    if df.columns[0] == '_c0':
        for key in df.columns[1:]:
            rows[key] = []
    else:
        for key in df.columns:
            rows[key] = []

    coluna_ordenada = list(rows.keys())
    coluna_ordenada.sort()

    for i, key in enumerate(coluna_ordenada):
        download.tabela.setItem(0, i, QTableWidgetItem(key))
        etl.tabela_adicionar.setItem(i, 0, QTableWidgetItem(key))
        etl.condicao_coluna.addItem(key)
    column_n = 0
    row = 1

    for column in coluna_ordenada:
        for i in range(1, 11):
            download.tabela.setItem(
                row, column_n, QTableWidgetItem(
                    str(df.select(df[column]).take(i)[i - 1][0])))
            row += 1
        row = 1
        column_n += 1


def funciona_tabela():
    if (download.sistema.currentText() != 'SELECIONAR SISTEMA' and
            download.locais.currentText() != 'SELECIONAR LOCAL'):

        try:
            download.funcao_tabela = _Thread(insere_na_tabela)
            download.funcao_tabela.start()

            download.loop = _Loop(download.funcao_tabela)
            download.loop.sinal.connect(update_progressbar)
            download.loop.start()
        except:
            ...


def adicionar_item():
    selecionado = etl.tabela_adicionar.currentRow()
    local_aplicar = etl.tabela_aplicar.currentRow()
    coluna = etl.tabela_adicionar.item(selecionado, 0).text()
    etl.tabela_aplicar.setItem(local_aplicar, 0,
                               QTableWidgetItem(coluna))


def remover_item():
    selecionado = etl.tabela_aplicar.currentRow()
    etl.tabela_aplicar.removeRow(selecionado)


def aplicar_itens():
    global new_df
    etl.tabela_exportar.clear()
    colunas_selecionadas = []

    try:
        for coluna in etl.tabela_aplicar.selectedItems():
            colunas_selecionadas.append(coluna.text())

        new_df = ''

        remocao = []
        for coluna in df.columns:
            if coluna not in colunas_selecionadas:
                remocao.append(coluna)

        new_df = df.drop(*remocao)

        for i, coluna in enumerate(new_df.columns):
            etl.tabela_exportar.setItem(0, i, QTableWidgetItem(coluna))
        numero_column = new_df.select(new_df.columns[0]).count()

        column_n = 0
        row = 1
        for columns in new_df.columns:
            for i in range(1, 11):
                etl.tabela_exportar.setItem(
                    row, column_n, QTableWidgetItem(
                        str(new_df.select(
                            new_df[columns]).take(i)[i - 1][0])))
                row += 1
            row = 1
            column_n += 1
    except:
        ...


def thread_aplicar_itens():
    etl.thread = _Thread(aplicar_itens)
    etl.thread.start()


def exportar_df_csv():
    try:
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        filename, _ = QFileDialog.getSaveFileName(etl,
                                                  'Save File', '',
                                                  'csv(*.txt)')
        if filename:
            new_df.toPandas().to_csv(filename)
    except NameError:
        ...


def exportar_profile():
    try:
        etl.label_grafico.setText('Trabalhando na Analise')
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        filename, _ = QFileDialog.getSaveFileName(etl,
                                                  'Save File', '',
                                                  'html(*.txt)')
        if filename:
            try:
                tmp_new_df = new_df.toPandas()
                profile = ProfileReport(tmp_new_df,
                                        title='Profile',
                                        explorative=True)
                profile.to_file(f'{filename}')
                etl.label_grafico.setText('Analise gerado com sucesso')
            except:
                tmp_df = df.toPandas()
                profile = ProfileReport(tmp_df,
                                        title='Profile',
                                        explorative=True)
                profile.to_file(f'{filename}')
                etl.label_grafico.setText('Analise gerado com sucesso')
    except NameError:
        ...


@pyqtSlot(int)
def update_barra_html(val):
    etl.barra_salvar_html.setValue(val)


def thread_exportar_html():
    etl.thread_exportar = _Thread(exportar_profile)
    etl.thread_exportar.start()

    etl.thread_barra = _Loop(etl.thread_exportar)
    etl.thread_barra.sinal.connect(update_barra_html)
    etl.thread_barra.start()


if __name__ == '__main__':
    from sys import argv, exit
    from os import path, listdir
    from multiprocessing import cpu_count
    from time import sleep
    from threading import Thread
    import platform
    import re

    from pydatasus import PyDatasus
    from f_spark import spark_conf, start_spark

    app = QApplication(argv)

    download = Download()

    download.sistema.addItems([
        'SELECIONAR SISTEMA', 'SIH', 'SIM', 'SINAN', 'SINASC'
    ])
    download.sistema.currentTextChanged.connect(sistema_bases)
    download.bases.setEnabled(False)

    download.locais.addItems([
        'SELECIONAR LOCAL', 'TODOS', 'ESTADO', 'REGIÃO'
    ])
    download.locais.currentTextChanged.connect(estado_regiao)

    download.spin_cores.setMinimum(2)
    download.spin_cores.setValue(4)
    download.spin_cores.setMaximum(cpu_count() - 2)

    download.spin_memoria.setMinimum(2)
    download.spin_memoria.setValue(4)
    download.spin_memoria.setMaximum(memoria() - 4)

    download.carregar_banco.clicked.connect(baixar_banco)
    download.visualizar_banco.clicked.connect(funciona_tabela)

    etl = Etl()
    etl.botao_adicionar.clicked.connect(adicionar_item)
    etl.botao_remover.clicked.connect(remover_item)
    etl.botao_aplicar_aplicar.clicked.connect(thread_aplicar_itens)
    etl.botao_exportar.clicked.connect(exportar_df_csv)
    etl.botao_salvar_html.clicked.connect(exportar_profile)
    etl.condicao_coluna.addItem('Selecionar Coluna')
    etl.condicao_coluna.setEditable(True)

    ajuda = Ajuda()
    mesclar = Merge()

    mesclar.escolher_csv_esquerda.clicked.connect(b1_)
    mesclar.escolher_csv_direita.clicked.connect(b2_)
    mesclar.selecionar_coluna.setEditable(True)
    mesclar.adicionar_coluna.clicked.connect(add_column_same)
    mesclar.botao_aplicar.clicked.connect(get_merge_data)
    mesclar.botao_exportar.clicked.connect(exportar_reduzido)

    pydb = Pydbsus_gui(download, etl, mesclar, ajuda)
    exit(app.exec_())
