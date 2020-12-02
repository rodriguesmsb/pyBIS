#!/usr/bin/env python3

from sys import argv, exit
from os.path import expanduser, isfile, dirname, join
from os import listdir, remove
from multiprocessing import cpu_count
from time import sleep
import re
from glob import glob
import plotly.offline as po
import plotly.graph_objs as go
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import (QMainWindow, QApplication, QTabWidget, QWidget,
                             QRadioButton, QPushButton, QProgressBar,
                             QSlider, QSpinBox, QLabel, QComboBox, QMessageBox,
                             QTableWidget, QDialog, QTableWidgetItem,
                             QHeaderView, QSizePolicy)
from PyQt5.QtCore import (QRect, pyqtSlot, QThread, pyqtSignal,
                          QObject, QSize)
from PyQt5 import QtCore
from PyQt5.QtGui import QFont
from pydatasus import PyDatasus
from convert_dbf_to_csv import ReadDbf
import pandas as pd

from f_spark import memory, spark_conf, start_spark


class Thread(QThread):

    intReady = pyqtSignal()
    finished = pyqtSignal(int)

    def __init__(self, fn, *args, **kwargs):
        super().__init__()

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.fn(*self.args, **self.kwargs)
        self.finished.emit(int)


class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()

        self.spark = ''

        self.title = 'Sistema Datasus'
        self.width_height = 1100, 700
        self.estados = {'Acre': ['Rio Branco', 'AC', 'Norte'],
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
        self.setupUI()

    def setupUI(self):
        self.setWindowTitle(self.title)
        self.setStyleSheet('font: 16px; font-family: Helvetica;')
        self.setFixedSize(self.width_height[0], self.width_height[1])

        self.tabs_widget()
        self.buttons()

        self.show()

    def tabs_widget(self):
        self.tabs = QTabWidget()
        self.tab_config = QWidget()
        self.tab_manipulate = QWidget()
        self.tab_profile = QWidget()
        self.tab_about = QWidget()

        self.tabs.addTab(self.tab_manipulate, 'Manipulação')
        self.tabs.addTab(self.tab_profile, 'Profile')
        self.tabs.addTab(self.tab_about, 'Ajuda')

        self.tabs.setCurrentIndex(0)

        self.setCentralWidget(self.tabs)

    def buttons(self):

        self.qcombo_select_system = QComboBox(self.tab_manipulate)
        self.qcombo_select_system.addItems(['SELECIONAR SISTEMA', 'SIHS',
                                            'SIM', 'SINAN', 'SINASC'])
        self.qcombo_select_system.currentTextChanged.connect(
            self.change_base_system)
        self.qcombo_select_system.setGeometry(40, 35, 210, 30)

        self.qcombo_select_database = QComboBox(self.tab_manipulate)
#       self.qcombo_select_database.currentTextChanged.connect(self.update_lines)
        self.qcombo_select_database.setEnabled(False)
        self.qcombo_select_database.setGeometry(40, 95, 210, 30)

        self.qcombo_select_state = QComboBox(self.tab_manipulate)
        self.qcombo_select_state.addItems(['TODOS', 'ESTADO', 'REGIÃO'])
        self.qcombo_select_state.currentTextChanged.connect(
            self.insert_region_combobox)

        self.qcombo_select_state.setGeometry(330, 35, 210, 30)

        self.qcombo_select_limit = QComboBox(self.tab_manipulate)
        self.qcombo_select_limit.setEnabled(False)
        self.qcombo_select_limit.setGeometry(330, 95, 210, 30)

        self.push_button_gen_csv = QPushButton('CARREGAR BANCO',
                                               self.tab_manipulate)
        self.push_button_gen_csv.clicked.connect(self.download_csv_dbc)

        self.push_button_gen_csv.setGeometry(600, 220, 200, 30)

        self.push_button_stop = QPushButton('ENCERRAR PROCESSO',
                                            self.tab_manipulate)
        self.push_button_stop.setGeometry(40, 155, 210, 30)

        QLabel('ANO INICIAL:', self.tab_manipulate).move(610, 35)
        self.label_init_date = QLabel('2010', self.tab_manipulate)
        self.label_init_date.move(920, 35)
        self.qslider_init_date = QSlider(self.tab_manipulate,
                                         orientation=QtCore.Qt.Horizontal)

        self.qslider_init_date.setMinimum(2010)
        self.qslider_init_date.setMaximum(2019)
        self.qslider_init_date.setPageStep(1)
        self.qslider_init_date.valueChanged[
            'int'].connect(self.label_init_date.setNum)
        self.qslider_init_date.setGeometry(760, 32, 150, 30)

        QLabel('ANO FINAL:', self.tab_manipulate).move(610, 90)
        self.label_final_date = QLabel('2010', self.tab_manipulate)
        self.label_final_date.move(920, 90)
        self.qslider_final_date = QSlider(self.tab_manipulate,
                                          orientation=QtCore.Qt.Horizontal)
        self.qslider_final_date.setMinimum(2010)
        self.qslider_final_date.setMaximum(2019)
        self.qslider_final_date.setPageStep(1)
        self.qslider_final_date.valueChanged[
            'int'].connect(self.label_final_date.setNum)
        self.qslider_final_date.setGeometry(760, 87, 150, 30)

        QLabel('SETAR CORES:', self.tab_manipulate).move(610, 150)
        QLabel('SETAR MEMÓRIA:', self.tab_manipulate).move(810, 150)

        self.spin_cores = QSpinBox(self.tab_manipulate)
        self.spin_cores.setMinimum(2)
        self.spin_cores.setValue(cpu_count() // 2)
        self.spin_cores.setMaximum(cpu_count() - 2)
        self.spin_cores.move(740, 145)

        self.spin_memory = QSpinBox(self.tab_manipulate)
        self.spin_memory.setMinimum(2)
        self.spin_memory.setValue(memory() // 2)
        self.spin_memory.setMaximum(memory() - 2)
        self.spin_memory.move(960, 145)

        self.push_button_apply = QPushButton('VISUALIZAR DADOS',
                                             self.tab_manipulate)
        self.push_button_apply.clicked.connect(self.start_spark_qt)
        self.push_button_apply.move(810, 220)

        self.table_preview = QTableWidget(10, 150, self.tab_manipulate)
        self.table_preview.setGeometry(40, 290, 1000, 344)

        self.all_buttons = [
            self.qcombo_select_system, self.qcombo_select_database,
            self.qcombo_select_state, self.qcombo_select_limit,
            self.push_button_gen_csv, self.push_button_stop,
            self.push_button_apply
        ]

        self.progress_bar = QProgressBar(self.tab_manipulate)
        self.progress_bar.setValue(0)
        self.progress_bar.setGeometry(40, 220, 505, 30)

###############################################################################

        self.qcombo_columns = QComboBox(self.tab_profile)
        self.qcombo_columns.setEditable(True)
        self.qcombo_columns.setEnabled(False)
        self.qcombo_columns.currentTextChanged.connect(self.altera_linhas)
        self.qcombo_columns.setGeometry(40, 35, 210, 30)

        self.table_dict = QTableWidget(1, 3, self.tab_profile)
        header = self.table_dict.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table_dict.setGeometry(40, 550, 1000, 90)
        self.table_dict.setItem(0, 0, QTableWidgetItem('Variavel'))
        self.table_dict.setItem(0, 1, QTableWidgetItem('Descrição'))
        self.table_dict.setItem(0, 2, QTableWidgetItem('Classe'))

        self.qpush_graphic = QPushButton('Gerar Gráfico', self.tab_profile)
        self.qpush_graphic.clicked.connect(
            self.exibe_dados)

        self.qpush_graphic.setGeometry(40, 100, 210, 30)
###############################################################################

    def load_columns(self):
        if self.data.columns[0] == '_c0':
            self.qcombo_columns.setEnabled(True)
            columns = [column for column in self.data.columns[1:]]
        else:
            self.qcombo_columns.setEnabled(True)
            columns = [column for column in self.data.columns]

        return columns

    def load_table(self):
        self.qcombo_columns.clear()
        pasta_raiz = dirname(__file__)
        pasta_dicionario = join(pasta_raiz, 'dicionarios')

        choice = {
            'Óbito': 'sim_obito.csv',
            'Óbito Fetal': 'sim_obito_fetal.csv',
            'Animais Peçonhentos': 'sinan_animais.csv',
            'Botulismo': 'sinan_botulismo.csv',
            'Chagas': 'sinan_chagas.csv',
            'Cólera': 'sinan_colera.csv',
            'Coqueluche': 'sinan_coqueluche.csv',
            'Difteria': 'sinan_difteria.csv',
            'Esquistossomose': 'sinan_esquistossomose.csv',
            'Febre Maculosa': 'sinan_maculosa',
            'Febre Tifóide': 'sinan_tifoide.csv',
            'Hanseníase': 'sinan_hanseniase.csv',
            'Leptospirose': 'sinan_leptospirose.csv',
            'Meningite': 'sinan_meningite',
            'Raiva': 'sinan_raiva.csv',
            'Tétano': 'sinan_tetano.csv',
            'Tuberculose': 'sinan_tuberculose.csv',
            'Nascidos Vivos': 'sinasc_nascidos.csv'
        }
        try:
            if self.qcombo_select_database.currentText() in choice.keys():
                dicionario = choice.get(
                    self.qcombo_select_database.currentText())
                print(pasta_dicionario + '/' + dicionario)
                self.df = pd.read_csv(pasta_dicionario + '/' + dicionario)
            self.qcombo_columns.addItems(self.load_columns())
        except:
            ...

    @pyqtSlot(str)
    def altera_linhas(self, text):
        try:
            if text in [obj for obj in self.df['variavel']]:
                val = self.df.loc[self.df['variavel'] == text].index[0]
                variavel, descricao, classe = self.df.iloc[val, [0, 1, 2]]
                self.table_dict.setItem(0, 0, QTableWidgetItem(variavel))
                self.table_dict.setItem(0, 1, QTableWidgetItem(descricao))
        except:
            ...

            try:
                self.table_dict.setItem(0, 2,
                                        QTableWidgetItem(
                                            classe.strip('{').strip('}')))
            except:
                ...

    def exibe_dados(self):
        text = self.qcombo_columns.currentText()
        data_groupby = self.data.groupby(text).count().take(10)
        column_var = list(dict(data_groupby).keys())
        column_value = list(dict(data_groupby).values())

        fig = go.Figure(data=[{'type': 'bar', 'x': column_var,
                               'y': column_value}])
        fig.update_layout(
            title=self.qcombo_select_system.currentText(),
            xaxis_title=text,
            yaxis_title='count')

        self.fig_view = self.show_qt(fig)

    def change_base_system(self):
        self.qcombo_select_database.clear()
        self.qcombo_select_database.setEnabled(True)

        if self.qcombo_select_system.currentText() == 'SIM':
            obitos = ['Todos', 'Óbito', 'Óbito Fetal']
            self.qcombo_select_database.addItems(obitos)

        elif self.qcombo_select_system.currentText() == 'SINAN':
            animais = [
                'Todos', 'Animais Peçonhentos', 'Botulismo', 'Chagas',
                'Cólera', 'Coqueluche', 'Difteria', 'Esquistossomose',
                'Febre Maculosa', 'Febre Tifóide', 'Hanseníase',
                'Leptospirose', 'Meningite', 'Raiva', 'Tétano', 'Tuberculose'
            ]
            self.qcombo_select_database.addItems(animais)

        elif self.qcombo_select_system.currentText() == 'SINASC':
            self.qcombo_select_database.addItem('Nascidos Vivos')

        elif self.qcombo_select_system.currentText() == 'SELECIONAR SISTEMA':
            self.qcombo_select_database.setEnabled(False)

    def insert_region_combobox(self):
        self.qcombo_select_limit.clear()
        self.qcombo_select_limit.setEnabled(True)

        if self.qcombo_select_state.currentText() == 'ESTADO':
            for state in self.estados.keys():
                self.qcombo_select_limit.addItem(state)

        elif self.qcombo_select_state.currentText() == 'REGIÃO':
            self.qcombo_select_limit.addItems(
                ['Norte', 'Nordeste', 'Sul', 'Sudeste', 'Centro-Oeste'])

        else:
            self.qcombo_select_limit.setEnabled(False)

    def range_date(self):
        if self.qslider_init_date.value() < self.qslider_final_date.value():
            return [date for date in range(self.qslider_init_date.value(),
                                           self.qslider_final_date.value()
                                           + 1)]

        elif self.qslider_init_date.value() > self.qslider_final_date.value():
            return [date for date in range(self.qslider_final_date.value(),
                                           self.qslider_init_date.value() + 1)]

        elif self.qslider_init_date.value() == self.qslider_final_date.value():
            return self.qslider_init_date.value()

    def loop(self):
        [button.setEnabled(False) for button in self.all_buttons]
        cnt = 0
        while self.csv.isRunning():
            sleep(0.3)
            cnt += 1
            if cnt == 99:
                cnt = 0
            self.progress_bar.setValue(cnt)
        self.dbc.start()
        while self.dbc.isRunning():
            sleep(0.3)
            cnt += 1
            if cnt == 99:
                cnt = 0
            self.progress_bar.setValue(cnt)

        sleep(1)
        self.progress_bar.setValue(100)
        sleep(2)
        self.progress_bar.setValue(0)
        [button.setEnabled(True) for button in self.all_buttons]

    def get_qcombobox(self):

        if self.qcombo_select_state.currentText() == 'TODOS':
            return None

        elif self.qcombo_select_state.currentText() == 'ESTADO':
            return self.estados.get(self.qcombo_select_limit.currentText())[1]

        elif self.qcombo_select_state.currentText() == 'REGIÃO':
            return [value[1] for value in list(self.estados.values())
                    if value[2] == self.qcombo_select_limit.currentText()]

    def download_csv_dbc(self):
        dict_database = {
            'Óbito': 'DO', 'Óbito Fetal': 'DOFE',
            'Animais Pençonhentos': 'ANIM', 'Botulismo': 'BOTU',
            'Chagas': 'CHAG', 'Cólera': 'COLE',
            'Coqueluche': 'COQU', 'Difteria': 'DIFT',
            'Esquistossomose': 'ESQU', 'Febre Maculosa': 'FMAC',
            'Febre Tifóide': 'FTIF', 'Hanseníase': 'HANS',
            'Leptospirose': 'LEPT', 'Meningite': 'MENI',
            'Raiva': 'RAIV', 'Tétano': 'TETA',
            'Tuberculose': 'TUBE'
        }

        if 'SELECIONAR SISTEMA' != self.qcombo_select_system.currentText():
            date = self.range_date()
            states = self.get_qcombobox()

            system = self.qcombo_select_system.currentText()
            database = self.qcombo_select_database.currentText()

            cond = [system, database, states, date, self.estados]

            self.csv = Thread(PyDatasus().get_csv, *cond)
            self.dbc = Thread(PyDatasus().get_db_complete, system)

            self.loop_function = Thread(self.loop)

            self.csv.start()
            self.loop_function.start()

    def loop_convert(self):
        [button.setEnabled(False) for button in self.all_buttons]
        cnt = 0
        while self.clean.isRunning():
            cnt += 1
            sleep(0.3)
            if cnt > 99:
                cnt = 0
            self.progress_bar.setValue(cnt)
        self.progress_bar.setValue(100)
        sleep(2)
        self.progress_bar.setValue(0)
        [button.setEnabled(True) for button in self.all_buttons]

    def clean_folder(self, system):
        [button.setEnabled(False) for button in self.all_buttons]
        data_DBC = [x for x in glob(expanduser(
            f'~/Documentos/files_db/{system}/*.DBC'))]
        data_dbc = [x for x in glob(expanduser(
            f'~/Documentos/files_db/{system}/*.dbc'))]

        list(map(ReadDbf, data_dbc))
        list(map(ReadDbf, data_DBC))

        list(map(remove, data_dbc))
        list(map(remove, data_DBC))
        self.clean_dbf(system)
        [button.setEnabled(True) for button in self.all_buttons]

    def clean_dbf(self, system):
        data_dbf = [x for x in glob(expanduser(
            f'~/Documentos/files_db/{system}/*.dbf'))]

        list(map(remove, data_dbf))

    def convert_or_no(self, system):
        try:
            check = ''
            for db in listdir(expanduser(f'~/Documentos/files_db/{system}/')):
                if (db.endswith('.dbc') or db.endswith('.DBC')
                        or db.endswith('.dbf')):
                    check = True
                else:
                    ...

            return check
        except:
            ...

    def start_spark_qt(self):
        if self.qcombo_select_system.currentText() != 'SELECIONAR SISTEMA':
            system = self.qcombo_select_system.currentText()

            try:
                self.spark.stop()
            except AttributeError:
                ...

            memory = self.spin_memory.value()
            cores = self.spin_cores.value()

            config = spark_conf('AggData', cores, memory)
            self.spark = start_spark(config)
            dbc_folder = glob(expanduser(
                f'~/Documentos/files_db/{system}/*.csv'))

            if self.convert_or_no(system):
                try:
                    self.clean = Thread(self.clean_folder, system)
                    self.clean.start()

                    self.outra_thread = Thread(self.loop_convert)
                    self.outra_thread.start()

                except:
                    ...

            try:
                self.data = self.spark.read.csv(dbc_folder, header=True)

                self.insercao = Thread(self.insert_data_table, self.data)
                self.insercao.start()
            except:
                ...

            self.load_table()

    def insert_data_table(self, data):
        self.table_preview.clear()
        rows = {}

        if data.columns[0] == '_c0':
            for key in data.columns[1:]:
                rows[key] = []
        else:
            for key in data.columns:
                rows[key] = []

        for i, key in enumerate(list(rows.keys())):
            self.table_preview.setItem(0, i, QTableWidgetItem(key))

        column_n = 0
        row = 1

        for column in rows.keys():
            for i in range(1, 11):
                self.table_preview.setItem(
                    row, column_n, QTableWidgetItem(
                        str(data.select(data[column]).take(i)[i - 1][0])))
                row += 1
            row = 1
            column_n += 1

    def show_qt(self, fig):
        raw_html = '<html><head><meta charset="utf-8" />'
        raw_html += '''
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script></head>
        '''
        raw_html += '<body>'
        raw_html += po.plot(fig, include_plotlyjs=False, output_type='div')
        raw_html += '</body></html>'

        fig_view = QWebEngineView()
        fig_view.setHtml(raw_html)
        fig_view.show()
        fig_view.raise_()
        return fig_view


if __name__ == '__main__':
    app = QApplication(argv)
    window = MainWindow()
    exit(app.exec_())
