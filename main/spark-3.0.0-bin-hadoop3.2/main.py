#!/usr/bin/env python3

from sys import argv, exit
from os.path import expanduser
from os import listdir
from threading import Thread
from multiprocessing import cpu_count
from time import sleep
import re
from PyQt5.QtWidgets import (QMainWindow, QApplication, QTabWidget, QWidget,
                             QRadioButton, QPushButton, QProgressBar,
                             QSlider, QSpinBox, QLabel, QComboBox,
                             QTableWidget, QDialog)
from PyQt5.QtCore import QRect, pyqtSlot
from PyQt5 import QtCore
from PyQt5.QtGui import QFont

from pydatasus import PyDatasus
from convert_dbf_to_csv import ReadDbf
from f_spark import memory, spark_conf, start_spark, spark_df


class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.datasus = PyDatasus()
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
        self.resize(self.width_height[0], self.width_height[1])

        self.tabs_widget()
        self.buttons()

        self.show()

    def tabs_widget(self):
        self.tabs = QTabWidget()
        self.tab_download = QWidget()
        self.tab_config = QWidget()
        self.tab_manipulate = QWidget()
        self.tab_visualization = QWidget()
        self.tab_about = QWidget()

        self.tabs.addTab(self.tab_download, 'Downloads')
        self.tabs.addTab(self.tab_manipulate, 'Manipulação')
        self.tabs.addTab(self.tab_visualization, 'Visualização')
        self.tabs.addTab(self.tab_about, 'Ajuda')

        self.tabs.setCurrentIndex(0)

        self.setCentralWidget(self.tabs)

    def buttons(self):
        self.radio_button_sim = QRadioButton('SIM', self.tab_download)
        self.radio_button_sim.move(90, 70)

        self.radio_button_sinan = QRadioButton('SINAN', self.tab_download)
        self.radio_button_sinan.move(90, 130)

        self.radio_button_sinasc = QRadioButton('SINASC', self.tab_download)
        self.radio_button_sinasc.move(90, 190)

        QLabel('FILTRAR POR:', self.tab_download).move(420, 40)
        self.qcombo_limit = QComboBox(self.tab_download)
        self.qcombo_limit.addItems(['Todos', 'Região', 'Estado'])
        self.qcombo_limit.currentTextChanged.connect(self.select)
        self.qcombo_limit.setGeometry(420, 85, 110, 30)

        self.qcombo_select_limit = QComboBox(self.tab_download)
        self.qcombo_select_limit.currentTextChanged.connect(
            self.send_value_select_limit)
        self.qcombo_select_limit.setEditable(True)
        self.qcombo_select_limit.setEnabled(False)
        self.qcombo_select_limit.setGeometry(420, 130, 150, 30)

        self.qcombo_select_regiao = QComboBox(self.tab_download)
        self.qcombo_select_regiao.setEditable(True)
        self.qcombo_select_regiao.setEnabled(False)
        self.qcombo_select_regiao.setGeometry(420, 175, 150, 30)

        self.push_button_gen_csv = QPushButton('GERAR ARQUIVO CSV',
                                               self.tab_download)
        self.push_button_gen_csv.setGeometry(420, 220, 200, 30)
        self.push_button_gen_csv.clicked.connect(self.download_csv)

        self.push_button_gen_dbc = QPushButton('BAIXAR ARQUIVOS DBC',
                                               self.tab_download)
        self.push_button_gen_dbc.clicked.connect(self.download_dbc)
        self.push_button_gen_dbc.setGeometry(420, 265, 200, 30)

        self.push_button_stop = QPushButton('ENCERRAR PROCESSO',
                                            self.tab_download)
        self.push_button_stop.setGeometry(420, 310, 200, 30)

        self.progress_bar = QProgressBar(self.tab_download)
        self.progress_bar.setValue(0)
        self.progress_bar.setGeometry(75, 355, 560, 30)

#       self.push_button_start_spark = QPushButton('INICIAR SPARK',
#                                                  self.tab_config)
#       self.push_button_start_spark.clicked.connect(
#           lambda:
#               start_spark(spark_conf('SISTEMA DATASUS',
#                           self.spin_cores.value(),
#                           self.spin_memory.value())))

#       self.push_button_start_spark.move(40, 40)

#       self.push_button_stop_spark = QPushButton('ENCERAR SPARK',
#                                                 self.tab_config)
#       self.push_button_stop_spark.setGeometry(40, 100, 120, 30)

        QLabel('SELECIONAR SISTEMA:', self.tab_manipulate).move(40, 40)
        self.qcombo_select_system = QComboBox(self.tab_manipulate)
        self.qcombo_select_system.addItems(['SIM', 'SINAN', 'SINASC'])
        self.qcombo_select_system.move(230, 35)

        months = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
                  'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO',
                  'DEZEMBRO']

        QLabel('MÊS INICIAL:', self.tab_manipulate).move(40, 100)
        self.qcombo_month = QComboBox(self.tab_manipulate)
        self.qcombo_month.addItems(months)
        self.qcombo_month.move(200, 95)

        QLabel('MÊS FINAL:', self.tab_manipulate).move(40, 160)
        self.qcombo_month = QComboBox(self.tab_manipulate)
        self.qcombo_month.addItems(months)
        self.qcombo_month.move(200, 150)

        QLabel('ANO INICIAL:', self.tab_manipulate).move(40, 220)
        self.label_init_date = QLabel('2010', self.tab_manipulate)
        self.label_init_date.move(245, 240)
        self.qslider_init_date = QSlider(self.tab_manipulate,
                                         orientation=QtCore.Qt.Horizontal)
        self.qslider_init_date.setMinimum(2010)
        self.qslider_init_date.setMaximum(2019)
        self.qslider_init_date.setPageStep(1)
        self.qslider_init_date.valueChanged[
            'int'].connect(self.label_init_date.setNum)
        self.qslider_init_date.setGeometry(180, 220, 150, 30)

        QLabel('ANO FINAL:', self.tab_manipulate).move(40, 280)
        self.label_final_date = QLabel('2010', self.tab_manipulate)
        self.label_final_date.move(245, 300)
        self.qslider_final_date = QSlider(self.tab_manipulate,
                                          orientation=QtCore.Qt.Horizontal)
        self.qslider_final_date.setMinimum(2010)
        self.qslider_final_date.setMaximum(2019)
        self.qslider_final_date.setPageStep(1)
        self.qslider_final_date.valueChanged[
            'int'].connect(self.label_final_date.setNum)
        self.qslider_final_date.setGeometry(180, 280, 150, 30)

        self.push_button_apply = QPushButton('APLICAR FILTRO',
                                             self.tab_manipulate)
        self.push_button_apply.clicked.connect(self.load_database)
        self.push_button_apply.move(190, 360)

        self.table_preview = QTableWidget(5, 50, self.tab_manipulate)
        self.table_preview.setGeometry(350, 40, 700, 200)

        QLabel('SETAR CORES:', self.tab_manipulate).move(700, 443)
        QLabel('SETAR MEMÓRIA:', self.tab_manipulate).move(700, 503)
        self.spin_cores = QSpinBox(self.tab_manipulate)
        self.spin_cores.setMinimum(2)
        self.spin_cores.setMaximum(cpu_count() - 2)
        self.spin_cores.move(840, 433)

        self.spin_memory = QSpinBox(self.tab_manipulate)
        self.spin_memory.setMinimum(2)
        self.spin_memory.setMaximum(memory() - 4)
        self.spin_memory.move(850, 493)

    def deactivate_buttons_download(self):
        self.radio_button_sim.setEnabled(False)
        self.radio_button_sinan.setEnabled(False)
        self.radio_button_sinasc.setEnabled(False)
        self.push_button_gen_csv.setEnabled(False)
        self.push_button_gen_dbc.setEnabled(False)
        self.push_button_apply.setEnabled(False)

    def activate_buttons_download(self):
        self.radio_button_sim.setEnabled(True)
        self.radio_button_sinan.setEnabled(True)
        self.radio_button_sinasc.setEnabled(True)
        self.push_button_gen_csv.setEnabled(True)
        self.push_button_gen_dbc.setEnabled(True)
        self.push_button_apply.setEnabled(True)

    @pyqtSlot(str)
    def send_value_select_limit(self, text):
        data = []
        self.qcombo_select_regiao.clear()
        if 'Estado' == text:
            data = list(self.estados.keys())

        elif 'Norte' == text:
            for states, region in self.estados.items():
                if region[2] == 'Norte':
                    data.append(states)
            self.qcombo_select_regiao.setEnabled(True)
            self.qcombo_select_regiao.addItems(data)

        elif 'Nordeste' == text:
            for states, region in self.estados.items():
                if region[2] == 'Nordeste':
                    data.append(states)
            self.qcombo_select_regiao.setEnabled(True)
            self.qcombo_select_regiao.addItems(data)

        elif 'Sul' == text:
            for states, region in self.estados.items():
                if region[2] == 'Sul':
                    data.append(states)
            self.qcombo_select_regiao.setEnabled(True)
            self.qcombo_select_regiao.addItems(data)

        elif 'Sudeste' == text:
            for states, region in self.estados.items():
                if region[2] == 'Sudeste':
                    data.append(states)
            self.qcombo_select_regiao.setEnabled(True)
            self.qcombo_select_regiao.addItems(data)

        elif 'Centro-Oeste' == text:
            for states, region in self.estados.items():
                if region[2] == 'Centro-Oeste':
                    data.append(states)
            self.qcombo_select_regiao.setEnabled(True)
            self.qcombo_select_regiao.addItems(data)

        return data

    @pyqtSlot(str)
    def select(self, text):
        if 'Todos' == text:
            self.qcombo_select_limit.setEnabled(False)
            self.qcombo_select_regiao.setEnabled(False)
            self.qcombo_select_regiao.clear()
            self.qcombo_select_limit.clear()

        elif 'Região' == text:
            self.qcombo_select_limit.clear()
            self.qcombo_select_limit.setEnabled(True)
            self.qcombo_select_limit.addItems(['Norte', 'Nordeste', 'Sul',
                                               'Sudeste', 'Centro-Oeste'])

        elif 'Estado' == text:
            self.qcombo_select_limit.clear()
            self.qcombo_select_limit.setEnabled(True)
            self.qcombo_select_limit.addItems(
                self.send_value_select_limit(text))

            self.qcombo_select_regiao.clear()
            self.qcombo_select_regiao.setEnabled(False)

    def verify_radio_button_activate(self):
        if self.radio_button_sim.isChecked():
            return self.radio_button_sim.text()
        elif self.radio_button_sinan.isChecked():
            return self.radio_button_sinan.text()
        elif self.radio_button_sinasc.isChecked():
            return self.radio_button_sinasc.text()
        else:
            return 'pass'

    @pyqtSlot()
    def download_csv(self):
        thread = Thread(target=self.datasus.get_csv,
                        args=(self.verify_radio_button_activate(),),
                        daemon=True)
        thread.start()
        Thread(target=self.loop_progress_bar, args=(thread,),
               daemon=True).start()

    @pyqtSlot()
    def download_dbc(self):
        thread = Thread(target=self.datasus.get_db_partial,
                        args=(self.verify_radio_button_activate(),),
                        daemon=True)
        thread.start()
        Thread(target=self.loop_progress_bar, args=(thread,),
               daemon=True).start()

    def get_date_init_and_date_final(self):
        date_init = self.qslider_init_date.value()
        date_final = self.qslider_final_date.value()

        dates = []
        if date_init != date_final:
            if date_init < date_final:
                for date in range(date_init, date_final + 1):
                    dates.append(str(date))

            elif date_init > date_final:
                for date in range(date_final, date_init + 1):
                    dates.append(str(date))

        elif date_init == date_final:
            dates.append(date_init)

        return dates

    def load_database(self):
        count = 0
        dbases = []

        for date in self.get_date_init_and_date_final():
            system = self.qcombo_select_system.currentText()
            directory_db = expanduser('~/Documentos/files_db/' + system + '/')

            regex = re.compile(r'\w+BR{}\.\w+'.format(date))

            if re.findall(regex, ','.join(listdir(directory_db))):
                database = re.findall(regex, ','.join(listdir(directory_db)))
                for db in database:
                    dbases.append(directory_db + db)

            elif not re.findall(regex, ','.join(listdir(directory_db))):
                break

        if count == 0:
            thread = Thread(target=self.datasus.get_db_complete,
                            args=(system,), daemon=True)
            thread.start()

            Thread(target=self.loop_progress_bar, args=(thread,),
                   daemon=True).start()

            Thread(target=self.start_thread_convert, args=(thread, dbases,),
                   daemon=True).start()

        elif count > 0:
            process = Thread(target=self.read_database, args=(dbases,),
                             daemon=True)
            process.start()
            Thread(target=self.loop_progress_bar, args=(process,),
                   daemon=True).start()

    def start_thread_convert(self, thread, dbases):
        while thread.is_alive():
            sleep(2)

        process = Thread(target=self.read_database, args=(dbases,),
                         daemon=True)
        process.start()
        Thread(target=self.loop_progress_bar, args=(process,),
               daemon=True).start()

    def read_database(self, dbase):
        for db in dbase:
            ReadDbf(dbase)

    def loop_progress_bar(self, thread):
        self.deactivate_buttons_download()
        count = 0
        while thread.is_alive():
            if count == 100:
                count = 0
            self.progress_bar.setValue(count)
            count += 1
            sleep(1)
        self.progress_bar.setValue(100)
        self.activate_buttons_download()


if __name__ == '__main__':
    app = QApplication(argv)
    window = MainWindow()
    exit(app.exec_())
