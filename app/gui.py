#!/usr/bin/env python3

import sys
import time
import os

"""
try:
    os.system("pip install -r {}".format(
              os.path.join(os.path.dirname(__file__),
                           '../requirements.txt')))
except:
    pass
"""

import subprocess
from datetime import date
import pathlib
import shutil
import multiprocessing
import psutil
import re
import requests
from requests.exceptions import ConnectionError
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QMessageBox, QPushButton, QTableWidgetItem,
    QTabBar, QTabWidget, QStyle, QStyleOptionTab, QStylePainter, QFileDialog,
    QHeaderView, QStyleFactory, QDateEdit)
from PyQt5.QtGui import (QFont, QIcon, QStandardItemModel, QStandardItem,
        QPixmap, QColor)
from PyQt5.QtCore import (QThread, pyqtSignal, QObject, QRect, QPoint, QSize,
                          QDate)
from PyQt5 import uic
import json
import pandas as pd
import webbrowser

from pydatasus import PyDatasus
# from convert_dbf_to_csv import ReadDbf
from circularprogressbar import QRoundProgressBar


sys.path.append(os.path.join(os.path.dirname(__file__),
                "../scripts/SpatialSUSapp/"))

dir_ui = os.path.join(os.path.dirname(__file__), "../layouts/")
dir_ico = os.path.join(os.path.dirname(__file__), "../assets/")
conf = os.path.join(os.path.dirname(__file__), "../conf/")
dir_dbc = os.path.expanduser("~/datasus_dbc/")
dir_sus_conf = os.path.join(os.path.dirname(__file__),
                            "../scripts/SpatialSUSapp/conf/")
blast_ = os.path.join(os.path.dirname(__file__), './blast-dbf')


class Error(QMessageBox):
    def __init__(self):
        super().__init__()

        self.setupUI()

    def setupUI(self):
        self.setFont(QFont("Arial", 15))
        self.setWindowTitle("Erro!")
        self.setStandardButtons(self.Ok)


class TabBar(QTabBar):
    def tabSizeHint(self, index):
        s = QTabBar.tabSizeHint(self, index)
        s.transpose()
        return s

    def paintEvent(self, event):
        painter = QStylePainter(self)
        opt = QStyleOptionTab()

        for i in range(self.count()):
            self.initStyleOption(opt, i)
            painter.drawControl(QStyle.CE_TabBarTabShape, opt)
            painter.save()

            s = opt.rect.size()
            s.transpose()
            r = QRect(QPoint(), s)
            r.moveCenter(opt.rect.center())
            opt.rect = r

            c = self.tabRect(i).center()
            painter.translate(c)
            painter.rotate(90)
            painter.translate(-c)
            painter.drawControl(QStyle.CE_TabBarTabLabel, opt)
            painter.restore()


class TabWidget(QTabWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setTabBar(TabBar(self))
        self.setTabPosition(QTabWidget.West)


class TestConnection(QObject):

    status = pyqtSignal(str)

    def __init__(self):
        super().__init__()

    def check(self):
        self.activate = True
        while self.activate:
            time.sleep(5)
            try:
                if requests.get('https://www.google.com').status_code:
                    self.status.emit('Conectado a internet')
                else:
                    self.status.emit('Sem internet')
            except ConnectionError:
                pass


class AttSizeBase(QObject):
    ftpdatasus_signal = pyqtSignal(int)
    opendatasus_signal = pyqtSignal(int)

    def __init__(self):
        super().__init__()

    def opendatasus(self):
        self.datasus_activate = True

        while self.datasus_activate:
            time.sleep(5)
            size = 0
            opendatasus = os.path.expanduser('~/OpenDatasus/')
            try:
                for path, dirs, files in os.walk(opendatasus):
                    for f in files:
                        fp = os.path.join(path, f)
                        size += os.path.getsize(fp)
            except FileNotFoundError:
                pass

            self.opendatasus_signal.emit(int(size / (1024**3)))

    def ftpdatasus(self):
        self.ftp_activate = True

        while self.ftp_activate:
            size = 0
            time.sleep(5)
            opendatasus = os.path.expanduser('~/datasus_dbc/')
            try:
                for path, dirs, files in os.walk(opendatasus):
                    for f in files:
                        fp = os.path.join(path, f)
                        size += os.path.getsize(fp)
            except FileNotFoundError:
                pass

            self.ftpdatasus_signal.emit(int(size / (1024**3)))


class Manager(QMainWindow):

    resized = pyqtSignal()

    def __init__(self, *tabs):
        super().__init__()

        self.setWindowTitle("pyBiss")

        self.internet = TestConnection()
        self.internet.status.connect(self.statusBar().showMessage)

        self.thread = Thread(self.internet.check)
        self.thread.start()

        self.tab_manager = TabWidget()

        for tab in tabs:
            self.tab_manager.addTab(tab, QIcon(tab.windowIcon()), '')
            self.tab_manager.setIconSize(QSize(50, 100))

        self.setCentralWidget(self.tab_manager)
        self.show()

    def resizeEvent(self, event):
        self.resized.emit()

    def set_font_size(self):
        curr_geo = self.geometry().width()
        new_pixel_size = curr_geo//90

        self.setFont(QFont("Arial", new_pixel_size))

    def set_font(self, font):
        self.setFont(QFont(font))

    def closeEvent(self, event):
        self.internet.activate = False
        self.thread.terminate()


class Thread(QThread):
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        try:
            self.terminate()
        except RuntimeError:
            pass

    def run(self):
        self.func(*self.args, **self.kwargs)


class DownloadUi(QMainWindow):

    signal_col_etl = pyqtSignal(str)
    signal_clear_add = pyqtSignal(int)
    signal_clear_items = pyqtSignal(int)
    signal_cols_analysis = pyqtSignal(str)
    signal_export_count = pyqtSignal(int)
    signal_column_export = pyqtSignal(list)
    signal_column = pyqtSignal(list)
    signal_etl_el = pyqtSignal(list)
    signal_finished = pyqtSignal(int)
    signal_txt = pyqtSignal(str)
    signal_pbar = pyqtSignal(int)
    signal_error = pyqtSignal(list)
    resized = pyqtSignal()

    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "download.ui", self)
        self.reset()

        self.comboBox.currentTextChanged.connect(self.load_database)
        self.comboBox.currentTextChanged.connect(self.write_database)
        self.comboBox_2.currentTextChanged.connect(self.return_code_base)
        self.comboBox_3.currentTextChanged.connect(self.load_limit)
        self.comboBox_4.currentTextChanged.connect(self.return_uf)
        self.comboBox_5.currentTextChanged.connect(self.select_system_download)
        self.pushButton.clicked.connect(self.process_download)
        self.pushButton_2.clicked.connect(self.load_data_table)
        self.pushButton_3.clicked.connect(self.stop_thread)
        self.horizontalSlider.valueChanged.connect(self.return_date)
        self.horizontalSlider_2.valueChanged.connect(self.return_date_)
        self.spinBox.valueChanged.connect(self.mem)
        self.spinBox.setMaximum(self.get_size())
        self.spinBox_2.valueChanged.connect(self.cpu)
        self.spinBox_2.setMaximum(multiprocessing.cpu_count())

        self.all_buttons = self.findChildren(QPushButton)
        self.signal_error.connect(self.showError)
        self.resized.connect(self.set_font_size)

    def resizeEvent(self, event):
        self.resized.emit()

    def set_font_size(self):
        curr_geo = self.geometry().width()
        new_pixel_size = curr_geo//90

        self.setFont(QFont("Areial", new_pixel_size))

    def showError(self, signal):
        self.error = Error()
        self.error.setText(signal[1])
        try:
            self.error.setWindowIcon(QIcon(signal[2]))
            self.error.setIconPixmap(QPixmap(signal[2]))
        except IndexError:
            pass
        self.error.buttonClicked.connect(lambda: self.destroy)
        self.error.show()

    def get_size(self):
        swap = psutil.swap_memory()
        mem = str(swap.total // 1024)
        mem = mem[0:2]
        return int(mem)

    def select_system_download(self):
        if self.comboBox_5.currentText() == 'FTP Datasus':
            self.comboBox.clear()
            self.comboBox_2.clear()
            self.comboBox.addItems(['SELECIONAR SISTEMA DE DADOS',
                                    'SIHSUS',
                                    'SIM',
                                    'SINAN',
                                    'SINASC'])

            self.label.setText('Ano inicial:')
            self.label_2.setText('Ano final:')
            self.horizontalSlider.show()
            self.horizontalSlider_2.show()
            self.lcdNumber.show()
            self.lcdNumber_2.show()
            self.dt_min.hide()
            self.dt_max.hide()

        elif self.comboBox_5.currentText() == 'OpenDatasus':
            self.comboBox.clear()
            self.comboBox_2.clear()
            self.comboBox.addItems(['SELECIONAR SISTEMA DE DADOS',
                                    'REGISTROS DE VACINAÇÃO COVID-19',
                                    'SÍNDROME GRIPAL',
                                    'OCUPAÇÃO HOSPITALAR COVID-19',
                                    ])

            self.label.setText('Período inicial:')
            self.label_2.setText('Período final:')

            self.lcdNumber.hide()
            self.lcdNumber_2.hide()

            self.horizontalSlider.hide()
            self.horizontalSlider_2.hide()
            dt_min = QDate(2020, 1, 1)
            dt_max = QDate(date.today())

            self.dt_min = QDateEdit()
            self.dt_min.setMinimumDate(dt_min)
            self.dt_min.setMaximumDate(dt_max)
            self.dt_max = QDateEdit()
            self.dt_max.setMinimumDate(dt_min)
            self.dt_max.setDate(QDate(date.today()))
            self.dt_max.setMaximumDate(dt_max)

            self.gridLayout_6.addWidget(self.dt_min, 0, 2, 1, 4)
            self.gridLayout_6.addWidget(self.dt_max, 1, 2, 1, 4)
            self.gridLayout_6.addWidget(self.spinBox, 2, 3, 1, 3)
            self.gridLayout_6.addWidget(self.spinBox_2, 3, 3, 1, 3)

    def reset(self):
        try:
            with open(conf + "search.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(conf + "search.json", "w", encoding='utf8') as f:
                data["database"] = ""
                data["base"] = ""
                data["limit"] = ""
                data["date_range"] = ["2010"]
                json.dump(data, f, indent=4)

            with open(conf + "config.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(conf + "config.json", "w", encoding='utf8') as f:
                data["mem"] = "2"
                data["cpu"] = "2"
                json.dump(data, f, indent=4)

            with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
                data["time_range"] = [2010]
                json.dump(data, f, indent=4)
        except FileNotFoundError:
            self.ifJsonNotExist()

    def ifJsonNotExist(self):
        data = {}
        with open(conf + "search.json", "w", encoding='utf8') as f:
            data["database"] = ""
            data["base"] = ""
            data["limit"] = ""
            data["date_range"] = ["2010"]
            json.dump(data, f, indent=4)

        data = {}
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["mem"] = "2"
            data["cpu"] = "2"
            json.dump(data, f, indent=4)

        data = {}
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            data["time_range"] = [2010]
            json.dump(data, f, indent=4)

    def mem(self, val):
        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["mem"] = val
            json.dump(data, f, indent=4)

    def cpu(self, val):
        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["cpu"] = val
            json.dump(data, f, indent=4)

    def return_code_base(self, choice: str) -> str:
            if self.comboBox.currentText().lower() == "sihsus":
                self.write_base("RD")
            elif (self.comboBox.currentText().lower()
                    != "selecionar sistema de dados"):
                with open(conf + "database.json", "r", encoding='utf8') as f:
                    database_json = json.load(f)
                    try:
                        self.write_base(
                            database_json[self.comboBox.currentText()][choice]
                        )
                    except KeyError:
                        pass
            else:
                self.write_base("")

    def return_uf(self, limit: str)-> str:
        ufs = set()
        with open(conf + "locales.json", "r", encoding='utf8') as f:
            stts_json = json.load(f)
            if limit != '':
                if limit == 'Brasil':
                    regiao = [
                       'Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul'
                    ]
                    for reg in regiao:
                        for uf in stts_json[reg]:
                            ufs.add(uf)
                            ufs.add("BR")

                elif (limit in stts_json.keys()
                        and limit not in ['Norte', 'Nordeste',
                                          'Centro-Oeste', 'Sul', 'Sudeste']
                        ):
                    ufs.add(stts_json[limit])

                else:
                    try:
                        for uf in stts_json[limit]:
                            ufs.add(uf)
                    except KeyError:
                        pass

            with open(conf + "search.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(conf + "search.json", "w", encoding='utf8') as f:
                data["limit"] = list(ufs)
                json.dump(data, f, indent=4)

        with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            if limit == "BRASIL":
                data["area"] = limit.lower()
            elif limit in ['Norte', 'Nordeste', 'Centro-Oeste',
                            'Sudeste', 'Sul']:
                data["area"] = limit.lower()
            else:
                data["area"] = ''.join(list(ufs)).lower()

            json.dump(data, f, indent=4)

    def write_database(self, database: str):
        if database.lower() == "selecionar sistema de dados":
            with open(conf + "search.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(conf + "search.json", "w", encoding='utf8') as f:
                    data["database"] = ""
                    json.dump(data, f, indent=4)
            with open(dir_sus_conf + "conf.json",
                      "r", encoding='utf8') as f:
                data = json.load(f)
            with open(dir_sus_conf + "conf.json",
                      "w", encoding='utf8') as f:
                data["sistema"] = ""
                json.dump(data, f, indent=4)
        else:
            with open(conf + "search.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(conf + "search.json", "w", encoding='utf8') as f:
                    data["database"] = database
                    json.dump(data, f, indent=4)

            with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
                data["sistema"] = database
                json.dump(data, f, indent=4)

    def write_base(self, base: str):
        with open(conf + "search.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "search.json", "w", encoding='utf8') as f:
            data["base"] = base
            json.dump(data, f, indent=4)

    def write_date(self, date: list):
        with open(conf + "search.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "search.json", "w", encoding='utf8') as f:
            data["date_range"] = date
            json.dump(data, f, indent=4)

        with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            data["time_range"] = date
            json.dump(data, f, indent=4)

    def return_list_date(self, date: int, date_: int) -> list:
        if date < date_:
            self.write_date([str(dt) for dt in range(date, date_ + 1)])
        elif date > date_:
            self.write_date([str(dt) for dt in range(date_, date + 1)])
        else:
            self.write_date([str(date)])

    def load_database(self, database: str):
        def read_json_database(database):
            with open(conf + 'database.json', 'r', encoding='utf8') as f:
                self.comboBox_2.clear()
                self.comboBox_2.setEnabled(True)
                bases = []
                data = json.load(f)
                try:
                    data = data[database]
                    for base in data:
                        bases.append(base)
                    bases.sort()
                    self.comboBox_2.addItems(bases)
                except KeyError:
                    pass

        if database.lower() != 'selecionar sistema de dados':
            if database.lower() == 'sihsus':
                database = 'SIH'
                read_json_database(database)
            else:
                read_json_database(database)
        else:
            self.comboBox_2.clear()
            self.comboBox_2.setEnabled(False)

    def load_limit(self, limit: str):
        def load_states():
            with open(conf + 'locales.json', 'r', encoding='utf8') as f:
                self.comboBox_4.setEnabled(True)
                states = json.load(f)
                states_list = []
                for keys in states.keys():
                    if keys not in ['Norte', 'Nordeste',
                            'Sudeste', 'Sul', 'Centro-Oeste']:
                        states_list.append(keys)
                        states_list.sort()
                self.comboBox_4.clear()
                self.comboBox_4.addItems(states_list)

        if limit.lower() != "selecionar local":
            if limit.lower() == 'estado':
                load_states()
            elif limit.lower() == 'região':
                self.comboBox_4.clear()
                self.comboBox_4.addItems(
                    ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']
                )
                self.comboBox_4.setEnabled(True)
            elif limit.lower() == 'brasil':
                self.comboBox_4.clear()
                self.comboBox_4.addItem('Brasil')
                self.comboBox_4.setEnabled(False)
        else:
            self.comboBox_4.clear()
            self.comboBox_4.setEnabled(False)

    def return_date(self, date):
        self.return_list_date(date, self.horizontalSlider_2.value())

    def return_date_(self, date_):
        self.return_list_date(date_, self.horizontalSlider.value())

    def load_conf(self):
        with open(conf + "search.json", 'r', encoding='utf8') as f:
            data = json.load(f)
            database = data["database"]
            base = data["base"]
            state = data["limit"]
            date = data["date_range"]
            return database, base, state, date

    def process_download(self):
        def elastic_download():
            try:
                pathlib.Path(
                    os.path.expanduser('~/OpenDatasus/')
                ).mkdir(parents=True, exist_ok=True)
            except FileExistsError:
                pass

            directory = os.path.expanduser('~/OpenDatasus/')
            database, base, states, dates = self.load_conf()

            db = ''
            from datetime import date
            if database == 'REGISTROS DE VACINAÇÃO COVID-19':
                db = 'vacinacao'
            elif database == 'SÍNDROME GRIPAL':
                db = 'notificacao_sg'
            elif database == 'OCUPAÇÃO HOSPITALAR COVID-19':
                db = 'leitos_covid19'

            filename = db + date.today().strftime('%d-%m-%y')
            from pyOpenDatasus import PyOpenDatasus

            date_min = str(self.dt_min.date().toPyDate())
            date_max = str(self.dt_max.date().toPyDate())

            for state in states:
                api = PyOpenDatasus(db, state, date_min, date_max)
                api.sig.connect(self.progressBar.setValue)
                api.download(
                    os.path.join(directory, filename + '_' + state + '.csv')
                )
            print('concluido')


        if self.comboBox_5.currentText() == 'FTP Datasus':
            if not all(self.load_conf()):
                self.signal_error.emit(
                    [
                        1, 'Precisa selecionar todos os parametros de download',
                        dir_ico + 'cat_sad_2.png']
                )
            else:
                pydatasus = PyDatasus()
                pydatasus.download_signal.connect(self.progressBar.setValue)
                pydatasus.label_signal.connect(self.label_5.setText)
                pydatasus.lcd_signal.connect(self.lcdNumber_3.display)
                pydatasus.finished.connect(self.finished)
                [btn.setEnabled(False)
                        for btn in self.all_buttons
                        if btn.text().lower() != "encerrar"
                ]
                self.thread_download = Thread(pydatasus.get_data,
                                              *self.load_conf()
                )
                self.thread_download.start()

        elif self.comboBox_5.currentText() == 'OpenDatasus':
            self.thread_thread = Thread(elastic_download)
            self.thread_thread.start()

    def finished(self, val):
        [btn.setEnabled(True) for btn in self.all_buttons]

    def stop_thread(self):
        [btn.setEnabled(True) for btn in self.all_buttons]
        self.progressBar.setValue(0)
        self.tableWidget.clear()
        try:
            self.lcdNumer_3.display(0)
            self.label_5.setText("")
        except AttributeError:
            pass
        try:
            self.thread_download.stop()
        except AttributeError:
            pass
        try:
            self.thread_table.stop()
        except AttributeError:
            pass

    def load_data_table(self):
        if not all(self.load_conf()):
            self.signal_error.emit(
                [
                    1,
                    'Precisa selecionar todos os parametros de vizualização',
                    dir_ico + 'cat_sad_2.png']
            )
        else:
            self.database, self.base, self.limit, self.date = self.load_conf()

            if self.database == 'SINAN':
                if isinstance(self.date, list):
                    self.date = [x[2:4] for x in self.date]
                elif isinstance(self.date, str):
                    self.date = self.date[2:4]
            elif self.database == 'SIHSUS':
                if isinstance(self.date, list):
                    self.date = [x[2:4] + r'\d{2}' for x in self.date]
                elif isinstance(self.date, str):
                    self.date = self.date[2:4] + r'\d{2}'

            if isinstance(self.limit, str) and isinstance(self.date, str):
                self.regex = [ self.base + self.limit + self.date + '.csv' ]
            elif isinstance(self.limit, str) and isinstance(self.date, list):
                self.regex = [
                    self.base
                    + self.limit
                    + date
                    + '.csv' for
                    date in self.date 
                ]
            elif isinstance(self.limit, list) and isinstance(self.date, str):
                self.regex =  [
                    self.base + state + self.date + '.csv'
                    for state in self.limit 
                ]
            elif isinstance(self.limit, list) and isinstance(self.date, list):
                self.regex =  [
                    self.base + state + date + '.csv'
                    for state in self.limit
                    for date in self.date
                ]

            bases = re.compile("|".join(self.regex))
            files = []
            if self.database.lower() == "sih":
                self.database = "SIHSUS"
            dir_database = os.path.expanduser(
                "~/datasus_dbc/" + self.database + "/"
            )
            try:
                folder_csv = os.listdir(dir_database)
                for file_csv in folder_csv:
                    if re.search(bases, file_csv):
                        files.append(os.path.expanduser(
                            "~/datasus_dbc/" + self.database + "/" + file_csv)
                        )

                self.files = files

                self.thread_table = Thread(self.read_file)
                self.thread_table.start()
                [btn.setEnabled(False)
                        for btn in self.all_buttons
                        if btn.text().lower() != "encerrar"
                ]
            except FileNotFoundError:

                self.signal_error.emit(
                    [
                        1, f'A pasta {self.database} não foi encontrada',
                        dir_ico + 'cat_sad_3.png'
                    ]
                )

    def read_file(self):

        def unionAll(dfs):
            cols = set()

            for df in dfs:
                for col in df.columns:
                    cols.add(col)
            cols = sorted(cols)

            new_dfs = {}

            for i, d in enumerate(dfs):
                new_name = 'df' + str(i)
                new_dfs[new_name] = d

                for col in cols:
                    if col not in d.columns:
                        new_dfs[new_name] = new_dfs[new_name].withColumn(col,
                                                                    F.lit(0))
                new_dfs[new_name] = new_dfs[new_name].select(cols)
            result = new_dfs['df0']
            dfs_to_add = new_dfs.keys()
            keys = list(dfs_to_add)
            keys.remove('df0')

            for x in dfs_to_add:
                result = result.union(new_dfs[x])
            return result

        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)

            if sys.platform == "win32":
                os.environ['HADOOP_HOME'] = os.path.join(os.path.dirname(__file__), '../hadoop-3.2.2/')
                import findspark
                findspark.init(os.path.join(os.path.dirname(__file__), '../spark-3.2.0-bin-hadoop2.7/'))
            from pyspark import SparkConf
            from pyspark.sql import SparkSession
            # import pyspark.sql.types as T
            import pyspark.sql.functions as F

            conf_file = SparkConf().setMaster(
                'local[*]'.replace('*', str(data['cpu']))) \
                .setAll([
                    ("spark.executor.memory", "ng".replace("n",
                                                           str(data["mem"]))),
                    ("spark.driver.memory", "20g"),
                    ("spark.driver.bindAddres", "127.0.0.1"),
                    ]).setAppName('pyBis')

            self.spark = SparkSession.builder.config(conf=conf_file).getOrCreate()

            try:

                self.lista_spark = list(
                    map(lambda x: self.spark.read.csv(x, header=True),
                        self.files)
                )


                self.df = unionAll(self.lista_spark)
                self.write_table()
            except:
                self.signal_error.emit(
                    [
                        1, 'O arquivo solicitado não foi encontrado',
                        dir_ico
                        + 'cat_sad_3.png'
                   ]
                )
                self.stop_thread()


    def receive_data(self, dataframe):
        self.df = dataframe

    def ant_bug_column(self, col):
        self.tableWidget.setHorizontalHeaderItem(col[0], col[1])

    def write_header(self):
        self.signal_finished.connect(self.finished)

        self.tableWidget.clear()
        if self.df.columns[0] == "_c0":
            self.cols = [col for col in self.df.columns[1:]]
        else:
            self.cols = [col for col in self.df.columns]

        self.cols.sort()
        self.tableWidget.setColumnCount(len(self.cols))
        self.signal_clear_add.emit(1)
        self.signal_clear_items.emit(1)
        self.signal_cols_analysis.emit("")
        for i, col in enumerate(self.cols):
            self.signal_col_etl.emit(col)
            column = QTableWidgetItem(col)
            self.signal_column.connect(self.ant_bug_column)
            self.signal_column.emit([i, column])
            i += 1

    def write_body(self):
        self.tableWidget.setRowCount(20)
        val = 0

        self.percentage = 0
        self.signal_txt.connect(self.label_5.setText)
        self.signal_txt.emit("Escrevendo seus dados")

        df = self.df.head(20)
        try:
            for id_col, columns in enumerate(self.cols):
                val += 1
                for n in range(20):
                    self.tableWidget.setItem(
                        n, id_col, QTableWidgetItem(df[n].asDict()[columns])
                    )

                self.signal_pbar.connect(self.progressBar.setValue)
                self.signal_pbar.emit(int(round(
                    100 * round((float(val / len(list(df[0].asDict().keys())))
                        * 100 - 6), 1) / (100 - 6), 1
                    ))
                )

            self.signal_txt.emit("Os dados foram escritos com sucesso")
            self.signal_finished.emit(1)
        except IndexError:
            self.signal_txt.emit("Não há nada aqui")
            self.signal_finished.emit(1)

    def write_table(self):
        self.write_header()
        self.write_body()

    def trim_data(self, params):
        drop_list = []
        if params[0] != None:
            for itm in self.df.columns:
                if itm not in params[0]:
                    drop_list.append(itm)
            try:
                self.data_drop = self.df.drop(*drop_list)
            except TypeError:
                self.data_drop = self.df.drop(drop_list, axis=1)

            if params[1] != None:
                self.data_filtered = self.data_drop.filter(
                    ' and '.join(params[1])
                )
                try:
                    self.data_filtered = self.data_drop.query(
                        ' and '.join(params[1])
                    )
                except AttributeError:
                    pass
            elif params[1] == None:
                self.data_filtered = self.data_drop

        elif params[0] == None:
            self.data_drop = self.df
            if params[1] != None:
                self.data_filtered = self.data_drop.filter(
                    ' and '.join(params[1])
                )
                try:
                    self.data_filtered = self.data_drop.query(
                        ' and '.join(params[1])
                    )
                except AttributeError:
                    pass
            elif params[1] == None:
                self.data_filtered = self.data_drop

        self.signal_export_count.emit(len(self.data_filtered.columns))

        self.signal_clear_items.emit(1)

        self.signal_cols_analysis.emit("")
        for i, col in enumerate(self.data_filtered.columns):
            self.signal_cols_analysis.emit(col)
            column = QTableWidgetItem(col)
            self.signal_column_export.emit([i, column])
            i += 1


        col_n = 0
        row_n = 0

        try:
            for col in self.data_filtered.columns:
                for r in range(1, 21):
                    self.signal_etl_el.emit(
                        [row_n, col_n, QTableWidgetItem(str(
                            self.data_filtered.select(
                        self.data_filtered[col]).take(r)[r - 1][0]))])
                    row_n += 1
                row_n = 0
                col_n += 1
        except AttributeError:
            for e, col in enumerate(self.data_filtered.columns):
                for r in range(20):
                    try:
                        self.signal_etl_el.emit(
                            [
                                r, e, QTableWidgetItem(
                                    str(self.data_filtered[col].iloc[r]))
                            ]
                        )
                    except KeyError:
                        pass

        except IndexError:
            print("Argumento incorreto para a coluna")

    def save_file(self, params):
        try:
            self.data_filtered.coalesce(1).write.mode('overwrite').csv(params[2], header=True)

        except AttributeError:
            pathlib.Path(os.path.join(os.path.dirname(__file__),
                "../scripts/SpatialSUSapp/data/")
            ).mkdir(parents=True, exist_ok=True)

            self.data_filtered.to_csv(params[2] + 'new.csv')

        for file in os.listdir(os.path.join(os.path.dirname(__file__),
                                            "../scripts/SpatialSUSapp/data/")):
            if file.endswith(".csv"):
                os.rename(
                    os.path.join(os.path.dirname(__file__),
                                 f"../scripts/SpatialSUSapp/data/{file}"),
                    os.path.join(os.path.dirname(__file__),
                                 f"../scripts/SpatialSUSapp/data/data.csv")
                )


class EtlUi(QMainWindow):

    signal_trim_data = pyqtSignal(list)
    signal_save = pyqtSignal(list)

    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "etl.ui", self)

        self.model_col_add = QStandardItemModel()
        self.model_col_apply = QStandardItemModel()
        self.model_col_ext = QStandardItemModel()

        self.column_add.setModel(self.model_col_add)
        self.column_apply.setModel(self.model_col_apply)
        self.column_apply.doubleClicked.connect(self.remove_col_apply)
        self.column_add.doubleClicked.connect(self.add_col_apply
        )

        self.column_ext.setModel(self.model_col_ext)

        self.button_lt.clicked.connect(self.send_op)
        self.button_gt.clicked.connect(self.send_op)
        self.button_lte.clicked.connect(self.send_op)
        self.button_gte.clicked.connect(self.send_op)
        self.button_dif.clicked.connect(self.send_op)
        self.button_equal.clicked.connect(self.send_op)
        self.button_and.clicked.connect(self.send_op)
        self.button_or.clicked.connect(self.send_op)
        self.button_not.clicked.connect(self.send_op)
        self.button_in.clicked.connect(self.send_op)

        self.gen_filter.clicked.connect(self.add_filter_to_list)
        self.rm_filter.clicked.connect(self.rm_el_list_filter)
        self.apply_filter.clicked.connect(self.apply_all_filters)
        self.pushButton.clicked.connect(self.export_file_csv)

    def convert_model(self, col):
        itm = QStandardItem(col)
        self.model_col_add.appendRow(itm)

    def clear_models(self, val):
        self.model_col_add.clear()
        self.model_col_apply.clear()
        self.model_col_ext.clear()
        self.line_select.clear()

    def add_col_apply(self):
        apply_itms = [
            self.model_col_apply.item(index).text()
            for index in range(self.model_col_apply.rowCount())
        ]
        add_itms = self.column_add.selectedIndexes()

        for itm in add_itms:
            lines = []
            if itm.data() not in apply_itms:
                lines.append(QStandardItem(itm.data()))
                self.model_col_apply.appendRow(lines)

    def remove_col_apply(self):
        itms = self.column_apply.selectedIndexes()
        for itm in itms:
            self.model_col_apply.takeRow(itm.row())

    def send_op(self):
        ops = [
            "menor", "maior", "menor igual", "maior igual", "diferente",
            "igual", "e", "ou", "não", "em"
        ]
        ops_t = [
            "<", ">", "<=", ">=", "!=", "==", "and", "or", "not", "in"
        ]

        if self.sender().text().lower() in ops:
            self.op = ops_t[ops.index(self.sender().text().lower())]
        self.line_edit.setText(self.op + " ")

    def add_filter_to_list(self):
        self.model_col_ext.appendRow(QStandardItem(
            self.line_select.currentText() + " " + self.line_edit.text()))

    def rm_el_list_filter(self):
        itms = self.column_ext.selectedIndexes()
        for itm in itms:
            self.model_col_ext.takeRow(itm.row())

    def apply_all_filters(self):
        self.drop_list = None
        self.filter_list = None

        if len(range(self.model_col_apply.rowCount())):
           self.drop_list = []
           for idx in range(self.model_col_apply.rowCount()):
               self.drop_list.append(self.model_col_apply.item(idx).text())
           self.table_export.setRowCount(20)

        if len(range(self.model_col_ext.rowCount())):
           self.filter_list = []
           for idx in range(self.model_col_ext.rowCount()):
               self.filter_list.append(self.model_col_ext.item(idx).text())

        self.signal_trim_data.emit([self.drop_list, self.filter_list])

        try:
            shutil.rmtree(os.path.join(os.path.dirname(__file__),
                '../scripts/SpatialSUSapp/data/')
            )
        except:
            pass

        self.signal_save.emit(
            ["header", "true", os.path.join(os.path.dirname(__file__),
             "../scripts/SpatialSUSapp/data/")
             ]
        )

        try:
            os.rename("mv {} {}".format(
                os.path.join(os.path.dirname(__file__),
                    "../scripts/SpatialSUSapp/data/*.csv"),
                os.path.join(os.path.dirname(__file__),
                    "../scripts/SpatialSUSapp/data/data.csv"))
            )
        except:
            pass

    def header_etl(self, col):
        self.table_export.setHorizontalHeaderItem(col[0], col[1])

    def header_etl_count(self, n):
        self.table_export.setColumnCount(n)
        self.table_export.setRowCount(20)

    def build_table(self, params):
        self.table_export.setItem(params[0], params[1], params[2])

    def export_file_csv(self):
        try:
            filename, _ = QFileDialog.getSaveFileName(self.pushButton,
                                                      "Salvar Arquivo",
                                                      f"{dir_dbc}",
                                                      "Arquivo csv (*.csv)")
            if filename.endswith(".csv"):
                self.signal_save.emit(["header", "true", filename])
            else:
                self.signal_save.emit(["header", "true",
                                              filename + ".csv"])
        except NameError:
            pass


class MergeUi(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "merge.ui", self)

        self.tables = []
        self.select_file1.clicked.connect(self.select_file)
        self.select_file2.clicked.connect(self.select_file)
        self.button_add.clicked.connect(self.add_col)
        self.pushButton.clicked.connect(self.remove)
        self.button_apply.clicked.connect(self.merge_data)
        self.model_add = QStandardItemModel()
        self.table3.setModel(self.model_add)

    def select_file(self):
        try:
            filename, _ = QFileDialog.getOpenFileName(self,
                                                      "Carregar Arquivo",
                                                      f"{dir_dbc}",
                                                      "Arquivo csv (*.csv)")

            if self.sender().objectName() == "select_file1":
                self.df_1 = self.verify_column(pd.read_csv(filename,
                                               low_memory=False,
                                               encoding="iso-8859-1"))
                self.table1.clear()
                self.table1.setRowCount(20)
                self.table1.setColumnCount(len(self.df_1.columns))
                for e, col in enumerate(self.df_1.columns):
                    self.table1.setHorizontalHeaderItem(e,
                                                        QTableWidgetItem(col))
                    self.table1.horizontalHeader().setSectionResizeMode(e,
                        QHeaderView.Stretch
                    )
                    e += 1

                col_n = 0
                row_n = 0
                for col in self.df_1.columns:
                    for i in range(0, 21):
                        try:
                            self.table1.setItem(row_n, col_n,
                                QTableWidgetItem(str(self.df_1[col][i])))
                        except KeyError:
                            pass
                        row_n += 1
                    row_n = 0
                    col_n += 1

                self.tables.append(self.df_1)

            elif self.sender().objectName() == "select_file2":
                self.df_2 = self.verify_column(pd.read_csv(filename,
                                               low_memory=False,
                                               encoding="iso-8859-1"))
                self.table2.clear()
                self.table2.setRowCount(20)
                self.table2.setColumnCount(len(self.df_2.columns))
                self.table2.setRowCount(20)
                for e, col in enumerate(self.df_2.columns):
                    self.table2.setHorizontalHeaderItem(e,
                                                        QTableWidgetItem(col))
                col_n = 0
                row_n = 0
                for col in self.df_2.columns:
                    for i in range(0, 21):
                        try:
                            self.table2.setItem(row_n, col_n,
                                QTableWidgetItem(str(self.df_2[col][i])))
                        except KeyError:
                            pass
                        row_n += 1
                    row_n = 0
                    col_n += 1
                    e += 1

                self.tables.append(self.df_2)
        except FileNotFoundError:
            pass

        if len(self.tables) > 1:
            self.get_same_columns()

    def get_same_columns(self):
        self.add_line.clear()
        result_index = []
        for cols in self.df_1.columns:
            if cols in self.df_2.columns:
                result_index.append(cols)
        self.add_line.addItems(result_index)
        self.tables.clear()

    def year_month(self, date):
        def correct_state(x):
            x = str(x)
            if len(x) < 8:
                x = "0" + x
            return x

        date = date.apply(lambda x: correct_state(x))
        date = pd.to_datetime(date.astype(str), format="%d%m%Y")
        year, month = date.dt.year, date.dt.month

        return (year, month)

    def verify_column(self, data):
        if "TIPOBITO" in data.columns:
            year, month = self.year_month(data["DTOBITO"])
            data["YEAR"] = year
            data["MONTH"] = month
        elif "DTNASC" in data.columns and "TPOBITO" not in data.columns:
            year, month = self.year_month(data["DTNASC"])
            data["YEAR"] = year
            data["MONTH"] = month

        return data

    def add_col(self):
        self.model_add.appendRow(QStandardItem(self.add_line.currentText()))

    def remove(self):
        itms = self.table3.selectedIndexes()
        for itm in itms:
            self.model_add.takeRow(itm.row())

    def merge_data(self):
        columns = []

        for index in range(self.model_add.rowCount()):
            columns.append(self.model_add.item(index).text())

        df_1 = self.df_1[columns].groupby(columns).size().reset_index(
                name="b1_count")

        df_2 = self.df_2[columns].groupby(columns).size().reset_index(
                name="b2_count"
        )

        self.df_reduce = pd.merge(df_1, df_2, on=columns)

        i = 0
        self.table4.setColumnCount(len(self.df_reduce.columns))
        self.table4.setRowCount(20)
        for col in self.df_reduce.columns:
            self.table4.setHorizontalHeaderItem(i, QTableWidgetItem(col))
            i += 1

        col_n = 0
        row_n = 0
        for col in self.df_reduce.columns:
            for r in range(0, 21):
                try:
                    self.table4.setItem(row_n, col_n,
                        QTableWidgetItem(str(self.df_reduce[col][r])))
                except KeyError:
                    pass
                row_n += 1
            row_n = 0
            col_n += 1


class AnalysisUi(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "analysis.ui", self)

        self.radioButton.toggled.connect(self.configure_combobox)
        self.radioButton_2.toggled.connect(self.configure_combobox)
        self.radioButton_3.toggled.connect(self.configure_combobox)

        self.comboBox.currentTextChanged.connect(self.write_column_var)
        self.comboBox_2.currentTextChanged.connect(self.write_column_var)

        self.lineEdit.textChanged.connect(self.write_text)

        self.pushButton.clicked.connect(self.terminate)
        self.pushButton_2.clicked.connect(self.start_server)
        self.pushButton_3.clicked.connect(self.write_items)

        self.tableWidget.doubleClicked.connect(self.deleteClicked)

        self.tableWidget.setColumnCount(2)

        types = ['Tipo da variavel', 'Nome da variavel']

        for i in range(2):
            self.tableWidget.setHorizontalHeaderItem(
                i, QTableWidgetItem(types[i])
            )
            self.tableWidget.horizontalHeader().setSectionResizeMode(
                i, QHeaderView.Stretch
            )

    def write_items(self):
        def write_json(items):
            with open(dir_sus_conf + 'conf.json', 'r',
                      encoding='utf8') as f:
                data = json.load(f)
            with open(dir_sus_conf + 'conf.json', 'w',
                      encoding='utf8') as f:
                data['var_type'] = []
                data['var_col'] = []

                for n, itm in enumerate(items):
                    if n % 2:
                        data['var_type'].append(itm)
                    else:
                        data['var_col'].append(itm)

                json.dump(data, f, indent=4)

        if (self.comboBox_3.currentText() in ['Categorica', 'Numerica']
                and self.comboBox_4.currentText() != ''):
            rowPosition = self.tableWidget.rowCount()
            var = self.comboBox_4.currentText()
            type_var = self.comboBox_3.currentText()
            self.tableWidget.insertRow(rowPosition)

            self.tableWidget.setItem(rowPosition, 1, QTableWidgetItem(var))
            self.tableWidget.setItem(rowPosition, 0,
                                     QTableWidgetItem(type_var))

            items = []
            for row in range(self.tableWidget.rowCount()):
                for col in range(self.tableWidget.columnCount()):
                    _item = self.tableWidget.item(row, col)
                    if _item:
                        item = self.tableWidget.item(row, col).text()
                        items.append(item)

            write_json(items)

    def deleteClicked(self, mi):
        def write_json(items):
            with open(dir_sus_conf + 'conf.json', 'r',
                      encoding='utf8') as f:
                data = json.load(f)
            with open(dir_sus_conf + 'conf.json', 'w',
                      encoding='utf8') as f:
                data['var_type'] = []
                data['var_col'] = []

                for n, itm in enumerate(items):
                    if n % 2:
                        data['var_type'].append(itm)
                    else:
                        data['var_col'].append(itm)

                json.dump(data, f, indent=4)

        self.tableWidget.removeRow(mi.row())

        items = []
        for row in range(self.tableWidget.rowCount()):
            for col in range(self.tableWidget.columnCount()):
                _item = self.tableWidget.item(row, col)
                if _item:
                    item = self.tableWidget.item(row, col).text()
                    items.append(item)

        write_json(items)

    def configure_combobox(self):
        radiobutton = self.sender()
        if radiobutton.isChecked():
            if radiobutton.text() == "Espacial":
               self.comboBox.setEnabled(True)
               self.comboBox_2.setEnabled(False)
               self.write_chocie_json("spatial")
            elif radiobutton.text() == "Espaço temporal":
                self.comboBox.setEnabled(True)
                self.comboBox_2.setEnabled(True)
                self.write_chocie_json("spatio_temporal")
            elif radiobutton.text() == "Temporal":
                self.comboBox.setEnabled(False)
                self.comboBox_2.setEnabled(True)
                self.write_chocie_json("temporal")

    def write_chocie_json(self, e):
        with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            data["type"] = e
            json.dump(data, f, indent=4)

    def write_text(self, e):
        with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            data["name"] = e
            json.dump(data, f, indent=4)

    def write_column_var(self, e):
        if self.sender().objectName() == "comboBox":
            self.id_area(self.sender().currentText())

        elif self.sender().objectName() == "comboBox_2":
            self.time_col(self.sender().currentText())

    def id_area(self, var):
        with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            data["id_area"] = var
            json.dump(data, f, indent=4)

    def time_col(self, var):
        with open(dir_sus_conf + "conf.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(dir_sus_conf + "conf.json", "w", encoding='utf8') as f:
            data["time_col"] = var
            json.dump(data, f, indent=4)

    def start_server(self):
        if self.radioButton_2.isChecked():
            self.server_spatio_temporal = subprocess.Popen(
                ["python3", os.path.join(os.path.dirname(__file__),
                "../scripts/SpatialSUSapp/spatio_temporal.py")
                ]
            )
            time.sleep(2)
            webbrowser.open('127.0.0.1:8050')
        elif self.radioButton.isChecked():
            self.msg = Error()
            self.msg.setText("Em desenvolvimento")
            self.msg.setWindowIcon(QIcon(dir_ico + "cat_dev.png"))
            self.msg.setIconPixmap(QPixmap(dir_ico + "cat_dev.png"))
            self.msg.buttonClicked.connect(lambda: self.destroy)
            self.msg.show()

        elif self.radioButton_3.isChecked():
            self.server_temporal = subprocess.Popen(
                ["python3", os.path.join(os.path.dirname(__file__),
                "../scripts/SpatialSUSapp/temporal.py")
                ]
            )
            time.sleep(2)
            webbrowser.open('127.0.0.1:8050')

    def terminate(self):
        os.system(
            "kill -9 $(netstat -tulpn | grep 8050 | awk '{print $7}'\
                    | egrep ^[0-9]{1\,6}) 2>/dev/null")
        try:
            self.server_temporal.terminate()
        except AttributeError:
            try:
                self.server_spatio_temporal.terminate()
            except AttributeError:
                pass
        try:
            self.server_spatio_temporal.terminate()
        except AttributeError:
            try:
                self.server_temporal.terminate()
            except AttributeError:
                pass

    def clear_items(self, val):
        self.comboBox_4.clear()
        self.comboBox.clear()
        self.comboBox_2.clear()
        self.comboBox.addItem('Selecionar variavel de espaço')
        self.comboBox_2.addItem('Selecionar variavel de tempo')

    def update_items(self, cols):
        self.comboBox_4.addItem(cols)
        self.comboBox.addItem(cols)
        self.comboBox_2.addItem(cols)


class LoadFile(QMainWindow):

    column_data = pyqtSignal(str)
    dataframe = pyqtSignal(pd.DataFrame)
    clear_dataframe = pyqtSignal(int)

    def __init__(self):
        super().__init__()

        uic.loadUi(dir_ui + "load.ui", self)
        self.files = []

        self.pushButton.clicked.connect(self.loadFile)
        self.pushButton_2.clicked.connect(self.readList)
        self.pushButton_3.clicked.connect(self.clear)
        self.listWidget.itemDoubleClicked.connect(self.remove_db)

    def loadFile(self):
        def convert(df):
            if df[-4:] == ".dbf" or df[-4:] == ".DBF":
                # nem lembro pq eu usava isso
                csv = df.replace('.dbf', '.csv').replace('.DBF', '.csv')

                string = ReadDbf({df}, convert='convert', tmp=True)
                self.files.append(pd.read_csv(string.tmp_file))

            elif df[-4:] == ".csv":
                self.files.append(pd.read_csv(df))

            elif df[-4:] == ".dbc":
                dbf = df[:-3] + "dbf"
                os.system(f"{blast_} {df} {dbf}")

                string = ReadDbf({dbf}, convert=True, tmp=True)

                self.files.append(pd.read_csv( string.tmp_file))

            elif df[-4:] == ".xlsx" or df[-4:] == ".xls":
                self.files.append(pd.read_excel(df))

            self.listWidget.addItem(df)

        file_ = QFileDialog.getOpenFileNames(self, "Carregar arquivo",
            os.path.expanduser('~/'),
            ("Arquivo csv (*.csv);;Arquivo Excel (*.xlsx *.xls);;\
              Arquivo dbf (*.DBF *.dbf);;Arquivo dbc (*.dbc)")
        )

        list(map(convert, file_[0]))

    def readList(self):
        try:
            self.all_files = pd.concat(self.files)
            self.write_table(self.all_files)

            self.dataframe.emit(self.all_files)
        except ValueError:
            msg_error = "Nenhum arquivo foi carregado como banco de dados"
            self.error = Error()
            self.error.setWindowIcon(QIcon(dir_ico + "cat_sad_3"))
            self.error.setIconPixmap(QPixmap(dir_ico + "cat_sad_3"))
            self.error.setText(msg_error)
            self.error.show()

    def write_table(self, data):
        self.tableWidget.setRowCount(data.shape[0])
        self.tableWidget.setColumnCount(data.shape[1])

        for e, column in enumerate(sorted(data.columns.to_list())):
            self.tableWidget.setHorizontalHeaderItem(
                e, QTableWidgetItem(column))

            self.column_data.emit(column)

            for row, ele in enumerate(data[column]):
                self.tableWidget.setItem(
                    row, e, QTableWidgetItem(str(ele))
                )

    def remove_db(self):
        coluna = self.listWidget.selectedItems()
        self.files.pop(self.listWidget.currentRow())
        for col in coluna:
            self.listWidget.takeItem(self.listWidget.row(col))

    def clear(self):
        self.files.clear()
        self.tableWidget.clear()
        self.listWidget.clear()
        self.tableWidget.setRowCount(0)
        self.tableWidget.setColumnCount(0)


class Config(QMainWindow):

    send_font = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "config.ui", self)

        self.load_conf()
        self.pushButton_3.clicked.connect(self.write_conf_spark)
        self.pushButton_4.clicked.connect(self.write_conf_java)
        self.fontComboBox.currentTextChanged.connect(self.send_font_text)
        self.comboBox.addItems(QStyleFactory.keys())
        self.pushButton.clicked.connect(self.clear_config)

        self.cbar = QRoundProgressBar()
        colors = [
            (0., QColor.fromRgb(0,255,0)),
            (0.5, QColor.fromRgb(255,255,0)),
            (1., QColor.fromRgb(255,0,0)),
        ]
        self.cbar.setValue(0)
        self.cbar.setDataColors(colors)
        self.verticalLayout_7.addWidget(self.cbar)
        self.attsize = AttSizeBase()
        self.attsize.ftpdatasus_signal.connect(self.lcdNumber.display)
        self.attsize.opendatasus_signal.connect(self.lcdNumber_2.display)
        self.thread_ftp = Thread(self.attsize.ftpdatasus)
        self.thread_open = Thread(self.attsize.opendatasus)
        self.thread_ftp.start()
        self.thread_open.start()
        self.thread_cbar = Thread(self.update_cbar)
        self.thread_cbar.start()

    def update_cbar(self):
        self.cbar_running = True
        while self.cbar_running:
            time.sleep(2)
            count = self.lcdNumber.value() + self.lcdNumber_2.value()
            try:
                total, used, free = shutil.disk_usage(
                    os.path.expanduser('~/datasus_dbc')
                )
                self.cbar.setValue((count * 100) / (total // (2 ** 30)))
            except FileNotFoundError:
                self.cbar.setValue(count)

    def load_conf(self):
        pass
        # with open(conf + "config.json", "r", encoding="utf8") as f:
        #     data = json.load(f)
        #     self.pushButton_3.setText(data["spark"])
        #     self.pushButton_4.setText(data["java"])

    def clear_config(self):
        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["java"] = None
            json.dump(data, f, indent=4)
        self.pushButton_3.setText("...")

        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["spark"] = None
            json.dump(data, f, indent=4)
        self.pushButton_4.setText("...")

    def send_font_text(self, font):
        self.send_font.emit(font)

    def write_conf_java(self):
        java = QFileDialog.getExistingDirectory(self,
            "Selecione o caminho da aplicação Java"
        )
        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["java"] = java
            json.dump(data, f, indent=4)
        self.pushButton_4.setText(java)

    def write_conf_spark(self):
        spark = QFileDialog.getExistingDirectory(self,
            "Selecione o caminho da aplicação Hadoop Spark"
        )
        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
        with open(conf + "config.json", "w", encoding='utf8') as f:
            data["spark"] = spark
            json.dump(data, f, indent=4)
        self.pushButton_3.setText(spark)

    def closeEvent(self, event):
        self.attsize.ftp_activate = False
        self.attsize.datasus_activate = False
        self.cbar_running = False
        self.thread.terminate()


class Help(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowIcon(QIcon(dir_ico + "help.png"))


class Home(QMainWindow):
    def __init__(self):
        super().__init__()

        uic.loadUi(dir_ui + "home.ui", self)

        self.pushButton.clicked.connect(self.open_link_github)
        self.pushButton_2.clicked.connect(self.open_link_github)

    def open_link_github(self):
        webbrowser.open(self.sender().text())


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("pyBIS")
    app.setWindowIcon(QIcon(dir_ico + "favicon.ico"))
    home = Home()
    download = DownloadUi()
    etl = EtlUi()
    merge = MergeUi()
    loadfile = LoadFile()
    config = Config()
    help = Help()
    analysis = AnalysisUi()
    loadfile.column_data.connect(etl.convert_model)
    loadfile.column_data.connect(etl.line_select.addItem)
    loadfile.dataframe.connect(download.receive_data)
    download.signal_col_etl.connect(etl.convert_model)
    download.signal_col_etl.connect(etl.line_select.addItem)
    download.signal_clear_add.connect(etl.clear_models)
    download.signal_export_count.connect(etl.header_etl_count)
    download.signal_column_export.connect(etl.header_etl)
    download.signal_etl_el.connect(etl.build_table)
    loadfile.column_data.connect(analysis.update_items)
    download.signal_col_etl.connect(analysis.update_items)
    download.signal_cols_analysis.connect(analysis.update_items)
    download.signal_clear_items.connect(analysis.clear_items)
    download.signal_clear_items.connect(etl.table_export.clear)
    etl.signal_trim_data.connect(download.trim_data)
    etl.signal_save.connect(download.save_file)
    manager = Manager(home,loadfile, download, etl, merge, analysis, config, help)
    config.send_font.connect(manager.set_font)
    config.comboBox.currentTextChanged.connect(app.setStyle)
    manager.setWindowIcon(QIcon(dir_ico + "favicon.ico"))
    manager.resized.connect(manager.set_font_size)
    try:
        app.aboutToQuit.connect(lambda: analysis.terminate())
    except AttributeError:
        pass
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
