#!/usr/bin/env python3

import sys
import time
import subprocess
import os
import multiprocessing
import psutil
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QMessageBox,
        QPushButton, QTableWidgetItem, QTabBar, QTabWidget, QStyle,
        QStyleOptionTab, QStylePainter, QFileDialog, QHeaderView)
from PyQt5.QtGui import QFont, QIcon, QStandardItemModel, QStandardItem
from PyQt5.QtCore import (QThread, pyqtSignal, QObject, QRect, QPoint,
        pyqtSlot, Qt)
from PyQt5 import uic
import json
import pandas as pd
import webbrowser

from pydatasus import PyDatasus
from f_spark import spark_conf, start_spark

sys.path.append(os.path.join(os.path.dirname(__file__),
                "../scripts/SpatialSUSapp/"))

dir_ui = os.path.join(os.path.dirname(__file__), "../layouts/")
dir_ico = os.path.join(os.path.dirname(__file__), "../assets/")
conf = os.path.join(os.path.dirname(__file__), "../conf/")
dir_dbc = os.path.expanduser("~/datasus_dbc/")
dir_sus_conf = os.path.join(os.path.dirname(__file__),
                            "../scripts/SpatialSUSapp/conf/")


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


class Manager(QMainWindow):
    def __init__(self, *tabs):
        super().__init__()

        self.setWindowTitle("pyBiss")
        self.setFont(QFont("Arial", 12))
        self.tab_manager = TabWidget()

        for tab in tabs:
            self.tab_manager.addTab(tab, QIcon(tab.windowIcon()), '')

        self.setCentralWidget(self.tab_manager)
        self.show()


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

    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "download.ui", self)
        self.reset()

        self.comboBox.currentTextChanged.connect(self.load_database)
        self.comboBox.currentTextChanged.connect(self.write_database)
        self.comboBox_2.currentTextChanged.connect(self.return_code_base)
        self.comboBox_3.currentTextChanged.connect(self.load_limit)
        self.comboBox_4.currentTextChanged.connect(self.return_uf)
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

    def get_size(self):
        swap = psutil.swap_memory()
        mem = str(swap.total // 1024)
        mem = mem[0:2]
        return int(mem)

    def reset(self):
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
                if limit != 'Brasil':
                    ufs.add(stts_json[limit])
                else:
                    ufs.add(stts_json['Norte'])
                    ufs.add(stts_json['Nordeste'])
                    ufs.add(stts_json['Centro-Oeste'])
                    ufs.add(stts_json['Sudeste'])
                    ufs.add(stts_json['Sul'])

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
        else:
            with open(conf + "search.json", "r", encoding='utf8') as f:
                data = json.load(f)
            with open(conf + "search.json", "w", encoding='utf8') as f:
                    data["database"] = database
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
                data = data[database]
                for base in data:
                    bases.append(base)
                bases.sort()
                self.comboBox_2.addItems(bases)

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

        def load_region():
            with open(conf + 'locales.json', 'r', encoding='utf8') as f:
                self.comboBox_4.setEnabled(True)
                states = json.load(f)
                states_list = []
                for keys in states.keys():
                    if keys in ['Norte', 'Nordeste', 
                            'Sudeste', 'Sul', 'Centro-Oeste']:
                        states_list.append(keys)
                        states_list.sort()
                self.comboBox_4.clear()
                self.comboBox_4.addItems(states_list)

        if limit.lower() != "selecionar local":
            if limit.lower() == 'estado':
                load_states()
            elif limit.lower() == 'região':
                load_region()
            elif limit.lower() == 'brasil':
                self.comboBox_4.clear()
                self.comboBox_4.addItem('Brasil')
                self.comboBox_4.setEnabled(False)
        else:
            self.comboBox_4.clear()
            self.comboBox_4.setEnabled(False)

    def return_date(self, date: int) -> list:
        self.return_list_date(date, self.horizontalSlider_2.value())

    def return_date_(self, date_: int) -> list:
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
        if not all(self.load_conf()):
            error = QMessageBox()
            error.setFont(QFont("Arial", 15))
            error.setIcon(QMessageBox.Warning)
            error.setText("Parametros incorretos")
            error.setInformativeText(
                "Você precisa escolher todos os parametros de busca"
            )
            error.setWindowTitle("Erro!")
            error.exec_()
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
            error = QMessageBox()
            error.setFont(QFont("Arial", 15))
            error.setIcon(QMessageBox.Warning)
            error.setText("Parametros incorretos")
            error.setInformativeText(
                "Você precisa escolher todos os parametros de busca"
            )
            error.setWindowTitle("Erro!")
            error.exec_()
        else:
            self.database, self.base, self.limit, self.date = self.load_conf()

            if self.database == 'SINAN':
                if isinstance(self.date, list):
                    self.date = [x[2:4] for x in self.date]
                elif isinstance(self.date, str):
                    self.date = self.date[2:4]
            elif self.database == 'SIH':
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
            dir_database = os.path.expanduser("~/datasus_dbc/"
                                          + self.database
                                          + "/")
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
                error = QMessageBox()
                error.setFont(QFont("Arial", 15))
                error.setIcon(QMessageBox.Critical)
                error.setText("A pasta não foi encontrada!")
                error.setInformativeText("A pasta {}".format(self.database))
                error.setWindowTitle("Erro!")
                error.exec_()

    def read_file(self):
        with open(conf + "config.json", "r", encoding='utf8') as f:
            data = json.load(f)
            self.conf = spark_conf("pyBIS", data["cpu"], data["mem"],
                driver_memory=20
            )
            self.spark = start_spark(conf)
            self.df = self.spark.read.csv(self.files, header=True)
            self.write_table()

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
        col_n = 0
        row_n = 0
        val = 0

        self.percentage = 0
        self.signal_txt.connect(self.label_5.setText)
        self.signal_txt.emit("Escrevendo seus dados")
        try:
            for col in self.cols:
                val += 1
                for r in range(1, 21):
                    ratio = round((float(val / len(self.cols)) * 100 - 6), 1)
                    percentage = int(round(100 * ratio / (100 - 6), 1))
                    self.signal_pbar.connect(self.progressBar.setValue)
                    self.signal_pbar.emit(percentage)
                    self.tableWidget.setItem(
                        row_n, col_n, QTableWidgetItem(
                            str(self.df.select(
                                self.df[col]).take(r)[r - 1][0])))
                    row_n += 1
                row_n = 0
                col_n += 1
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
            self.data_drop = self.df.drop(*drop_list)

            if params[1] != None:
                try:
                    self.data_filtered = self.data_drop.filter(' and '.join(
                                                               params[1])
                    )
                except:
                    pass
            elif params[1] == None:
                self.data_filtered = self.data_drop

        elif params[0] == None:
            self.data_drop = self.df
            if params[1] != None:
                try:
                    self.data_filtered = self.data_drop.filter(' and '.join(
                                                               params[1])
                    )
                except:
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

        for col in self.data_filtered.columns:
            for r in range(1, 21):
                self.signal_etl_el.emit(
                    [row_n, col_n, QTableWidgetItem(str(
                        self.data_filtered.select(
                    self.data_filtered[col]).take(r)[r - 1][0]))])
                row_n += 1
            row_n = 0
            col_n += 1

    def save_file(self, params):
        self.data_filtered.coalesce(1).write.option(params[0],
                                                    params[1]).csv(
                                                        params[2]
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

        self.signal_trim_data.emit([self.drop_list,
                                            self.filter_list])

        try:
            os.system("rm -r {}".format(
                os.path.join(os.path.dirname(__file__),
                "../scripts/SpatialSUSapp/data/"))
            )
        except:
            pass

        self.signal_save.emit(
            ["header", "true", os.path.join(os.path.dirname(__file__),
             "../scripts/SpatialSUSapp/data/")
             ]
        )

        try:
            os.system("mv {} {}".format(
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
        self.servidor = subprocess.Popen(
            ["python3", os.path.join(os.path.dirname(__file__),
            "../scripts/SpatialSUSapp/index.py")
            ]
        )
        self.nav = Thread(webbrowser.open, "127.0.0.1:8050")
        self.nav.start()

    def terminate(self):
        try:
            self.servidor.kill()
            self.nav.stop()
        except AttributeError:
            print("O servidor não está em execução!")

    def clear_items(self, val):
        self.comboBox_4.clear()
        self.comboBox.clear()
        self.comboBox_2.clear()

    def update_items(self, cols):
        self.comboBox_4.addItem(cols)
        self.comboBox.addItem(cols)
        self.comboBox_2.addItem(cols)


class Help(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowIcon(QIcon(dir_ico + "help.png"))


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("pyBIS")
    app.setWindowIcon(QIcon(dir_ico + "favicon.ico"))
    download = DownloadUi()
    etl = EtlUi()
    merge = MergeUi()
    help = Help()
    analysis = AnalysisUi()
    download.signal_col_etl.connect(etl.convert_model)
    download.signal_col_etl.connect(etl.line_select.addItem)
    download.signal_clear_add.connect(etl.clear_models)
    download.signal_export_count.connect(etl.header_etl_count)
    download.signal_column_export.connect(etl.header_etl)
    download.signal_etl_el.connect(etl.build_table)
    download.signal_col_etl.connect(analysis.update_items)
    download.signal_cols_analysis.connect(analysis.update_items)
    download.signal_clear_items.connect(analysis.clear_items)
    etl.signal_trim_data.connect(download.trim_data)
    etl.signal_save.connect(download.save_file)
    manager = Manager(download, etl, merge, analysis, help)
    manager.setWindowIcon(QIcon(dir_ico + "favicon.ico"))
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
