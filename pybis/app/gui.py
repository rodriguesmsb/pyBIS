#!/usr/bin/env python3

import sys
import time
import os
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QMessageBox,
        QPushButton, QTableWidgetItem, QTabBar, QTabWidget, QStyle,
        QStyleOptionTab, QStylePainter)
from PyQt5.QtGui import QFont, QIcon, QStandardItemModel, QStandardItem
from PyQt5.QtCore import (QThread, pyqtSignal, QObject, QMetaType, QRect,
        QPoint, pyqtSlot)
from PyQt5 import uic
import json

from pydatasus import PyDatasus
from f_spark import spark_conf, start_spark

dir_ui = os.path.join(os.path.dirname(__file__), "../layouts/")
dir_ico = os.path.join(os.path.dirname(__file__), "../assets/")
conf = os.path.join(os.path.dirname(__file__), "../conf/")


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

class Update(QObject):

    signal = pyqtSignal(int)
    signal_clear_add = pyqtSignal(int)
    signal_finished = pyqtSignal(int)
    signal_txt = pyqtSignal(str)
    signal_col_etl = pyqtSignal(str)
    signal_column = pyqtSignal(list)
    signal_column_export = pyqtSignal(list)
    signal_trim_data = pyqtSignal(list)
    signal_export_count = pyqtSignal(int)
    signal_etl_el = pyqtSignal(list)

    def __init__(self):
        super().__init__()


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

    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "download.ui", self)
        self.reset()

        self.signal = Update()
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
        self.spinBox_2.valueChanged.connect(self.cpu)

        self.all_buttons = self.findChildren(QPushButton)

    def reset(self):
        with open(conf + "search.json", "r") as f:
            data = json.load(f)
        with open(conf + "search.json", "w") as f:
            data["database"] = ""
            data["base"] = ""
            data["limit"] = ""
            data["date_range"] = ["2010"]
            json.dump(data, f, indent=4)

        with open(conf + "config.json", "r") as f:
            data = json.load(f)
        with open(conf + "config.json", "w") as f:
            data["mem"] = "2"
            data["cpu"] = "2"
            json.dump(data, f, indent=4)

    def mem(self, val):
        with open(conf + "config.json", "r") as f:
            data = json.load(f)
        with open(conf + "config.json", "w") as f:
            data["mem"] = val
            json.dump(data, f, indent=4)

    def cpu(self, val):
        with open(conf + "config.json", "r") as f:
            data = json.load(f)
        with open(conf + "config.json", "w") as f:
            data["cpu"] = val
            json.dump(data, f, indent=4)

    def read_json_database(self, choice: str) -> list:
        values = []
        self.comboBox_2.setEnabled(True)
        with open(conf + "database.json", "r") as f:
            database = json.load(f)
            database = database["database"]
            for base in database:
                for val in base.get(choice):
                    values.append(list(val.values())[0])
            return sorted(values)

    def return_code_base(self, choice: str) -> str:
            if self.comboBox.currentText().lower() == "sihsus":
                self.write_base("RD")
            elif (self.comboBox.currentText().lower() 
                    != "selecionar sistema de dados"):
                with open(conf + "database.json", "r") as f:
                    database_json = json.load(f)
                    database_json = database_json["database"]
                    for base in database_json:
                        for val in base.get(self.comboBox.currentText()):
                            if choice == list(val.values())[0]:
                                self.write_base(list(val.values())[1])
            else:
                self.write_base("")

    def return_uf(self, limit: str)-> str:
        ufs = set()
        with open(conf + "locales.json", "r") as f:
            stts_json = json.load(f)
            stts_json = stts_json["estados"]
            for stt_keys in stts_json:
                if limit == stt_keys.get(limit):
                    ufs.add(stt_keys.get("UF"))
                elif limit == stt_keys.get(list(stt_keys.keys())[1]):
                    ufs.add(stt_keys.get("UF"))
                elif limit == stt_keys.get(list(stt_keys.keys())[3]):
                    ufs.add(stt_keys.get("UF"))

        with open(conf + "search.json", "r") as f:
            data = json.load(f)
        with open(conf + "search.json", "w") as f:
            data["limit"] = list(ufs)
            json.dump(data, f, indent=4)

    def load_json_locales(self, choice: str) -> list:
        stts = []
        region = set()
        self.comboBox_4.setEnabled(True)
        with open(conf + "locales.json", "r") as f:
            locales = json.load(f)
            locales = locales["estados"]
            if choice == "ESTADO":
                for limit in locales:
                    stts.append(limit.get(choice))
                    stts.sort()
                return stts
            elif choice == "REGIÃO":
                for limit in locales:
                    region.add(limit.get(choice))
                return sorted(list(region))
            else:
                return ["BRASIL"]

    def write_database(self, database: str):
        if database.lower() == "selecionar sistema de dados":
            with open(conf + "search.json", "r") as f:
                data = json.load(f)
            with open(conf + "search.json", "w") as f:
                    data["database"] = ""
                    json.dump(data, f, indent=4)
        else:
            with open(conf + "search.json", "r") as f:
                data = json.load(f)
            with open(conf + "search.json", "w") as f:
                    data["database"] = database
                    json.dump(data, f, indent=4)


    def write_base(self, base: str):
        with open(conf + "search.json", "r") as f:
            data = json.load(f)
        with open(conf + "search.json", "w") as f:
            data["base"] = base
            json.dump(data, f, indent=4)

    def write_date(self, date: list):
        with open(conf + "search.json", "r") as f:
            data = json.load(f)
        with open(conf + "search.json", "w") as f:
            data["date_range"] = date
            json.dump(data, f, indent=4)

    def return_list_date(self, date: int, date_: int) -> list:
        if date < date_:
            self.write_date([str(dt) for dt in range(date, date_ + 1)])
        elif date > date_:
            self.write_date([str(dt) for dt in range(date_, date + 1)])
        else:
            self.write_date([str(date)])

    def load_database(self, database: str):
        self.comboBox_2.clear()
        if (database.lower() != "selecionar sistema de dados"
                and database.lower() != "sihsus"):
            self.comboBox_2.addItems(self.read_json_database(database))
        elif database.lower() == "sihsus":
            self.comboBox_2.addItems(self.read_json_database("SIH"))
        else:
            self.comboBox_2.setEnabled(False)

    def load_limit(self, limit: str):
        self.comboBox_4.clear()
        if limit.lower() != "selecionar local":
            self.comboBox_4.addItems(self.load_json_locales(limit))
        else:
            self.comboBox_4.setEnabled(False)

    def return_limit_list(self, limit: str) -> list:
        pass

    def return_date(self, date: int) -> list:
        self.return_list_date(date, self.horizontalSlider_2.value())

    def return_date_(self, date_: int) -> list:
        self.return_list_date(date_, self.horizontalSlider.value())

    def load_conf(self):
        with open(conf + "search.json") as f:
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
            # QApplication.processEvents()

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
        with open(conf + "config.json", "r") as f:
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
        self.signal.signal_finished.connect(self.finished)

        self.tableWidget.clear()
        if self.df.columns[0] == "_c0":
            self.cols = [col for col in self.df.columns[1:]]
        else:
            self.cols = [col for col in self.df.columns]

        self.cols.sort()
        self.tableWidget.setColumnCount(len(self.cols))
        self.signal.signal_clear_add.emit(1)
        for i, col in enumerate(self.cols):
            self.signal.signal_col_etl.emit(col)
            column = QTableWidgetItem(col)
            self.signal.signal_column.connect(self.ant_bug_column)
            self.signal.signal_column.emit([i, column])
            i += 1

    def write_body(self):
        self.tableWidget.setRowCount(20)
        col_n = 0
        row_n = 0
        val = 0

        self.percentage = 0
        self.signal.signal_txt.connect(self.label_5.setText)
        self.signal.signal_txt.emit("Escrevendo seus dados")
        try:
            for col in self.cols:
                val += 1
                for r in range(1, 21):
                    ratio = round((float(val / len(self.cols)) * 100 - 6), 1)
                    percentage = int(round(100 * ratio / (100 - 6), 1))
                    self.signal.signal.connect(self.progressBar.setValue)
                    self.signal.signal.emit(percentage)
                    self.tableWidget.setItem(
                        row_n, col_n, QTableWidgetItem(
                            str(self.df.select(
                                self.df[col]).take(r)[r - 1][0])))
                    row_n += 1
                row_n = 0
                col_n += 1
            self.signal.signal_txt.emit("Os dados foram escritos com sucesso")
            self.signal.signal_finished.emit(1)
        except IndexError:
            self.signal.signal_txt.emit("Não há nada aqui")
            self.signal.signal_finished.emit(1)

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

        self.signal.signal_export_count.emit(len(self.data_filtered.columns))

        for i, col in enumerate(self.data_filtered.columns):
            column = QTableWidgetItem(col)
            self.signal.signal_column_export.emit([i, column])
            i += 1

        col_n = 0
        row_n = 0

        for col in self.data_filtered.columns:
            for r in range(1, 21):
                self.signal.signal_etl_el.emit(
                    [row_n, col_n, QTableWidgetItem(str(
                        self.data_filtered.select(
                    self.data_filtered[col]).take(r)[r - 1][0]))])
                row_n += 1
            row_n = 0
            col_n += 1


class EtlUi(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(dir_ui + "etl.ui", self)

        self.signal = Update()

        self.model_col_add = QStandardItemModel()
        self.model_col_apply = QStandardItemModel()
        self.model_col_ext = QStandardItemModel()

        self.column_add.setModel(self.model_col_add)
        self.column_apply.setModel(self.model_col_apply)
        self.column_ext.setModel(self.model_col_ext)

        self.button_add.clicked.connect(self.add_col_apply)
        self.button_rm.clicked.connect(self.remove_col_apply)

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
           for n, idx in enumerate(range(self.model_col_apply.rowCount())):
               self.drop_list.append(self.model_col_apply.item(idx).text())
           self.table_export.setRowCount(20)

        if len(range(self.model_col_ext.rowCount())):
           self.filter_list = []
           for idx in range(self.model_col_ext.rowCount()):
               self.filter_list.append(self.model_col_ext.item(idx).text())

        self.signal.signal_trim_data.emit([self.drop_list,
                                            self.filter_list])


    def header_etl(self, col):
        self.table_export.setHorizontalHeaderItem(col[0], col[1])

    def header_etl_count(self, n):
        self.table_export.setColumnCount(n)

    def build_table(self, params):
        self.table_export.setItem(params[0], params[1], params[2])


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("pyBIS")
    download = DownloadUi()
    etl = EtlUi()
    download.signal.signal_col_etl.connect(etl.convert_model)
    download.signal.signal_col_etl.connect(etl.line_select.addItem)
    download.signal.signal_clear_add.connect(etl.clear_models)
    download.signal.signal_export_count.connect(etl.header_etl_count)
    download.signal.signal_column_export.connect(etl.header_etl)
    download.signal.signal_etl_el.connect(etl.build_table)
    etl.signal.signal_trim_data.connect(download.trim_data)
    manager = Manager(download, etl)
    manager.setWindowIcon(QIcon(dir_ico + "favicon.ico"))
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
