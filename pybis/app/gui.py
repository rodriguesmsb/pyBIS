#!/usr/bin/env python3

import sys
import time
import os
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QMessageBox,
        QPushButton, QTableWidgetItem)
from PyQt5.QtGui import QFont
from PyQt5.QtCore import QThread, pyqtSlot, pyqtSignal, QObject, QMetaType
from PyQt5 import uic
import json

from pydatasus import PyDatasus
from f_spark import spark_conf, start_spark

downloadui = os.path.join(os.path.dirname(__file__), "../layouts/download.ui")
conf = os.path.join(os.path.dirname(__file__), "../conf/")


class Update(QObject):

    signal = pyqtSignal(int)
    signal_txt = pyqtSignal(str)
    signal_finished = pyqtSignal(int)
    signal_column = pyqtSignal(QMetaType)

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


class DownloadUi:

    def __init__(self):
        super().__init__()
        self.threadid = QThread.currentThread()
        self.reset()
        self.ui = uic.loadUi(downloadui)
        self.ui.show()

        self.ui.comboBox.currentTextChanged.connect(self.load_database)
        self.ui.comboBox.currentTextChanged.connect(self.write_database)
        self.ui.comboBox_2.currentTextChanged.connect(self.return_code_base)
        self.ui.comboBox_3.currentTextChanged.connect(self.load_limit)
        self.ui.comboBox_4.currentTextChanged.connect(self.return_uf)
        self.ui.pushButton.clicked.connect(self.process_download)
        self.ui.pushButton_2.clicked.connect(self.load_data_table)
        self.ui.pushButton_3.clicked.connect(self.stop_thread)
        self.ui.horizontalSlider.valueChanged.connect(self.return_date)
        self.ui.horizontalSlider_2.valueChanged.connect(self.return_date_)
        self.ui.spinBox.valueChanged.connect(self.mem)
        self.ui.spinBox_2.valueChanged.connect(self.cpu)

        self.ui.all_buttons = self.ui.findChildren(QPushButton)

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
        self.ui.comboBox_2.setEnabled(True)
        with open(conf + "database.json", "r") as f:
            database = json.load(f)
            database = database["database"]
            for base in database:
                for val in base.get(choice):
                    values.append(list(val.values())[0])
            return sorted(values)

    def return_code_base(self, choice: str) -> str:
            if self.ui.comboBox.currentText().lower() == "sihsus":
                self.write_base("RD")
            elif (self.ui.comboBox.currentText().lower() 
                    != "selecionar sistema de dados"):
                with open(conf + "database.json", "r") as f:
                    database_json = json.load(f)
                    database_json = database_json["database"]
                    for base in database_json:
                        for val in base.get(self.ui.comboBox.currentText()):
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
        self.ui.comboBox_4.setEnabled(True)
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
        self.ui.comboBox_2.clear()
        if (database.lower() != "selecionar sistema de dados"
                and database.lower() != "sihsus"):
            self.ui.comboBox_2.addItems(self.read_json_database(database))
        elif database.lower() == "sihsus":
            self.ui.comboBox_2.addItems(self.read_json_database("SIH"))
        else:
            self.ui.comboBox_2.setEnabled(False)

    def load_limit(self, limit: str):
        self.ui.comboBox_4.clear()
        if limit.lower() != "selecionar local":
            self.ui.comboBox_4.addItems(self.load_json_locales(limit))
        else:
            self.ui.comboBox_4.setEnabled(False)

    def return_limit_list(self, limit: str) -> list:
        pass

    def return_date(self, date: int) -> list:
        self.return_list_date(date, self.ui.horizontalSlider_2.value())

    def return_date_(self, date_: int) -> list:
        self.return_list_date(date_, self.ui.horizontalSlider.value())

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
            pydatasus.download_signal.connect(self.ui.progressBar.setValue)
            pydatasus.label_signal.connect(self.ui.label_5.setText)
            pydatasus.lcd_signal.connect(self.ui.lcdNumber_3.display)
            pydatasus.finished.connect(self.finished)
            [btn.setEnabled(False)
                    for btn in self.ui.all_buttons
                    if btn.text().lower() != "encerrar"
            ]
            self.thread_download = Thread(pydatasus.get_data,
                                          *self.load_conf()
            )
            self.thread_download.start()
            # QApplication.processEvents()

    def finished(self, val):
        [btn.setEnabled(True) for btn in self.ui.all_buttons]

    def stop_thread(self):
        [btn.setEnabled(True) for btn in self.ui.all_buttons]
        self.ui.progressBar.setValue(0)
        self.ui.tableWidget.clear()
        try:
            self.ui.lcdNumer_3.display(0)
            self.ui.label_5.setText("")
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

    # def transform_date(self):
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
                        for btn in self.ui.all_buttons
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

    def write_header(self):
        self.signal = Update()
        self.signal.signal_finished.connect(self.finished)

        self.ui.tableWidget.clear()
        if self.df.columns[0] == "_c0":
            self.cols = [col for col in self.df.columns[1:]]
        else:
            self.cols = [col for col in self.df.columns]

        self.cols.sort()
        self.ui.tableWidget.setColumnCount(len(self.cols))
        for i, col in enumerate(self.cols):
            column = QTableWidgetItem(col)
            self.signal.signal_column.connect(
                self.ui.tableWidget.setHorizontalHeaderItem
            )
            self.ui.tableWidget.setHorizontalHeaderItem(i, column)
            i += 1

    def write_body(self):
        self.ui.tableWidget.setRowCount(20)
        col_n = 0
        row_n = 0

        val = 0
        self.percentage = 0
        self.signal.signal_txt.connect(self.ui.label_5.setText)
        self.signal.signal_txt.emit("Escrevendo seus dados")
        for col in self.cols:
            val += 1
            for r in range(1, 21):
                ratio = round((float(val / len(self.cols)) * 100 - 6), 1)
                percentage = int(round(100 * ratio / (100 - 6), 1))
                self.signal.signal.connect(self.ui.progressBar.setValue)
                self.signal.signal.emit(percentage)

                self.ui.tableWidget.setItem(
                    row_n, col_n, QTableWidgetItem(
                        str(self.df.select(self.df[col]).take(r)[r - 1][0])))
                row_n += 1
            row_n = 0
            col_n += 1
        self.signal.signal_txt.emit("Os dados foram escritos com sucesso")
        self.signal.signal_finished.emit(1)


    def write_table(self):
        self.write_header()
        self.write_body()


def main():
    app = QApplication(sys.argv)
    download = DownloadUi()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
