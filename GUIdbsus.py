#!/usr/bin/env python3

import sys, threading, time, setSpark, multiprocessing, check_files
from app import *
from pyalert import *
from pydbsus import Datasus


class MyForm(QtWidgets.QDialog):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)

        self.ui.gerar_csv.clicked.connect(self.start_func_csv)
        self.ui.baixar_dbc.clicked.connect(self.start_pergunta)
        self.ui.start_spark.clicked.connect(self.spark)

    def start_pergunta(self):
        self.window2 = QtWidgets.QDialog()
        self.new_ui = Ui_pergunta()
        self.new_ui.setupUi(self.window2)

        self.new_ui.do_arquivo.clicked.connect(lambda: self.confirm(1))
        self.new_ui.da_memoria.clicked.connect(lambda: self.confirm(2))

        self.window2.show()
        self.window2.exec_()

    def alert(self):

        self.new_ui.label.setText(
            "In development... create\na csv and choose 'memory'")

    def confirm(self, choice):
        if choice == 1:
            check_files.get_system()
            self.alert()
        elif choice == 2:
            try:
                self.start_func_dbc()
            except:
                self.new_ui.label.setText(
                    "Please return... create\na csv and choose 'memory'")

    def spark(self):
        setSpark.start_spark(setSpark.spark_conf(n_cores =\
                         self.ui.set_processor.value(),\
                         executor_memory = self.ui.set_memory.value()))

    def activate_button(self):
        self.ui.baixar_dbc.setEnabled(True)
        self.ui.gerar_csv.setEnabled(True)

    def deactive_button(self):
        self.ui.baixar_dbc.setEnabled(False)
        self.ui.gerar_csv.setEnabled(False)

        self.ui.r_sim.setEnabled(False)
        self.ui.r_sinan.setEnabled(False)
        self.ui.r_sinasc.setEnabled(False)

    def start_func_csv(self):
        self.deactive_button()
        self.threads(self.get_var())

    def start_func_dbc(self):
        global t1

        t1 = threading.Thread(target = data.download, daemon = True)
        t1.start()

        threading.Thread(target = self.loop, daemon = True).start()

    def get_var(self):
        if self.ui.r_sim.isChecked():
            self.ui.r_sinan.setEnabled(False)
            self.ui.r_sinasc.setEnabled(False)
            return self.ui.r_sim.text()

        elif self.ui.r_sinan.isChecked():
            self.ui.r_sim.setEnabled(False)
            self.ui.r_sinasc.setEnabled(False)
            return self.ui.r_sinan.text()

        elif self.ui.r_sinasc.isChecked():
            self.ui.r_sim.setEnabled(False)
            self.ui.r_sinan.setEnabled(False)
            return self.ui.r_sinasc.text()

    def threads(self, retorno):
        global t1, data

        data = Datasus(retorno)
        t1 = threading.Thread(target = data.load_files, daemon = True)
        t1.start()
        threading.Thread(target = self.loop, args = (retorno,), \
                         daemon = True).start()

    def loop(self, retorno = None):
        self.deactive_button()
        n = 0
        while t1.is_alive():
            time.sleep(0.5)
            n += 1
            if n >= 99:
                n = 0
            self.ui.progressBar.setValue(n)
        self.ui.progressBar.setValue(100)
        if retorno == None:
            pass
        else:
            data.write_file(retorno)
        self.activate_button()

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    myform = MyForm()
    myform.show()
    sys.exit(app.exec_())
