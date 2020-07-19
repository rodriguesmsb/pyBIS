# -*- coding: utf-8 -*-

from os import system, path
import pkg_resources.py2_warn
from sys import argv, exit
import sys
from threading import Thread
from time import sleep
from setSpark import start_spark, spark_conf
from app import *
from pyalert import *
from pydbsus import Datasus

import func_py

class MyForm(QtWidgets.QDialog):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)

        self.ui.gerar_csv.clicked.connect(self.start_func_csv)
        self.ui.baixar_dbc.clicked.connect(self.download)
        self.ui.start_spark.clicked.connect(self.spark)

        self.data = Datasus()


    def download(self):
        print (func_py.box_download(self.ui.progressBar, self.ui.gerar_csv,
                             self.ui.baixar_dbc, self.ui.r_sim,
                             self.ui.r_sinan,self.ui.r_sinasc))


    def spark(self):
        start_spark(spark_conf(n_cores =\
                         self.ui.set_processor.value(),
                         executor_memory = self.ui.set_memory.value()))


    def start_func_csv(self):
        func_py.button_off(self.ui.gerar_csv, self.ui.baixar_dbc)
        self.threads(func_py.get_var(self.ui.r_sim, self.ui.r_sinan,
                                     self.ui.r_sinasc))


    def start_func_dbc(self):
        global t1

        t1 = Thread(target = self.data.download, daemon = True)
        t1.start()

        Thread(target = self.loop, daemon = True).start()


    def threads(self, retorno):
        global t1

        self.data = Datasus(retorno)
        t1 = Thread(target = self.data.load_files, daemon = True)
        t1.start()
        Thread(target = self.loop, args = (retorno,),
                         daemon = True).start()

    def loop(self, retorno = None):
         func_py.button_off(self.ui.gerar_csv, self.ui.baixar_dbc)
         n = 0
         while t1.is_alive():
             sleep(0.5)
             n += 1
             if n >= 99:
                 n = 0
             self.ui.progressBar.setValue(n)
         self.ui.progressBar.setValue(100)
         if retorno == None:
             pass
         else:
             self.data.write_file(retorno)
         func_py.button_on(self.ui.gerar_csv, self.ui.baixar_dbc)


if __name__ == '__main__':
    app = QtWidgets.QApplication(argv)
    myform = MyForm()
    myform.show()
    exit(app.exec_())
