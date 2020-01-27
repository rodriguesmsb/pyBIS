#!/usr/bin/env python3
# -*- coding:utf8 -*-

import sys, time, threading, random
from PyQt5 import QtCore, QtGui, QtWidgets
from pydbsus import Datasus

class DatasusUi(Datasus):
    def __init__(self, banco=None, PAGINA = 'ftp.datasus.gov.br', \
            PUBLICO = '/dissemin/publicos'):
        super().__init__(self)

        self.select = 0
        self.app = QtWidgets.QApplication(sys.argv)
        self.win = QtWidgets.QMainWindow()
        self.font = QtGui.QFont()
        self.font.setPointSize(12)
        self.win.resize(470, 339)

        self.sim = QtWidgets.QRadioButton(self.win)
        self.sim.setText('SIM')
        self.sim.setFont(self.font)
        self.sim.setGeometry(QtCore.QRect(40,40, 90,50))

        self.sinan = QtWidgets.QRadioButton(self.win)
        self.sinan.setText('SINAN')
        self.sinan.setFont(self.font)
        self.sinan.setGeometry(QtCore.QRect(40,80, 90,50))

        self.sinasc = QtWidgets.QRadioButton(self.win)
        self.sinasc.setText('SINASC')
        self.sinasc.setFont(self.font)
        self.sinasc.setGeometry(QtCore.QRect(40,120, 90,50))

        self.genCsv = QtWidgets.QPushButton(self.win)
        self.genCsv.setText('Gerar .csv')
        self.genCsv.setFont(self.font)
        self.genCsv.clicked.connect(self.gencsv)
        self.genCsv.setGeometry(QtCore.QRect(290, 40, 120, 50))

        self.downCsv = QtWidgets.QPushButton(self.win)
        self.downCsv.setText('Download .csv')
        self.downCsv.clicked.connect(self.downcsv)
        self.downCsv.setFont(self.font)
        self.downCsv.setGeometry(QtCore.QRect(290, 120, 120, 50))

        self.label = QtWidgets.QLabel(self.win)
        self.label.setFont(self.font)
        self.label.setGeometry(QtCore.QRect(30,210,411,23))

        self.progressbar = QtWidgets.QProgressBar(self.win)
        self.progressbar.setGeometry(QtCore.QRect(30, 250, 411, 23))
        self.progressbar.setFont(self.font)

        self.win.show()
        sys.exit(self.app.exec_())


    def gencsv(self):
        if self.sim.isChecked() == True:
            self.mygambiarra(self.sim.text())

        elif self.sinan.isChecked() == True:
            self.mygambiarra(self.sinan.text())

        elif self.sinasc.isChecked() == True:
            self.mygambiarra(self.sinasc.text())

        else:
            print ('Maque algo')

    def downcsv(self):

        if self.select != 0:
            Datasus.write_file(self.data)
        else:
            print ('Nenhum dado gerado')

    def mygambiarra(self, data):
        self.data = data
        self.thread_1 = threading.Thread(target=Datasus(self.data).load_files)
        self.thread_1.start()
        threading.Thread(target=self.statusProgressBar).start()
        threading.Thread(target=self.textProgressBar).start()

    def statusProgressBar(self):
        n = 0
        while self.thread_1.isAlive():
            if n < 100:
                self.progressbar.setValue(n)
                n += 1
                time.sleep(0.5)
            else:
                n = 0

        self.progressbar.setValue(100)
        self.select = 1
        print ('Dados preparados')

    def textProgressBar(self):
        label = ['Carregando...','Isso pode demorar vários minutos', \
                'Tenha Paciência','Porcentagem referente aos diretórios']
        n = 0
        while self.thread_1.isAlive():
            time.sleep(1)
            self.label.setText(random.choice(label))

        self.label.setText('Ja deve estar concluido')

if __name__ == '__main__':
    app = DatasusUi()
