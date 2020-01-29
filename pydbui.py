#!/usr/bin/env python3
# -*- coding:utf8 -*-

import sys, time, threading, random, platform, os
from PyQt5 import QtCore, QtGui, QtWidgets
from pydbsus import Datasus
import pandas as pd

class DatasusUi(Datasus):
    def __init__(self, banco=None, PAGINA = 'ftp.datasus.gov.br', \
            PUBLICO = '/dissemin/publicos'):
        super().__init__(self)

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
      # self.downCsv.clicked.connect(self.downcsv)
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
            self.startThreads(self.sim.text())

        elif self.sinan.isChecked() == True:
            self.startThreads(self.sinan.text())

        elif self.sinasc.isChecked() == True:
            self.startThreads(self.sinasc.text())

        else:
            print ('Maque algo')

    def startThreads(self, name):
        dado_bruto = Datasus(banco=f'{name}')
        
        self.thread_1 = threading.Thread(target = \
                dado_bruto.load_files)

        self.thread_1.start()

        self.threads = [self.statusProgressBar,\
                self.textProgressBar]

        for i in self.threads:
            threading.Thread(target=i).start()

        while self.thread_1.is_alive() == True:
            time.sleep(0.5)

            if self.thread_1.is_alive() == False:
                self.file_csv = pd.DataFrame.from_dict(dado_bruto.log)

                if platform.system().lower() == 'linux':
                    self.sysLinux(f'{name}')

                elif platform.system().lower() == 'windows':
                    self.sysWindows(f'{name}')

                break
            else:
                pass

    def sysLinux(self, name):
        diretorio = os.path.expanduser('~/Documentos/')
        try:
            os.mkdir(diretorio + 'files_csv/')
            self.file_csv.to_csv(f'{diretorio}/files_csv/'\
                    + name.lower() \
                     + '_linux' + '.csv', index=False)

        except FileExistsError:
            self.file_csv.to_csv(f'{diretorio}/files_csv/' \
                    + name.lower() \
                     + '_linux' + '.csv', index=False)

    def sysWindows(self, name):
        diretorio = os.path.expanduser('~\\Meus Documentos\\')
        try:
            os.mkdir(diretorio + 'files_csv\\')
            self.file_csv.to_csv(f'{diretorio}\\files_csv\\'\
                    + name.lower() \
                    + '_windows' + '.csv', index=False)
        except FileExistsError:
            pass
            self.file_csv.to_csv(f'{diretorio}\\files_csv\\' \
                    + name.lower() \
                    + '_windows' + '.csv', index=False)

    def statusProgressBar(self):
        n = 0
        while self.thread_1.is_alive() == True:
            if n < 100:
                self.progressbar.setValue(n)
                n += 1
                time.sleep(0.5)
            else:
                n = 0

        self.progressbar.setValue(100)
        print ('Dados preparados')

    def textProgressBar(self):
        label = ['Carregando...','Isso pode demorar vários minutos', \
                'Tenha Paciência','Porcentagem referente aos diretórios']

        n = 0
        while self.thread_1.is_alive() == True:
            time.sleep(1.5)
            self.label.setText(random.choice(label))

        self.label.setText('Ja deve estar concluido')

if __name__ == '__main__':
    app = DatasusUi()
