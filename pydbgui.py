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

        self.dados = ''
        self.app = QtWidgets.QApplication(sys.argv)
        self.win = QtWidgets.QMainWindow()
        self.font = QtGui.QFont()
        self.font.setPointSize(12)
        self.fontLabel = QtGui.QFont()
        self.fontLabel.setPointSize(11)
        self.win.setFixedSize(470, 339)
        self.win.setWindowTitle('pydbGUI')

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
        self.downCsv.clicked.connect(self.downDb)
        self.downCsv.setFont(self.font)
        self.downCsv.setGeometry(QtCore.QRect(290, 105, 120, 50))

        self.label = QtWidgets.QLabel(self.win)
        self.label.setFont(self.fontLabel)
        self.label.setGeometry(QtCore.QRect(30,230,411,23))

        self.progressbar = QtWidgets.QProgressBar(self.win)
        self.progressbar.setGeometry(QtCore.QRect(30, 270, 411, 23))
        self.progressbar.setFont(self.font)

        self.win.show()
        self.app.exec_()


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
        self.dados = Datasus(f'{name}')
        
        self.thread_1 = threading.Thread(target = \
                self.dados.load_files)

        self.thread_1.start()

        self.ilusion(name)


    def ilusion(self, name):
        processos = [self.statusProgressBar,\
                self.textProgressBar]

        for i in processos:
            threading.Thread(target=i, daemon=True).start()

        while self.thread_1.is_alive() == True:
            time.sleep(0.5)

            if self.thread_1.is_alive() == False:
                self.dados.write_file(name)

                break
            else:
                pass

    def downDb(self):
        try:
            self.dados.download()
        except AttributeError:
            print ('Precisa selecionar e gerar algo')

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
        emoji = ['\\0/\\0/\\0/\\0/','/0\\/0\\/0\\/0\\']
        n = 0
        while self.thread_1.is_alive() == True:
            time.sleep(2)
            try:
                self.label.setText(self.dados.log['Nome'][n]+'\t'+\
                        self.dados.log['Endereco'][n])
            except:
                self.label.setText('Isso pode demorar muito ')
            n += 1

        x = 0
        while x in range(60):
            if x == 2:
                x = 0
            time.sleep(1)
            self.label.setText(f'Seus dados estão prontos \
                    {emoji[x]}')
            x += 1


if __name__ == '__main__':
    app = DatasusUi()
