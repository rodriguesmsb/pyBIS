#!/usr/bin/env python3

import sys, time, threading, random
from PyQt5 import QtCore, QtWidgets, QtGui
from pydbsus import Datasus

class AttBar(QtCore.QThread):
    val = QtCore.pyqtSignal(int)

    def run(self):
        c = 0
        while c < 100:
            time.sleep(0.2)
            c += 1
            self.val.emit(c)
            if c == 100:
                c = 0

class SIM_csv(QtCore.QThread):
    texto = QtCore.pyqtSignal(str)
    val = QtCore.pyqtSignal(int)
    data = Datasus('sim')

    def run(self):
        self.val.emit(0)
        self.texto.emit('Escrevendo dados do SIM')
        self.data.load_files()
        self.data.write_file('sim')
        self.texto.emit('Pronto para começar o download')
        self.val.emit(100)

class SIM_db(QtCore.QThread):
    texto = QtCore.pyqtSignal(str)
    val = QtCore.pyqtSignal(int)

    def run(self):
        self.val.emit(0)
        self.texto.emit('Baixando dados do SIM')
        SIM_csv().data.download()
        self.texto.emit('Download concluido')
        self.val.emit(100)

class SINAN_csv(QtCore.QThread):
    texto = QtCore.pyqtSignal(str)
    val = QtCore.pyqtSignal(int)
    data = Datasus('sinan')

    def run(self):
        self.val.emit(0)
        self.texto.emit('Escrevendo dados do SINAN')
        self.data.load_files()
        self.data.write_file('sim')
        self.texto.emit('Pronto para começar o download')
        self.val.emit(100)

class SINAN_db(QtCore.QThread):
    texto = QtCore.pyqtSignal(str)
    val = QtCore.pyqtSignal(int)

    def run(self):
        self.val.emit(0)
        self.texto.emit('Baixando dados do SINAN')
        SIM_csv().data.download()
        self.texto.emit('Download concluido')
        self.val.emit(100)

class SINASC_csv(QtCore.QThread):
    texto = QtCore.pyqtSignal(str)
    val = QtCore.pyqtSignal(int)
    data = Datasus('sinasc')

    def run(self):
        self.val.emit(0)
        self.texto.emit('Escrevendo dados do SINASC')
        self.data.load_files()
        self.data.write_file('sim')
        self.texto.emit('Pronto para começar o download')
        self.val.emit(100)

class SINAN_db(QtCore.QThread):
    texto = QtCore.pyqtSignal(str)
    val = QtCore.pyqtSignal(int)

    def run(self):
        self.val.emit(0)
        self.texto.emit('Baixando dados do SINASC')
        SIM_csv().data.download()
        self.texto.emit('Download concluido')
        self.val.emit(100)


class Hc(QtWidgets.QWidget):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self,parent) 

        self.setGeometry(300,300,600,400)
        self.setFixedSize(600,400)
        self.setWindowTitle('Ancião III')

        self.font = QtGui.QFont()
        self.font.setPointSize(12)

        self.sim = QtWidgets.QRadioButton(self)
        self.sim.setText('SIM')
        self.sim.setFont(self.font)
        self.sim.setGeometry(60,70,100,60)

        self.sinan = QtWidgets.QRadioButton(self)
        self.sinan.setText('SINAN')
        self.sinan.setFont(self.font)
        self.sinan.setGeometry(60,120,100,60)

        self.sinasc = QtWidgets.QRadioButton(self)
        self.sinasc.setText('SINASC')
        self.sinasc.setFont(self.font)
        self.sinasc.setGeometry(60,170,100,60)

        self.botao = QtWidgets.QPushButton(self)
        self.botao.setText('Gerar csv')
        self.botao.setFont(self.font)
        self.botao.clicked.connect(self.genCSV)
        self.botao.setGeometry(QtCore.QRect(440,90,120,40))

        self.botao1 = QtWidgets.QPushButton(self)
        self.botao1.setText('Download db*')
        self.botao1.setFont(self.font)
        self.botao1.clicked.connect(self.downDB)
        self.botao1.setGeometry(QtCore.QRect(440,160,120,40))

        self.label = QtWidgets.QLabel(self)
        self.label.setText('')
        self.label.setFont(self.font)
        self.label.setGeometry(40,250,500,40)
        
        self.barra = QtWidgets.QProgressBar(self)
        self.barra.setFont(self.font)
        self.barra.setProperty('value', 0)
        self.barra.setGeometry(40,300,500,30)


    def genCSV(self):
        if self.sim.isChecked() == True:
            self.sim_csv()

        elif self.sinan.isChecked() == True:
            self.sinan_csv()

        elif self.sinasc.isChecked() == True:
            self.sinasc_csv()

    def sim_csv(self):

        self.thread = SIM_csv()
        self.thread.texto.connect(self.setLabel)
        self.thread.val.connect(self.startProgressBar)
        self.thread.start()

        self.thread_bar = AttBar()
        self.thread_bar.val.connect(self.setProgressBar)

    def sim_db(self):
        self.thread = SIM_db()
        self.thread.texto.connect(self.setLabel)
        self.thread.val.connect(self.startProgressBar)
        self.thread.start()

        self.thread_bar = AttBar()
        self.thread_bar.val.connect(self.setProgressBar)

    def sinan_csv(self):
        self.thread = SINAN_csv()
        self.thread.texto.connect(self.setLabel)
        self.thread.val.connect(self.startProgressBar)
        self.thread.start()

        self.thread_bar = AttBar()
        self.thread_bar.val.connect(self.setProgressBar)

    def sinan_db(self):
        self.thread = SINAN_db()
        self.thread.texto.connect(self.setLabel)
        self.thread.val.connect(self.startProgressBar)
        self.thread.start()

        self.thread_bar = AttBar()
        self.thread_bar.val.connect(self.setProgressBar)

    def sinasc_csv(self):
        self.thread = SINASC_csv()
        self.thread.texto.connect(self.setLabel)
        self.thread.val.connect(self.startProgressBar)
        self.thread.start()

        self.thread_bar = AttBar()
        self.thread_bar.val.connect(self.setProgressBar)

    def sinasc_db(self):
        self.thread = SINASC_db()
        self.thread.texto.connect(self.setLabel)
        self.thread.val.connect(self.startProgressBar)
        self.thread.start()

        self.thread_bar = AttBar()
        self.thread_bar.val.connect(self.setProgressBar)


    def downDB(self):
        if self.sim.isChecked() == True:
            self.sim_db()

        elif self.sinan.isChecked() == True:
            self.sinan_db()

        elif self.sinasc.isChecked() == True:
            self.sinasc_db()
        
    def startProgressBar(self, sinal):
        if sinal == 0:
            self.thread_bar.start()
        if sinal == 100:
            self.thread_bar.terminate()
            self.barra.setValue(100)

    def setProgressBar(self, val):
        self.barra.setValue(val)

    def setLabel(self, texto):
        self.label.setText(texto)



partida = QtWidgets.QApplication(sys.argv)
carregador = Hc()
carregador.show()
partida.exec_()
