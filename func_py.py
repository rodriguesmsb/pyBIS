#!/usr/bin/env python3

import os, platform, sys, app, threading, GUIdbsus, time
import pandas as pd
from pyalert import *
from pydbsus import Datasus

def get_system(banco):
    if platform.system().lower() == 'linux':
        try:
            os.mkdir(os.path.expanduser('~/Documentos/files_csv/'))
        except:
            pass
        diretorio = os.path.expanduser('~/Documentos/files_csv/')
        data = [x for x in os.listdir(diretorio)]

        if banco in data:
            try:
                arquivo = pd.read_csv(diretorio+banco)
                return (arquivo)
            except:
                return 'O arquivo não existe'
        else:
            print ('O banco requisitado não existe')

    elif platform.system().lower() == 'windows':
        try:
            os.mkdir(os.path.expanduser('~\\Meus Documentos\\files_csv\\'))
        except:
            pass
        diretorio = os.path.expanduser('~\\Meus Documentos\\files_csv\\')

def button_off(*args):
    for i in args:
        i.setEnabled(False)

def button_on(*args):
    for i in args:
        i.setEnabled(True)

def get_var(*args):
    for i in args:
        if i.isChecked():
            return i.text()
        else:
            i.setEnabled(False)

def get_checked(*args):
    for i in args:
        if i.isChecked():
            return i.text()

def get_db(arg):
    pass

def box_download(*args):
    global w

    w = QtWidgets.QDialog()
    ui = Ui_pergunta()
    ui.setupUi(w)
    ui.do_arquivo.clicked.connect(get_db)
   #ui.da_memoria.clicked.connect(lambda: get_db(2))
    w.show()
