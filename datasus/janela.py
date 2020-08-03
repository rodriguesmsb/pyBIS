#!/usr/bin/env python3

from tkinter import *
from tkinter import ttk
import threading, time, os
from pydbgui.pydatasus import PyDatasus

# -------------- Funções  -------------- #

count = {'SIM':0, 'SINAN':0, 'SINASC':0}

def get_csv():

    if var.get() == 1:
        PyDatasus().get_csv('SIM')
        count['SIM'] += 1
    elif var.get() == 2:
        PyDatasus().get_csv('SINAN')
        count['SINAN'] += 1
    elif var.get() == 3:
        PyDatasus().get_csv('SINASC')
        count['SINASC'] += 1
    else:
        CSV['state'] = 'normal'

def get_db():

    if var.get() == 1:
        if count['SIM'] > 0:
            PyDatasus().get_db('SIM')
        elif count['SIM'] <= 0:
            pass

    elif var.get() == 2:
        if count['SINAN'] > 0:
            PyDatasus().get_db('SINAN')
        elif count['SINAN'] <= 0:
            pass

    elif var.get() == 3:
        if count['SINASC'] > 0:
            PyDatasus().get_db('SINASC')
        elif count['SINASC'] <= 0:
            pass

def loop():
    n = 0
    while t1.is_alive():
        DB['state'] = 'disable'
        time.sleep(0.2)
        n += 1
        barra.set(n)
        if n >= 99:
            n = 0
    n = 100

def thread_csv():
    global t1
    t1 = threading.Thread(target = get_csv, daemon = True)
    t1.start()
    threading.Thread(target = loop, daemon = True).start()

def thread_db():
    global t1
    t1 = threading.Thread(target = get_db, daemon = True)
    t1.start()
    threading.Thread(target = loop, daemon = True).start()

def start_spark():
    setSpark.spark_conf
    setSpark.start_spark

    print ('spark configurad\nspark startado')
# -------------- Instancia de Tk e "base" de design -------------- #

w = Tk()

try:

    w.call('wm', 'iconphoto', w._w, \
            PhotoImage(file='/home/fabio/Imagens/uni.png'))

except:
    pass

w.title('PyDatasus')
w.resizable(False,False)

# -------------- Configurando variaveis -------------- #

FONTE = ('Arial','13')
var = IntVar()
barra = DoubleVar()
barra.set(0)

# -------------- Criando abas ------------------------#
abas = ttk.Notebook(w)

exportacao = Canvas(w, bg = '#F0FFF0')
exportacao.pack()

visualizar = Canvas(w, bg = '#F0FFF0')
visualizar.pack()

manipulacao = Canvas(w, bg = '#F0FFF0')
manipulacao.pack()

funcao = Canvas(w, bg = '#F0FFF0')
funcao.pack()

spark = Canvas(w, bg = '#F0FFF0')
spark.pack()

# -------------- Instanciando widgets p/ acessar propriedades -------------- #

START = Button(text = 'Start Spark', command = start_spark, \
               bg = '#F0FFF0', font = FONTE)

SIM = Radiobutton(text = 'SIM', variable = var, value = 1, \
        bg = '#F0FFF0', font = FONTE)

SINAN = Radiobutton(text = 'SINAN', variable = var, value = 2, \
        bg = '#F0FFF0', font = FONTE)

SINASC = Radiobutton(text = 'SINASC', variable = var, value = 3, \
        bg = '#F0FFF0', font = FONTE)

CSV = Button(text = 'Gerar csv', command = thread_csv, width = 10, \
        bg = '#F0FFF0', font = FONTE)

DB = Button(text = 'Download db', command = thread_db, width = 10, \
        bg = '#F0FFF0', font = FONTE)

BARRA_PROGRESSO = ttk.Progressbar(variable = barra, maximum = 100, \
        length = 330)

# -------------- Disposição dos widgets no funcao -------------- #

spark.create_window(190, 120, window = START, anchor = 'center')

funcao.create_window(30, 40, window = SIM, anchor = 'nw')

funcao.create_window(30, 75, window = SINAN, anchor = 'nw')

funcao.create_window(30, 110, window = SINASC, anchor = 'nw')

funcao.create_window(230, 40, window = CSV, anchor = 'nw')

funcao.create_window(230, 90, window = DB, anchor = 'nw')

funcao.create_window(30, 210, window = BARRA_PROGRESSO, anchor = 'nw')

# -------------- Cria o loop da janela principal -------------- #

abas.add(funcao, text = 'Funções')

abas.add(spark, text='Spark')

abas.add(visualizar, text = 'Visualização')

abas.add(manipulacao, text = 'Manipulação')

abas.add(exportacao, text = 'Exportação')

abas.pack()

w.mainloop()
