#!/usr/bin/env python3

import time
#from pydbgui.pydatasus import PyDatasus
from threading import Thread

def thread_csv(*args):
    global t1

    for i in args[1::]:
        if i.isChecked():
            t1 = Thread(target=gen_csv, args=(i.text(),), daemon=True)
            t1.start()
            for i in args[1::]:
                i.setEnabled(False)
            t2 = Thread(target=verifica, args=args, daemon=True).start()

def thread_db_memory(*args):
    global t1

    for i in args[1::]:
        if i.isChecked():
            t1 = Thread(target=down_db_memory, args=(i.text(),), daemon=True)
            t1.start()
            for i in args[1::]:
                i.setEnabled(False)
            t2 = Thread(target=verifica, args=args, daemon=True).start()

def thread_db_file(*args):
    global t1

    for i in args[1::]:
        if i.isChecked():
            t1 = Thread(target=down_db_file, args=(i.text(),), daemon=True)
            t1.start()
            for i in args[1::]:
                i.setEnabled(False)
            t2 = Thread(target=verifica, args=args, daemon=True).start()

def gen_csv(banco):
    data = PyDatasus()
    data.get_csv(banco)

def down_db_memory(banco):
    data = PyDatasus()
    data.get_db_from_memory(banco)

def down_db_file(banco):
    data = PyDatasus()
    data.get_db_from_file(banco)

def verifica(*args):
    n = 0
    while t1.is_alive():
        n += 1
        if n >= 100:
            n = 0
        time.sleep(1)
        args[0].setValue(n)
    args[0].setValue(100)
    for i in args[1::]:
        i.setEnabled(True)
