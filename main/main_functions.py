#!/usr/bin/env python3

import subprocess
from os import path, remove, listdir
from os.path import expanduser
from glob import glob
from sys import argv, exit
from time import sleep
from threading import Thread
from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt5.QtCore import QThread, pyqtSignal, pyqtSlot

from pydatasus import PyDatasus
from convert_dbf_to_csv import ReadDbf
from f_spark import spark_conf, start_spark


class MyThread(QThread):

    def __init__(self, function, *args, **kwargs):
        super().__init__()

        self.function = function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.function(*self.args, **self.kwargs)
        return


class Funcoes:

    def __init__(self, thread, system, base, dict_states, select_region,
                 progressbar, delimitation, date_init, date_final, *buttons):

        self.thread = thread
        self.system = system.currentText()
        self.base_text = base.currentText()
        self.base = base
        self.dict_states = dict_states
        self.progressbar = progressbar
        self.select_region = select_region
        self.delimitation_text = delimitation.currentText()
        self.delimitation = delimitation
        self.date_init = date_init.value()
        self.date_final = date_final.value()
        self.all_buttons = buttons

    def disable_activate_buttons(self):
        for button in self.all_buttons:
            if button.isEnabled():
                button.setEnabled(False)

            else:
                button.setEnabled(True)

    def range_date(self):
        if self.date_init < self.date_final:
            return [date for date in range(self.date_init, self.date_final
                    + 1)]

        elif self.date_init > self.date_final:
            return [date for date in range(self.date_final, self.date_init
                    + 1)]

        elif self.date_init == self.date_final:
            return self.date_init

    def download_csv_dbc(self):

        dict_disease = {'Óbito': 'DO', 'Óbito Fetal': 'DOFE',
                        'Animais Pençonhentos': 'ANIM', 'Botulismo': 'BOTU',
                        'Chagas': 'CHAG', 'Cólera': 'COLE',
                        'Coqueluche': 'COQU', 'Difteria': 'DIFT',
                        'Esquistossomose': 'ESQU', 'Febre Maculosa': 'FMAC',
                        'Febre Tifóide': 'FTIF', 'Hanseníase': 'HANS',
                        'Leptospirose': 'LEPT', 'Meningite': 'MENI',
                        'Raiva': 'RAIV', 'Tétano': 'TETA',
                        'Tuberculose': 'TUBE'}

        if 'SELECIONAR SISTEMA' != self.system:

            date = self.range_date()

            if self.delimitation.currentText() in self.dict_states:
                reg_states = self.dict_states.get(
                    self.delimitation.currentText())[1]

                self.csv = MyThread(PyDatasus().get_csv,
                                    args=(self.system, self.base, reg_states,
                                    date, self.dict_states,))
                self.csv.start()

        else:
            reg_states = [value[1] for value in list(
                self.dict_states.values()) if value[2] == delimitation]

            csv = Thread(target=PyDatasus().get_csv,
                         args=(system.currentText(), base.currentText(),
                               self.delimitation.currentText(), date,
                               self.dict_states,),
                         daemon=True)

            dbc = Thread(target=PyDatasus().get_db_complete,
                         args=(system.currentText(),), daemon=True)

            convert = Thread(target=convert_dbc_to_csv,
                             args=(system.currentText(),), daemon=True)

            loop = Thread(target=update_loop,
                          args=(csv, dbc, convert, progressbar, buttons,),
                          daemon=True)

            csv.start()
            loop.start()

    def select_database(self):
        self.base.clear()
        self.base.setEnabled(True)

        if 'SIM' == self.system:
            self.base.addItems(['Todos', 'Óbito', 'Óbito Fetal'])

        elif 'SINAN' == self.system:
            self.base.addItems(['Todos', 'Animais Pençonhentos',
                                'Botulismo', 'Chagas', 'Cólera',
                                'Coqueluche', 'Difteria',
                                'Esquistossomose', 'Febre Maculosa',
                                'Febre Tifóide', 'Hanseníase',
                                'Leptospirose', 'Meningite', 'Raiva',
                                'Tétano', 'Tuberculose'])

        elif 'SINASC' == self.system:
            self.base.addItem('Nascidos Vivos')

        elif 'SELECIONAR SISTEMA' == button_system.currentText():
            self.base.setEnabled(False)

    def insert_region_uf_combobox(self):
        self.delimitation.clear()
        self.delimitation.setEnabled(True)

        if 'ESTADO' == self.select_region.currentText():
            for states in self.dict_states.keys():
                self.delimitation.addItem(states)

        elif 'REGIÃO' == self.select_region.currentText():
            self.delimitation.addItems(['Norte', 'Nordeste', 'Sul',
                                        'Sudeste', 'Centro-Oeste'])

        else:
            self.delimitation.setEnabled(False)


def read_system_csv(spark, dir_system, table_preview):
    df = spark.read.csv(dir_system, header=True)

    rows = {}

    if df.columns[0] == '_c0':
        for key in df.columns[1:]:
            rows[key] = []

    else:
        for key in df.columns:
            rows[key] = []

    for i, key in enumerate(df.columns[1:]):
        table_preview.setItem(0, i, QTableWidgetItem(key))

    column_n = 0
    row = 1
    for column in rows.keys():
        for i in range(1, 11):
            table_preview.setItem(
                row, column_n, QTableWidgetItem(
                    str(df.select(df[column]).take(i)[i - 1][0])))

            row += 1
        row = 1
        column_n += 1


def reset_spark(spark):
    try:
        spark.stop()
        print('parou')
    except:
        print('erro')


def visualization(memory, cpu, system, table_preview):
    global spark

    try:
        spark.stop()
    except:

        if system.currentText() != 'SELECIONAR SISTEMA':
            if path.exists(expanduser(
                    f'~/Documentos/files_db/{system.currentText()}/')):
                if (len(listdir(expanduser(
                        f'~/Documentos/files_db/{system.currentText()}/')))
                        == 0):
                    ...
                else:
                    dir_system = expanduser(
                        f'~/Documentos/files_db/{system.currentText()}/')

                    spark = start_spark(spark_conf('PyDatasus',
                                                   cpu.value(),
                                        memory.value()))

                    thread_spark = Thread(target=read_system_csv,
                                          args=(spark, dir_system,
                                                table_preview,),
                                          daemon=True)
                    thread_spark.start()


def remove_dbf(system):
    data_dbf = [x for x in glob(
        expanduser(f'~/Documentos/files_db/{system}/*.dbf'))]
    list(map(remove, data_dbf))


def convert_dbc_to_csv(system):
    data_DBC = [x for x in glob(
        expanduser(f'~/Documentos/files_db/{system}/*.DBC'))]

    data_dbc = [x for x in glob(
        expanduser(f'~/Documentos/files_db/{system}/*.dbc'))]

    data_csv = expanduser(f'~/Documentos/files_db/{system}/')

    list(map(ReadDbf, data_DBC))
    list(map(ReadDbf, data_dbc))

    list(map(remove, data_DBC))
    list(map(remove, data_dbc))
    remove_dbf(system)

    '''
    def update_loop(thread_csv, thread_dbc, thread_convert,
                    progressbar, buttons: list):

        disable_activate_buttons(buttons)
        progressbar.show()
        val = 0
        while thread_csv.is_alive():
            sleep(1.2)
            val += 1

            if val >= 98:
                val = 0
            progressbar.progress_bar.setValue(val)
        progressbar.progress_bar.setValue(100)

        thread_dbc.start()
        while thread_dbc.is_alive():
            sleep(1.2)
            val += 1

            if val >= 98:
                val = 0
            progressbar.progress_bar.setValue(val)
        progressbar.progress_bar.setValue(100)

        thread_convert.start()
        while thread_convert.is_alive():
            sleep(1.2)
            val += 1

            if val >= 98:
                val = 0
            progressbar.progress_bar.setValue(val)
        progressbar.progress_bar.setValue(val)
        val = 100
        sleep(1)
        disable_activate_buttons(buttons)
        progressbar.destroy()
    '''


def insert_info_table(columns, data, table_preview):
    for i, column in enumerate(columns):
        table_preview.setItem(0, i, QTableWidgetItem(column))
    column = 0
    row = 1
    for i in range(5):
        for column in columns:
            for tag_row in data[tag_column]:
                table_preview.setItem(
                    row, column, QTableWidgetItem(str(tag_row)))
                row += 1
            row = 1
            column += 1
