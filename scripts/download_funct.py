import os
import sys
import json
import re
from time import sleep
from PyQt5.QtWidgets import QTableWidgetItem, QApplication, QMessageBox
from PyQt5.QtGui import QStandardItem, QStandardItemModel
from PyQt5.QtCore import QThread, pyqtSignal, pyqtSlot, QTimer

from pydatasus import PyDatasus
from f_spark import spark_conf, start_spark


dir_json = os.path.join(os.path.dirname(__file__), '../json/')
dir_spatial_conf = os.path.join(os.path.dirname(__file__),
                                '../scripts/SpatialSUSapp/conf/')


class _Loop(QThread):
    sinal = pyqtSignal(int)

    def __init__(self, thread):
        super().__init__()
        self.thread = thread

    def run(self):
        n = 0

        while self.thread.isRunning():
            n += 1
            if n == 100:
                n = 0

            sleep(0.3)
            QApplication.processEvents()
            self.sinal.emit(n)
        self.sinal.emit(100)


class _Thread(QThread):

    def __init__(self, fn, *arg, **kw):
        super().__init__()
        self.fn = fn
        self.arg = arg
        self.kw = kw

    @pyqtSlot()
    def run(self):
        self.fn(*self.arg, **self.kw)


def read_database():
    with open(dir_json + 'database.json') as f:
        data = json.load(f)
        database = data['database']
        return database


def read_states():
    with open(dir_json + 'locales.json') as f:
        data = json.load(f)
        states = data['estados']
        return states


def load_system():
    for base in read_database():
        list_system = list(base.keys())
    return list_system


def load_base(combobox, key):
    base_list = []
    for bases in read_database():
        if key != 'SELECIONAR SISTEMA':
            for base in bases.get(key):
                base_list.append(base.get('SIG'))
    combobox.clear()
    base_list.sort()
    combobox.addItems(base_list)


def load_locales_():
    keys = set()
    for key in read_states():
        for x in key.keys():
            if x != 'UF':
                keys.add(x)
    return keys

def write_conf(choice):
    with open(dir_spatial_conf + 'conf.json', 'r') as f:
        data = json.load(f)

    with open(dir_spatial_conf + 'conf.json', 'w') as f:
        data['area'] = choice.lower()
        json.dump(data, f, indent=2)


def load_locales_choice(locale, choice):
    uf = set()
    locale.clear()
    for state in read_states():
        uf.add(state.get(choice))
    uf = sorted(uf)
    locale.addItems(uf)


@pyqtSlot(str)
def set_spatial_conf(string):
    for key in read_states():
        if key['ESTADO'] == string:
            write_conf(key['UF'])
        elif key['BRASIL'] == string:
            write_conf('brasil')
        elif key['REGIÃO'] == string:
            write_conf(key['REGIÃO'])


def return_year(year, year_):
    if year_.value() > year.value():
        return [ str(x) for x in range(year.value(), year_.value() + 1) ]

    elif year_.value() < year.value():
        return [ str(x) for x in range(year_.value(), year.value() + 1) ]

    elif year_.value() == year.value():
        return str(year.value())


def return_bases(system, base):
    if system.currentText() != 'SELECIONAR SISTEMA':
        for system_name in read_database():
            for base_name in (system_name.get(system.currentText())):
                if base.currentText() == list(base_name.values())[0]:
                    return system.currentText(), list(base_name.values())[1]


def return_uf(select):
    states_ = []
    if select.currentText() != '':
        for states in read_states():
            if select.currentText() == states.get('ESTADO'):
                return [states.get('UF')]

            elif select is not None:
                if select.currentText() == states.get('REGIÃO'):
                    states_.append(states.get('UF'))

        return states_


def transform_date(system, date):
    if system == 'SINAN':
        if isinstance(date, list):
            return [x[2:4] for x in date]

        elif isinstance(date, str):
            return date[2:4]

    elif system == 'SIH':
        if isinstance(date, list):
            return [x[2:4] + r'\d{2}' for x in date]

        elif isinstance(date, str):
            return date[2:4] + r'\d{2}'

    else:
        return date


@pyqtSlot(int)
def update_progressbar(val):
    pbar.setValue(val)


def gen_csv(system, database, states, year, year_, program):
    try:
        system_, base_ = return_bases(system, database)
    except TypeError:
        ...

    state_ = []
    verify_states = return_uf(states)

    if verify_states is None:
        for ufs in read_states():
            state_.append(ufs['UF'])
    else:
        state_ = verify_states

    try:
        if system_ == 'SIH':
            system_ = 'SIHSUS'
        date_ = return_year(year, year_)
        download = [
            system_, base_, state_, date_,
        ]
    except UnboundLocalError:
        ...

    program.datasus = _Thread(PyDatasus().get_data, *download)
    program.loop = _Loop(program.datasus)
    global pbar
    pbar = program.progressBar
    program.loop.sinal.connect(update_progressbar)
    program.datasus.start()
    program.loop.start()


def active_spark(cores, mem):
    conf = spark_conf('pyBis', cores.value(), mem.value(), driver_memory=20)
    spark = start_spark(conf)
    return spark


def create_regex(system, base, state_, date_):
    date_ = transform_date(system, date_)
    if isinstance(state_, str) and isinstance(date_, str):
        return [ base + state_ + date_ + '.csv' ]

    elif isinstance(state_, str) and isinstance(date_, list):
        return [ base + state_ + date + '.csv' for date in dates ]

    elif isinstance(state_, list) and isinstance(date_, str):
        return [ base + state + date_ + '.csv' for state in state_ ]

    elif isinstance(state_, list) and isinstance(date_, list):
        return [
            base + state + date + '.csv'
            for state in state_
            for date in date_
        ]


def finder_csv(system, regex):
    bases = re.compile('|'.join(regex))
    files = []
    for file_csv in os.listdir(
        os.path.expanduser('~/datasus_dbc/' + system + '/')):
        if re.search(bases, file_csv):
            files.append(
                os.path.expanduser(
                    '~/datasus_dbc/' + system + '/' + file_csv))
    return files


def read_file(files, cores, mem):
    spark = active_spark(cores, mem)
    df = spark.read.csv(files, header=True)
    return df


def arranges_columns(df):
    if df.columns[0] == '_c0':
        return [ col for col in df.columns[1:] ]

    else:
        return [ col for col in df.columns ]


def write_header(table, df):
    cols = arranges_columns(df)
    table.setRowCount(10)
    table.setColumnCount(len(cols))
    cols.sort()

    for i, col in enumerate(cols):
        table.setHorizontalHeaderItem(i, QTableWidgetItem(col))
        i += 1


def write_body(table, df):
    cols = arranges_columns(df)
    cols.sort()

    col_n = 0
    row_n = 0

    for line in cols:
        for r in range(1, 11):
            table.setItem(row_n, col_n, QTableWidgetItem(
                str(df.select(df[line]).take(r)[r - 1][0])))

            row_n += 1
        row_n = 0
        col_n += 1


def write_table(table, df):
    write_header(table, df)
    write_body(table, df)


def send_data_to_etl_column(header, column_add):
    cols = arranges_columns(header)
    cols.sort()
    model = QStandardItemModel()
    column_add.setModel(model)
    for item in cols:
        line  = QStandardItem(item)
        model.appendRow(line)


def send_data_to_combobox(header, combobox):
    cols = arranges_columns(header)
    cols.sort()
    combobox.clear()
    combobox.addItems(cols)


def send_(data, panel):
    panel.data = data


def gen_sample(system, base, state, year, year_p, table, cores, mem,
               column_add, column_apply, combobox, panel):
    table.clear()
    model = QStandardItemModel()
    column_apply.setModel(model)

    system_, base = return_bases(system, base)
    year = return_year(year, year_p)
    state = return_uf(state)

    try:
        if system.currentText() == 'SIH':
            data = read_file(finder_csv('SIHSUS', create_regex(system_, base,
                                                               state, year)),
                             cores, mem)
            write_table(table, data)
            send_data_to_etl_column(data, column_add)
            send_data_to_combobox(data, combobox)
            send_(data, panel)

        else:
            data = read_file(
                finder_csv(system.currentText(), create_regex(system_, base,
                                                              state, year)),
                cores, mem)
            write_table(table, data)
            send_data_to_etl_column(data, column_add)
            send_data_to_combobox(data, combobox)
            send_(data, panel)
    except:
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setText("Error")
        msg.setInformativeText('More information')
        msg.setWindowTitle("Error")
        msg.exec_()


def thread_gen_sample(system, base, state, year, year_p, table, cores, mem,
                      column_add, column_apply, combobox, program, panel):

    program.thread_sample = _Thread(gen_sample, *[system, base, state, year,
                                    year_p, table, cores, mem, column_add,
                                    column_apply, combobox, panel])
    program.loop = _Loop(program.thread_sample)
    global pbar
    pbar = program.progressBar
    program.loop.sinal.connect(update_progressbar)
    program.thread_sample.start()
    program.loop.start()
