from os import path
from PyQt5.QtWidgets import QFileDialog, QTableWidgetItem
from PyQt5.QtGui import QStandardItem, QStandardItemModel
import pandas as pd


tables = []
dir_dbc = path.expanduser('~/datasus_dbc/')


def year_month(date):
    def correct_state(x):
        x = str(x)
        if len(x) < 8:
            x = "0" + x
        return x

    date = date.apply(lambda x: correct_state(x))
    date = pd.to_datetime(date.astype(str), format="%d%m%Y")
    year, month = date.dt.year, date.dt.month

    return (year, month)


def verify_column(data):
    if 'TIPOBITO' in data.columns:
        year, month = year_month(data['DTOBITO'])
        data['YEAR'] = year
        data['MONTH'] = month

    elif 'DTNASC' in data.columns and 'TPOBITO' not in data.columns:
        year, month = year_month(data['DTNASC'])
        data['YEAR'] = year
        data['MONTH'] = month

    return data

def write_header_table(data, table):
    header = data.columns[1:]
    table.clear()
    table.setRowCount(10)
    table.setColumnCount(len(header))
    i = 0
    for h in header:
        table.setHorizontalHeaderItem(i, QTableWidgetItem(h))
        i += 1


def write_body_table(data, table):
    items = data.columns[1:]
    col_n = 0
    row_n = 0

    for item in items:
        for i in range(1, 11):
            table.setItem(row_n, col_n, QTableWidgetItem(str(data[item][i])))
            row_n += 1
        row_n = 0
        col_n += 1


def get_same_columns(tables, combobox):
    combobox.clear()
    result_index = []
    for cols in tables[0].columns[1:]: #data_1.columns[1:]:
        if cols in tables[1].columns[1:]: #data_2.columns[1:]:
            result_index.append(cols)
    combobox.addItems(result_index)
    tables.clear()


def load_data(button, table, combobox):
    global tables

    try:
        filename, _ = QFileDialog.getOpenFileName(button, 'Carregar Arquivo',
                                                  f'{dir_dbc}',
                                                  'File csv (*.csv)')
        data = verify_column(pd.read_csv(str(filename), low_memory=False,
                                         encoding='iso-8859-1'))

        tables.append(data)
        write_header_table(data, table)
        write_body_table(data, table)

        if len(tables) > 1:
            get_same_columns(tables, combobox)

    except FileNotFoundError:
        pass


def verify_items(columnview):
    try:
        model = columnview.model()
        return (model, 
                [model.item(index).text()
                 for index in range(model.rowCount())])
    except AttributeError:
        pass


def add_column_same(combobox, columnview):
    model, list_model = verify_items(columnview)
    model.clear()
    text = combobox.currentText()
    if text not in list_model:
        model.appendRow(QStandardItem(text))
