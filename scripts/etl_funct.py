import re
from os import path
from PyQt5.QtGui import QStandardItem, QStandardItemModel
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import QFileDialog, QTableWidgetItem
import csv


dir_dbc = path.expanduser('~/datasus_dbc/')


def verify_items(col):
    try:
        model = col.model()
        return (model,
                [model.item(index).text()
                for index in range(model.rowCount())])
    except AttributeError:
        pass


def add_column(col_1, col_2):
    itms = col_1.selectedIndexes()
    try:
        model, list_model = verify_items(col_2)
    except TypeError:
        pass

    try:
        for itm in itms:
            lines = []
            if itm.data() not in list_model:
                lines.append(QStandardItem(itm.data()))
                model.appendRow(lines)
    except AttributeError:
        pass


def rm_column(col):
    model = col.model()
    itms = col.selectedIndexes()
    for itm in itms:
        model.takeRow(itm.row())


def add_list_filter(combobox, line, model):
    if len(combobox.currentText() + line.text()) != 0:
        model.appendRow(QStandardItem(combobox.currentText()
                                      + ' ' + line.text()))


def operator_line_edit(operator, edit):
    if len(edit.text()) == 0:
        edit.setText(operator.sender().text() + ' ')
    else:
        line = edit.text().split(' ')
        if len(line) > 1:
            edit.setText(operator.sender().text() + ' ' + line[1])
        elif len(line) == 1:
            edit.setText(operator.sender().text() + ' ')


def apply_filter(combobox, line, panel):
    model = panel.column_ext.model()
    expression = [model.item(idx).text() for idx in range(model.rowCount())]

    panel.filtered = panel.data.filter(' and '.join(expression))

    cols = []
    if '_c0' in panel.filtered.columns[0]:
      cols = [x for x in panel.filtered.columns[1:]]
    else:
      cols = [x for x in panel.filtered.columns]
    cols.sort()

    panel.table_export.setColumnCount(len(cols))
    panel.table_export.setRowCount(10)
    panel.table_export.clear()

    for n, col in enumerate(cols):
        panel.table_export.setHorizontalHeaderItem(n, QTableWidgetItem(col))
    
    col_n = 0
    row_n = 0

    for line in cols:
        for i in range(1, 11):
            panel.table_export.setItem(
                row_n, col_n, QTableWidgetItem(
                    str(panel.filtered.select(
                        panel.filtered[line]).take(i)[i - 1][0])))
            row_n += 1
        row_n = 0
        col_n += 1


def export_file_csv(button, panel):
    try:
        filename, _ = QFileDialog.getSaveFileName(panel, 'Salvar Arquivo',
                                                  f'{dir_dbc}',
                                                  'file csv (*.csv)')
        if filename:
            with open('{}.csv'.format(filename), 'w+') as csvfile:
                data = csv.writer(csvfile)
                data.writerow(panel.filtered.columns)
                for row in panel.filtered:
                    data.writerow(list(row))
    except NameError:
        pass
