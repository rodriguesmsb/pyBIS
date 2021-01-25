import re
from os import path
from PyQt5.QtGui import QStandardItem, QStandardItemModel
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import QFileDialog


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
        model.appendRow(QStandardItem(combobox.currentText() + ' '
                                      + line.text()))


def operator_line_edit(operator, edit):
    if len(edit.text()) == 0:
        edit.setText(operator.sender().text() + ' ')
    else:
        line = edit.text().split(' ')
        if len(line) > 1:
            edit.setText(operator.sender().text() + ' ' + line[1])
        elif len(line) == 1:
            edit.setText(operator.sender().text() + ' ')


def write_header(table, expression, filtered):
    table.setRowCount(10)
    for row in range(0, 11):
        if not table.item(row, 0):
            table.setItem(row, 0,
                          QTableWidgetItem(
                              f'{expression[0]} {selector} {expression[2]}')
            )
            break

    cols = []
    if '_c0' in filtered[0].__fields__:
        cols = [x for x in filtered[0].__fields__[1:]]
    else:
        cols = [x for x in filtered[0].__fields__]
    cols.sort()

    for idx, col in enumerate(cols):
        table.setHorizontalHeaderItem(idx, QTableWidgetItem(col))


def write_body():
    col_n = 0
    row_n = 0

    for itm in cols:
        for i in range(11):
            table.setItem(row, col_n,
                          QTableWidgetItem(str(filtered[i][itm])))
            row_n += 1
        row_n = 0
        col_n += 0


def apply_filter(combobox, line, panel):
    model = panel.column_ext.model()
    expression = [model.item(idx).text()
                  for idx in range(model.rowCount())]

    # filtered = None
    # for ff in expression:
    panel.data = panel.data.filter(' & '.join(expression)).collect()
    print('rodou')


def export_file_csv(button, panel):
    try:
        filename, _ = QFileDialog.getSaveFileName(panel, 'Salvar Arquivo',
                                                  f'{dir_dbc}',
                                                  'file csv (*.csv)')
        if filename:
            panel.data.toPandas().to_csv(filename, index=False)
    except NameError:
        pass
