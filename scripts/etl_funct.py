import re
from PyQt5.QtGui import QStandardItem, QStandardItemModel
from PyQt5.QtCore import pyqtSlot


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
        model.appendRow(QStandardItem(combobox.currentText() + line.text()))


def operator_line_edit(operator, edit):
    if len(edit.text()) == 0:
        edit.setText(operator.sender().text() + ' ')
    else:
        line = edit.text().split(' ')
        if len(line) > 1:
            edit.setText(operator.sender().text() + ' ' + line[1])
        elif len(line) == 1:
            edit.setText(operator.sender().text() + ' ')


def apply_filter(expression):
    print(expression.text())


def export_file_csv(dataframe):
    pass
