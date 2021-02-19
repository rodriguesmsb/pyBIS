import re
from os import path
from PyQt5.QtGui import QStandardItem, QStandardItemModel
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import QFileDialog, QTableWidgetItem
import csv

from download_funct import write_table


dir_dbc = path.expanduser('~/datasus_dbc/')
dir_spatial = path.join(path.dirname(__file__), "SpatialSUSapp/")


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


def drop_list(col, data):
    items_selected = []
    drop_list = []
    model = col.model()
    if model.rowCount():
        for idx in range(model.rowCount()):
            items_selected.append(model.item(idx).text())
        for itm in list(data.columns):
            if itm not in items_selected:
                drop_list.append(itm)
        return data.drop(*drop_list)
    else:
        return data


def apply_filter(combobox, line, panel, analysis):
    panel.data_drop = drop_list(panel.column_apply, panel.data)

    try:
        _, expression = verify_items(panel.column_ext)

        panel.filtered = panel.data_drop.filter(' and '.join(expression))
    except:
        panel.filtered = panel.data_drop

    cols = []
    try:
        if '_c0' in panel.filtered.columns[0]:
          cols = [x for x in panel.filtered.columns[1:]]
        else:
          cols = [x for x in panel.filtered.columns]
    except IndexError:
        pass
    cols.sort()
    analysis.comboBox_7.addItems(cols)
    analysis.comboBox_8.addItems(cols)
    analysis.comboBox_9.addItems(cols)
    analysis.comboBox_10.addItems(cols)
    panel.table_export.setColumnCount(len(cols))
    panel.table_export.setRowCount(10)
    panel.table_export.clear()

    try:
        write_table(panel.table_export, panel.filtered)
        panel.filtered.toPandas().to_csv(dir_spatial + "data/"
                                         + "data.csv", index=False)
    except IndexError:
        print("Erro de index")
        pass


def export_file_csv(button, panel):
    try:
        filename, _ = QFileDialog.getSaveFileName(panel, 'Salvar Arquivo',
                                                  f'{dir_dbc}',
                                                  'file csv (*.csv)')
        if filename.endswith(".csv"):
            panel.filtered.coalesce(1).write.option("header",
                                                    "true").csv(filename)
        else:
            panel.filtered.coalesce(1).write.option("header",
                                                    "true").csv(filename
                                                                + '.csv')

    except NameError:
        pass
