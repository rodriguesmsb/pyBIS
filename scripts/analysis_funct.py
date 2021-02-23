import sys
from os import path, system
import psutil
import webbrowser
from PyQt5.QtWidgets import QFileDialog
from PyQt5.QtCore import pyqtSlot, QThread, pyqtSignal
import json


sys.path.append(path.join(path.dirname(__file__), 'SpatialSUSapp'))
dir_spatial = path.join(path.dirname(__file__), 'SpatialSUSapp/conf/')
dir_dbc = path.expanduser('~/datasus_dbc/')


class MyThread(QThread):

    totsignal = pyqtSignal(int)
    cnt = pyqtSignal(int)

    def __init__(self, func, *args):
        super().__init__()
        self.threadactive = True
        self.func = func
        self.args = args

    def stop(self):
        self.threadactive = False
        self.kill()

    def run(self):
        try:
            self.func(*self.args)
        except TypeError:
            self.func()


def load_items(filename, frame):
    data = gpd.read_file(filename)
    combobox_s = [frame.comboBox, frame.comboBox_2, frame.comboBox_3]
    [combobox.clear() for combobox in combobox_s]
    [combobox.addItems(list(data.columns)) for combobox in combobox_s]


def get_shapefile(button):
    filename, _ = QFileDialog.getOpenFileName(button, 'Carregar Arquivo',
                                              f'{dir_dbc}',
                                              'File shp (*.shp)')
    # button.setEnabled(True)
    button.setText(filename)
    try:
        load_items(filename, frame)
    except fiona.errors.DriverError:
        pass


def get_csv(button, line):
    try:
        filename, _ = QFileDialog.getOpenFileName(
            button, 'Carregar Arquivo', f'{dir_dbc}', 'File csv (*.csv)'
        )
        line.setEnabled(True)
        line.setText(filename)
    except FileNotFoundError:
        pass


def trade_frame(layout, parent, frame):
    parent.setHidden(True)
    frame.setHidden(False)


def activate(checkbox, program):

    def write_conf(checkbox):
        with open(dir_spatial + 'conf.json', 'r') as f:
            data = json.load(f)
        with open(dir_spatial + 'conf.json', 'w') as f:
            data["type"] = checkbox
            json.dump(data, f, indent=2)

    if checkbox == "spatio_temporal":
        program.comboBox_2.setEnabled(True)
        program.comboBox_3.setEnalbed(True)
    elif checkbox == "spatial":
        program.comboBox_2.setEnabled(False)
        program.comboBox_3.setEnalbed(False)

    write_conf(checkbox)


def write_conf_time_unit(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data['time_unit'] = combobox
        json.dump(data, f, indent=4)


def write_conf_id_area(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data['id_area'] = combobox
        json.dump(data, f, indent=4)


def write_conf_cat(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data["var_cat"][0] = combobox
        json.dump(data, f, indent=4)


def write_conf_cat_1(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data["var_cat"][1] = combobox
        json.dump(data, f, indent=4)


def write_conf_num(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data["var_num"][0] = combobox
        json.dump(data, f, indent=4)


def write_conf_num_1(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data["var_num"][1] = combobox
        json.dump(data, f, indent=4)

def write_id_area(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data["id_area"] = combobox
        json.dump(data, f, indent=4)

def write_time_col(combobox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data["time_col"] = combobox
        json.dump(data, f, indent=4)


def start_server(program):
    import index

    def restart_server(thread, var_cat, var_cat_2, var_num, var_num_2,
                       var_id):
        if thread:
            thread.terminate()
            write_conf_cat(var_cat)
            write_conf_cat_1(var_cat_2)
            write_conf_num(var_num)
            write_conf_num_1(var_num_2)
            write_conf_id_area(var_id)
            write_id_area(program.comboBox.currentText())
            write_time_col(program.comboBox_2.currentText())
            with open(dir_spatial + 'conf.json', 'r') as f:
                data = json.load(f)
            with open(dir_spatial + 'conf.json', 'w') as f:
                data['name'] = program.lineEdit.text()
                json.dump(data, f, indent=4)

            thread.start()
        else:
            thread.start()

    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        data['name'] = program.lineEdit.text()
        json.dump(data, f, indent=4)

    program.analysis = MyThread(index.app.run_server)
    program.nav = MyThread(webbrowser.open, '127.0.0.1:8050')
    program.nav.moveToThread(program.analysis)
    program.analysis.started.connect(program.nav.run)

    restart_server(program.analysis, program.comboBox_7.currentText(),
        program.comboBox_8.currentText(), program.comboBox_9.currentText(),
        program.comboBox_10.currentText(), program.comboBox.currentText()
    )
