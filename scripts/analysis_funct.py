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
            self.func(self.args)
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


def activate(checkbox):
    with open(dir_spatial + 'conf.json', 'r') as f:
        data = json.load(f)
    with open(dir_spatial + 'conf.json', 'w') as f:
        if checkbox.text() == 'Espaço-Temporal':
            data["type"] = 'espatio_temporal'
        elif checkbox.text() == 'Espacial':
            data["type"] = 'espatial'
        json.dump(data, f, indent=2)


def start_server(program):
    import index

    if program.analysis == None:
        program.analysis = MyThread(index.app.run_server)
        program.analysis.start()
    elif program.analysis != None:
        program.analysis.terminate()
        program.analysis = MyThread(index.app.run_server)
        program.analysis.start()

    program.nav = Thread(target=webbrowser.open, args=('127.0.0.1:8050',),
                         daemon=True)
    program.nav.start()
