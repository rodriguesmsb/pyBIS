from os import path
from PyQt5.QtWidgets import QFileDialog
import geopandas as gpd
import fiona


dir_dbc = path.expanduser('~/datasus_dbc/')


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
        filename, _ = QFileDialog.getOpenFileName(button, 'Carregar Arquivo',
                                                  f'{dir_dbc}',
                                                  'File csv (*.csv)')
        line.setEnabled(True)
        line.setText(filename)
    except FileNotFoundError:
        pass


def trade_frame(layout, parent, frame):
    parent.setHidden(True)
    frame.setHidden(False)
