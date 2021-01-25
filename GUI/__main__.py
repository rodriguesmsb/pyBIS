#!/usr/bin/env python3

import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QLabel,
                             QStyleFactory)
from PyQt5.QtGui import QIcon, QMovie, QFont
from PyQt5.QtCore import Qt, pyqtSlot

from manager import Manager
from download import Download
from etl import Etl
from merge import Merge
from dashboard import Dashboard
from analysis import AnalysisUI
from config import Config
import download_funct as dd


sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))
layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


# def setStyle(text):
#     if text == 'Fusion':
#         app.setStyleSheet('')
#         app.setStyle(QStyleFactory.create('Fusion'))
#         config.select_font.setEnabled(True)
#         config.select_size_font.setEnabled(True)
#         config.select_color.setEnabled(True)
#     elif text == 'Windows':
#         app.setStyleSheet('')
#         app.setStyle(QStyleFactory.create('Windows'))
#         config.select_font.setEnabled(True)
#         config.select_size_font.setEnabled(True)
#         config.select_color.setEnabled(True)
#     elif text == 'Dark':
#         app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt5'))
#         config.select_font.setEnabled(False)
#         config.select_size_font.setEnabled(False)
#         config.select_color.setEnabled(False)
#     elif text == 'DarkGray':
#         app.setStyleSheet(qdarkgraystyle.load_stylesheet())
#         config.select_font.setEnabled(False)
#         config.select_size_font.setEnabled(False)
#         config.select_color.setEnabled(False)
# 
# 
# @pyqtSlot(int)
# def setFont(val):
#     app.setFont(QFont('', val))
# 
# 
# def setFontFamily(text):
#     app.setFont(QFont(text))
# 
# 
# @pyqtSlot(str)
# def setFontColor(color):
#     print(color)
#     palette.setColor(QPalette.Text, QColor(color))
#     palette.setColor(QPalette.ButtonText, QColor(color))
#     palette.setColor(QPalette.QProgressBar, QColor(color))
#     app.setPalette(palette)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setApplicationName('pyBis')
    download = Download()
    etl = Etl()
    merge = Merge()
    dashboard = Dashboard()
    analysis_ui = AnalysisUI()
    config = Config()

    # config.select_layout.currentTextChanged.connect(setStyle)
    # config.select_size_font.valueChanged.connect(setFont)
    # config.select_font.currentTextChanged.connect(setFontFamily)
    # config.select_color._color.connect(setFontColor)

    download.view_data.clicked.connect(
            lambda: dd.thread_gen_sample(download.database, download.base,
                                         download.locale_, download.year,
                                         download.year_, download.sample,
                                         download.cores, download.memory,
                                         etl.column_add, etl.column_apply,
                                         etl.line_select, download)
    )

    manager = Manager(download, etl, merge, dashboard, analysis_ui, config)
    manager.setWindowIcon(QIcon(icos + 'bis.png'))
    sys.exit(app.exec_())
