#!/usr/bin/env python3

import os
import subprocess
import sys
from threading import Thread
import webbrowser
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
icos = os.path.join(os.path.dirname(__file__), 'imgs/')

from scripts.SpatialSUSapp import app as server


def active_dashboard(val):
    def attach(index):
        os.system(f'python3 {index}')
        webbrowser.open_new('http://127.0.0.1:8050')
        QApplication.processEvents()

    if val == 3:
        server.run()#.run_server(debug=True)
        # index = os.path.join(os.path.dirname(__file__),
        #                      '../scripts/SpatialSUSapp/index.py')
        # subprocess.Popen(["python3", index])
        # thread = Thread(target=attach, args=(index,), daemon=True)
        # thread.start()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setApplicationName('pyBis')
    download = Download()
    etl = Etl()
    merge = Merge()
    dashboard = Dashboard()
    analysis = AnalysisUI()
    config = Config()

    download.view_data.clicked.connect(
            lambda: dd.thread_gen_sample(download.database, download.base,
                                         download.locale_, download.year,
                                         download.year_, download.sample,
                                         download.cores, download.memory,
                                         etl.column_add, etl.column_apply,
                                         etl.line_select, download, etl)
    )

    manager = Manager(download, etl, merge, dashboard, analysis, config)
    manager.tab_manager.currentChanged.connect(active_dashboard)
    manager.setWindowIcon(QIcon(icos + 'bis.png'))
    sys.exit(app.exec_())
