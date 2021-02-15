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
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


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
    manager.setWindowIcon(QIcon(icos + 'bis.png'))
    sys.exit(app.exec_())
