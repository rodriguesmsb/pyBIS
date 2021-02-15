import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QWidget, QLabel
from PyQt5.QtGui import QIcon, QMovie

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class Dashboard(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Dashboard')
        self.setWindowIcon(QIcon(icos + 'dashboard.png'))

        label = QLabel(self)
        gif = QMovie(icos + 'dev.gif')
        label.setMovie(gif)
        gif.start()
