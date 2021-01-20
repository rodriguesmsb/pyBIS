import os
import sys
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class Merge(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi(layout + 'merge.ui', self)
