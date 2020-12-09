from os import path
import sys
from PyQt5.QtWidgets import QApplication, QWidget, QLabel
from PyQt5.QtGui import QIcon, QMovie

img_folder = path.dirname(__file__)
img_dash = path.join(img_folder, 'imgs/dash/')


class Dashboard(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle('Dashboard')
        self.setWindowIcon(QIcon(img_dash + 'dashboard.png'))

        label = QLabel(self)
        gif = QMovie(img_dash + 'dev.gif')
        label.setMovie(gif)
        gif.start()
        # self.resize(gif.width(), gif.height())

        self.show()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    dash = Dashboard()
    sys.exit(app.exec_())
