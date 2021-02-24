import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabBar, QTabWidget,
                             QStyle, QStyleOptionTab, QStylePainter)
from PyQt5.QtCore import QRect, QPoint, Qt
from PyQt5.QtGui import QIcon, QFont
import json


dir_spatial = os.path.join(os.path.dirname(__file__),
                                           "../scripts/SpatialSUSapp/conf/")

class TabBar(QTabBar):
    def tabSizeHint(self, index):
        s = QTabBar.tabSizeHint(self, index)
        s.transpose()
        return s

    def paintEvent(self, event):
        painter = QStylePainter(self)
        opt = QStyleOptionTab()

        for i in range(self.count()):
            self.initStyleOption(opt, i)
            painter.drawControl(QStyle.CE_TabBarTabShape, opt)
            painter.save()

            s = opt.rect.size()
            s.transpose()
            r = QRect(QPoint(), s)
            r.moveCenter(opt.rect.center())
            opt.rect = r

            c = self.tabRect(i).center()
            painter.translate(c)
            painter.rotate(90)
            painter.translate(-c)
            painter.drawControl(QStyle.CE_TabBarTabLabel, opt)
            painter.restore()


class TabWidget(QTabWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setTabBar(TabBar(self))
        self.setTabPosition(QTabWidget.West)


class Manager(QMainWindow):
    def __init__(self, *tabs):
        super().__init__()

        self.setWindowTitle('pyBiss')
        self.setFont(QFont('Arial', 12))
        self.setWindowIcon(QIcon('imgs/bis.png'))
        self.tab_manager = TabWidget()

        for tab in tabs:
            self.tab_manager.addTab(tab, QIcon(tab.windowIcon()), '')

        self.setCentralWidget(self.tab_manager)
        self.show()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main = Manager()
    sys.exit(app.exec_())
