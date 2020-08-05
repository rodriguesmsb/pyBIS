#!/usr/bin/env python3

from PyQt5.QtCore import pyqtSlot
from sys import argv, exit
import interface.myfunctions as functions
import interface.myspark as spark
from interface.pydbGUI import *
from interface.msgbox import *


class Main(QtWidgets.QMainWindow):

    def __init__(self, parent=None):
        QtWidgets.QMainWindow.__init__(self, parent)
        self.setFixedSize(840, 320)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.abas.resize(840, 320)
        self.ui.spin_proc.setMinimum(2)

        '''
        funçoes na aba de funcao
        '''

        self.ui.b_generate.clicked.connect(
            lambda: functions.thread_csv(
                self.ui.b_progress,
                self.ui.b_generate,
                self.ui.b_download, self.ui.r_sim,
                self.ui.r_sinan, self.ui.r_sinasc))

        self.ui.b_download.clicked.connect(self.msgbox)

        '''
        funçoes na aba do spark
        '''

        self.ui.spark.clicked.connect(
            lambda: spark.start_spark(spark.spark_conf(
                app_name='AggData', n_cores=self.ui.spin_proc.value(),
                executor_memory=self.ui.spin_mem.value())))

        self.ui.b_readdata.clicked.connect(
            lambda: spark.spark_read(
                self.ui.comboBox, self.ui.list_data))

        self.ui.combo_space.activated.connect(self.get_combo_space)
        self.ui.qslader_year.valueChanged.connect(self.get_slader_year)

    def msgbox(self):
        '''
        funçao especifica do carregamento da caixa de download
        '''
        for b in [self.ui.r_sim, self.ui.r_sinan, self.ui.r_sinasc]:
            if b.isChecked():

                self.msgbox = Msg()
                self.msgbox.ui.b_memory.clicked.connect(
                    lambda: functions.thread_db_memory(
                        self.ui.b_progress,
                        self.ui.b_generate, self.ui.b_download,
                        self.ui.r_sim, self.ui.r_sinan, self.ui.r_sinasc))

                self.msgbox.ui.b_memory.clicked.connect(
                    lambda: self.msgbox.close())

                self.msgbox.ui.b_filecsv.clicked.connect(
                    lambda: functions.thread_db_file(
                        self.ui.b_progress,
                        self.ui.b_generate, self.ui.b_download,
                        self.ui.r_sim, self.ui.r_sinan, self.ui.r_sinasc))

                self.msgbox.ui.b_filecsv.clicked.connect(
                    lambda: self.msgbox.close())

                self.msgbox.show()

    def get_combo_space(self, val):
        self.ui.combo_selector.clear()
        if val == 0:
            self.ui.combo_selector.addItem('Todas as regiões')
        elif val == 1:
            self.ui.combo_selector.addItems(self.ui.estados)
        elif val == 2:
            self.ui.combo_selector.addItems(self.ui.municipios)

    def get_slader_year(self, val):
        if val == 2009:
            self.ui.label_time.setText('Todos')
        else:
            self.ui.label_time.setText(f'{val}')


class Msg(QtWidgets.QDialog):

    def __init__(self, parent=None):
        QtWidgets.QDialog.__init__(self, parent)

        self.ui = Ui_Dialog()
        self.ui.setupUi(self)


if __name__ == '__main__':
    app = QtWidgets.QApplication(argv)
    main = Main()
    main.show()
    exit(app.exec_())
