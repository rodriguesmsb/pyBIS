import sys, threading, time, setSpark
from esqueleto import *
from pydbsus import Datasus


class MyForm(QtWidgets.QDialog):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)

        self.ui.gerar_csv.clicked.connect(self.send_csv)
        self.ui.baixar_dbc.clicked.connect(self.send_dbc)

        self.ui.start_spark.clicked.connect(setSpark.spark_conf)
        self.ui.start_spark.clicked.connect(setSpark.start_spark)
        self.ui.start_spark.clicked.connect(self.emoji)

        self.data = Datasus()

    def loopa_csv(self, *banco):
        n = 0
        while t1.is_alive():
            self.down_state()
            time.sleep(0.5)
            n += 1
            if n >= 99:
                n = 0
            if t1.is_alive() == False:
                n = 100
            self.ui.progressBar.setValue(n)
        for i in banco:
            self.data.write_file(i)
        self.up_state()

    def loopa_dbc(self):
        n = 0
        while t2.is_alive():
            self.down_state()
            time.sleep(0.5)
            n += 1
            if n >= 99:
                n = 0
            if t2.is_alive() == False:
                n = 100
            self.ui.progressBar.setValue(n)
        self.up_state()

    def down_state(self):
        self.ui.gerar_csv.setEnabled(False)
        self.ui.baixar_dbc.setEnabled(False)
        self.ui.r_sim.setEnabled(False)
        self.ui.r_sinan.setEnabled(False)
        self.ui.r_sinasc.setEnabled(False)

    def up_state(self):
        self.ui.gerar_csv.setEnabled(True)
        self.ui.baixar_dbc.setEnabled(True)
        self.ui.r_sim.setEnabled(True)
        self.ui.r_sinan.setEnabled(True)
        self.ui.r_sinasc.setEnabled(True)

    def send_csv(self):
        if self.ui.r_sim.isChecked():
            self.baixar_csv('sim')

        elif self.ui.r_sinan.isChecked():
            self.baixar_csv('sinan')

        elif self.ui.r_sinasc.isChecked():
            self.baixar_csv('sinasc')

    def send_dbc(self):
        if self.ui.r_sim.isChecked():
            self.baixar_dbc()

        elif self.ui.r_sinan.isChecked():
            self.baixar_dbc()

        elif self.ui.r_sinasc.isChecked():
            self.baixar_dbc()

    def baixar_csv(self, banco):
        global t1
        self.data._Datasus__banco = banco
        t1 = threading.Thread(target = self.data.load_files,\
                              daemon = True)
        t1.start()
        threading.Thread(target = self.loopa_csv,\
                         args = (banco,), daemon = True).start()

    def baixar_dbc(self):
        global t2
        t2 = threading.Thread(target = self.data.download, daemon = True)
        t2.start()
        threading.Thread(target = self.loopa_dbc, daemon = True).start()

    def emoji(self):
        pass

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    myform = MyForm()
    myform.show()
    sys.exit(app.exec_())
