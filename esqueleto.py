# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'untitled.ui'
#
# Created by: PyQt5 UI code generator 5.14.0
#
# WARNING! All changes made in this file will be lost!


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("PydbSus")
        Dialog.setFixedSize(554,273)
       #Dialog.resize(554, 273)
        self.tabWidget = QtWidgets.QTabWidget(Dialog)
        self.tabWidget.setGeometry(QtCore.QRect(0, 0, 551, 271))
        self.tabWidget.setObjectName("tabWidget")
        self.tab = QtWidgets.QWidget()
        self.tab.setObjectName("tab")
        self.formFrame = QtWidgets.QFrame(self.tab)
        self.formFrame.setGeometry(QtCore.QRect(40, 40, 181, 101))
        self.formFrame.setObjectName("formFrame")
        self.formLayout = QtWidgets.QFormLayout(self.formFrame)
        self.formLayout.setObjectName("formLayout")
        self.r_sim = QtWidgets.QRadioButton(self.formFrame)
        self.r_sim.setObjectName("r_sim")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.r_sim)
        self.r_sinan = QtWidgets.QRadioButton(self.formFrame)
        self.r_sinan.setObjectName("r_sinan")
        self.formLayout.setWidget(1, QtWidgets.QFormLayout.LabelRole, self.r_sinan)
        self.r_sinasc = QtWidgets.QRadioButton(self.formFrame)
        self.r_sinasc.setObjectName("r_sinasc")
        self.formLayout.setWidget(2, QtWidgets.QFormLayout.LabelRole, self.r_sinasc)
        self.formFrame_2 = QtWidgets.QFrame(self.tab)
        self.formFrame_2.setGeometry(QtCore.QRect(360, 40, 181, 101))
        self.formFrame_2.setObjectName("formFrame_2")
        self.formLayout_2 = QtWidgets.QFormLayout(self.formFrame_2)
        self.formLayout_2.setObjectName("formLayout_2")
        self.gerar_csv = QtWidgets.QPushButton(self.formFrame_2)
        self.gerar_csv.setObjectName("gerar_csv")
        self.formLayout_2.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.gerar_csv)
        self.baixar_dbc = QtWidgets.QPushButton(self.formFrame_2)
        self.baixar_dbc.setObjectName("baixar_dbc")
        self.formLayout_2.setWidget(1, QtWidgets.QFormLayout.LabelRole, self.baixar_dbc)
        self.progressBar = QtWidgets.QProgressBar(self.tab)
        self.progressBar.setGeometry(QtCore.QRect(40, 190, 471, 23))
        self.progressBar.setProperty("value", 0)
        self.progressBar.setObjectName("progressBar")
        self.tabWidget.addTab(self.tab, "")
        self.tab_2 = QtWidgets.QWidget()
        self.tab_2.setObjectName("tab_2")
        self.start_spark = QtWidgets.QPushButton(self.tab_2)
        self.start_spark.setGeometry(QtCore.QRect(230, 90, 80, 25))
        self.start_spark.setObjectName("start_spark")
        self.tabWidget.addTab(self.tab_2, "")
        self.tab_3 = QtWidgets.QWidget()
        self.tab_3.setObjectName("tab_3")
        self.tabWidget.addTab(self.tab_3, "")
        self.tab_4 = QtWidgets.QWidget()
        self.tab_4.setObjectName("tab_4")
        self.tabWidget.addTab(self.tab_4, "")
        self.tab_5 = QtWidgets.QWidget()
        self.tab_5.setObjectName("tab_5")
        self.tabWidget.addTab(self.tab_5, "")

        self.retranslateUi(Dialog)
        self.tabWidget.setCurrentIndex(0)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("PydbSus", "PydbSus"))
        self.r_sim.setText(_translate("Dialog", "SIM"))
        self.r_sinan.setText(_translate("Dialog", "SINAN"))
        self.r_sinasc.setText(_translate("Dialog", "SINASC"))
        self.gerar_csv.setText(_translate("Dialog", "Gerar Csv"))
        self.baixar_dbc.setText(_translate("Dialog", "Baixar dbc"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("Dialog", "Funções"))
        self.start_spark.setText(_translate("Dialog", "Start Spark"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("Dialog", "Spark"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_3), _translate("Dialog", "Visualizar"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_4), _translate("Dialog", "Manipular"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_5), _translate("Dialog", "Exportar"))
