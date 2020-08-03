# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'pessoal.ui'
#
# Created by: PyQt5 UI code generator 5.14.0
#
# WARNING! All changes made in this file will be lost!

from os import path
import re
import pandas as pd
import platform, os, multiprocessing
from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(442, 319)
        MainWindow.adjustSize()
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(MainWindow.sizePolicy().hasHeightForWidth())
        MainWindow.setSizePolicy(sizePolicy)
        font = QtGui.QFont()
        font.setPointSize(12)
        MainWindow.setFont(font)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.abas = QtWidgets.QTabWidget(self.centralwidget)
        self.abas.setGeometry(QtCore.QRect(0, 0, 441, 291))
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.abas.sizePolicy().hasHeightForWidth())
        self.abas.setSizePolicy(sizePolicy)
        font = QtGui.QFont()
        font.setPointSize(12)
        self.abas.setFont(font)
        self.abas.setObjectName("abas")
        self.tab = QtWidgets.QWidget()
        self.tab.setObjectName("tab")
        self.verticalLayoutWidget = QtWidgets.QWidget(self.tab)
        self.verticalLayoutWidget.setGeometry(QtCore.QRect(80, 30, 150, 101))
        self.verticalLayoutWidget.setObjectName("verticalLayoutWidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.verticalLayoutWidget)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout.setObjectName("verticalLayout")
        self.r_sim = QtWidgets.QRadioButton(self.verticalLayoutWidget)
        self.r_sim.setObjectName("r_sim")
        self.verticalLayout.addWidget(self.r_sim)
        self.r_sinan = QtWidgets.QRadioButton(self.verticalLayoutWidget)
        self.r_sinan.setObjectName("r_sinan")
        self.verticalLayout.addWidget(self.r_sinan)
        self.r_sinasc = QtWidgets.QRadioButton(self.verticalLayoutWidget)
        self.r_sinasc.setObjectName("r_sinasc")
        self.verticalLayout.addWidget(self.r_sinasc)
        self.verticalLayoutWidget_2 = QtWidgets.QWidget(self.tab)
        self.verticalLayoutWidget_2.setGeometry(QtCore.QRect(580, 30, 160, 80))
        self.verticalLayoutWidget_2.setObjectName("verticalLayoutWidget_2")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget_2)
        self.verticalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.b_generate = QtWidgets.QPushButton(self.verticalLayoutWidget_2)
        self.b_generate.setObjectName("b_generate")
        self.verticalLayout_2.addWidget(self.b_generate)
        self.b_download = QtWidgets.QPushButton(self.verticalLayoutWidget_2)
        self.b_download.setObjectName("b_download")
        self.verticalLayout_2.addWidget(self.b_download)
        self.b_progress = QtWidgets.QProgressBar(self.tab)
        self.b_progress.setGeometry(QtCore.QRect(120, 190, 580, 31))
        self.b_progress.setProperty("value", 0)
        self.b_progress.setObjectName("b_progress")
        self.p_data = QtWidgets.QLabel(self.tab)
        self.p_data.setGeometry(QtCore.QRect(40, 150, 181, 31))
        self.p_data.setText("")
        self.p_data.setObjectName("p_data")
        self.abas.addTab(self.tab, "")
        self.tab_2 = QtWidgets.QWidget()
        self.tab_2.setObjectName("tab_2")
        self.spark = QtWidgets.QPushButton(self.tab_2)
        self.spark.setGeometry(QtCore.QRect(50, 20, 191, 31))
        self.spark.setObjectName("spark")
        self.verticalLayoutWidget_3 = QtWidgets.QWidget(self.tab_2)
        self.verticalLayoutWidget_3.setGeometry(QtCore.QRect(50, 80, 194, 71))
        self.verticalLayoutWidget_3.setObjectName("verticalLayoutWidget_3")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget_3)
        self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.label = QtWidgets.QLabel(self.verticalLayoutWidget_3)
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.setObjectName("label")
        self.verticalLayout_3.addWidget(self.label)
        self.spin_proc = QtWidgets.QSpinBox(self.verticalLayoutWidget_3)
        self.spin_proc.setObjectName("spin_proc")
        self.spin_proc.setMinimum(1)
        self.spin_proc.setMaximum(multiprocessing.cpu_count())
        self.verticalLayout_3.addWidget(self.spin_proc)
        self.verticalLayoutWidget_4 = QtWidgets.QWidget(self.tab_2)
        self.verticalLayoutWidget_4.setGeometry(QtCore.QRect(50, 170, 195, 71))
        self.verticalLayoutWidget_4.setObjectName("verticalLayoutWidget_4")
        self.verticalLayout_4 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget_4)
        self.verticalLayout_4.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.label_2 = QtWidgets.QLabel(self.verticalLayoutWidget_4)
        self.label_2.setAlignment(QtCore.Qt.AlignCenter)
        self.label_2.setObjectName("label_2")
        self.verticalLayout_4.addWidget(self.label_2)
        self.spin_mem = QtWidgets.QSpinBox(self.verticalLayoutWidget_4)
        self.spin_mem.setObjectName("spin_mem")
        self.spin_mem.setMinimum(2)
        self.spin_mem.setMaximum(20)
        self.verticalLayout_4.addWidget(self.spin_mem)

        self.comboBox = QtWidgets.QComboBox(self.tab_2)
        self.comboBox.setGeometry(QtCore.QRect(520, 20, 200, 31))
        self.comboBox.setEditable(True)
        self.comboBox.setObjectName("comboBox")

        if platform.system().lower() == 'linux':
            self.db = []
            try:
                for i in os.listdir(os.path.expanduser('~/Documentos/files_db/')):
                    for x in os.listdir(os.path.expanduser(
                        '~/Documentos/files_db/'+i+'/')):
                        if x.endswith(('.dbc','.DBC','.dbf','.DBF')):
                            if bool(re.search(r'DNBR[0-9]', x)):
                                self.db.append(os.path.expanduser(
                                    '~/Documentos/files_db/'+i+'/'+x))
                                self.comboBox.addItem(os.path.expanduser('~/Documentos/files_db/'+i+'/'+x).split('/')[5]+'/'+x)
            except FileNotFoundError:
                os.mkdir(os.path.expanduser('~/Documentos/files_db/'))


        elif platform.system().lower() == 'windows':
            db = []
            for i in os.listdir(os.path.expanduser('~\\Documentos/files_db\\')):
                for x in os.listdir(os.path.expanduser('~\\Documentos\\files_db\\'+i+'\\')):
                    if x.endswith(('.dbc','.dbf','.DBC','.DBF')):
                        if bool(re.search(r'DNBR[0-9]', x)):
                            db.append(os.path.expanduser('~\\Documentos\\files_db\\'+i+'\\'+x))
            self.comboBox.addItems(db)

        self.verticalLayoutWidget_5 = QtWidgets.QWidget(self.tab_2)
        self.verticalLayoutWidget_5.setGeometry(QtCore.QRect(520, 71, 199, 120))
        self.verticalLayoutWidget_5.setObjectName("verticalLayoutWidget_5")
        self.verticalLayout_5 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget_5)
        self.verticalLayout_5.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_5.setObjectName("verticalLayout_5")
        self.label_3 = QtWidgets.QLabel(self.verticalLayoutWidget_5)
        self.label_3.setAlignment(QtCore.Qt.AlignCenter)
        self.label_3.setObjectName("label_3")
        self.verticalLayout_5.addWidget(self.label_3)
        self.horizontalSlider = QtWidgets.QSlider(self.verticalLayoutWidget_5)
        self.horizontalSlider.setMinimum(2010)
        self.horizontalSlider.setMaximum(2017)
        self.horizontalSlider.setPageStep(1)
        self.horizontalSlider.setOrientation(QtCore.Qt.Horizontal)
        self.horizontalSlider.setTickInterval(1)
        self.horizontalSlider.setObjectName("horizontalSlider")
        self.verticalLayout_5.addWidget(self.horizontalSlider)
        self.formLayout = QtWidgets.QFormLayout()
        self.formLayout.setObjectName("formLayout")
        self.label_4 = QtWidgets.QLabel(self.verticalLayoutWidget_5)
        self.label_4.setObjectName("label_4")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.label_4)
        self.label_5 = QtWidgets.QLabel(self.verticalLayoutWidget_5)
        self.label_5.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignTrailing|QtCore.Qt.AlignVCenter)
        self.label_5.setObjectName("label_5")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.FieldRole, self.label_5)
        self.verticalLayout_5.addLayout(self.formLayout)
        self.b_readdata = QtWidgets.QPushButton(self.tab_2)
        self.b_readdata.setGeometry(QtCore.QRect(520, 209, 199, 31))
        self.b_readdata.setObjectName("b_readdata")
        self.abas.addTab(self.tab_2, "")
        self.tab_3 = QtWidgets.QWidget()
        self.tab_3.setObjectName("tab_3")
        self.abas.addTab(self.tab_3, "")
        self.tab_4 = QtWidgets.QWidget()
        self.tab_4.setObjectName("tab_4")
        self.abas.addTab(self.tab_4, "")
        self.tab_5 = QtWidgets.QWidget()
        self.tab_5.setObjectName("tab_5")
        self.abas.addTab(self.tab_5, "")
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        self.abas.setCurrentIndex(1)
        self.horizontalSlider.valueChanged['int'].connect(self.label_5.setNum)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.label_year = QtWidgets.QLabel(self.tab_3)
        self.label_year.setGeometry(50, 30, 100, 30)
        self.label_year.setText('Year:')

        self.qslader_year = QtWidgets.QSlider(self.tab_3)
        self.qslader_year.setOrientation(QtCore.Qt.Horizontal)
        self.qslader_year.setTickInterval(1)
        self.qslader_year.setPageStep(1)
        self.qslader_year.setMinimum(2010)
        self.qslader_year.setMaximum(2017)
        self.qslader_year.setGeometry(QtCore.QRect(130, 40, 160, 20))

        self.label_space = QtWidgets.QLabel(self.tab_3)
        self.label_space.setGeometry(QtCore.QRect(50, 80, 100, 30))
        self.label_space.setText('Space: ')

        self.combo_space = QtWidgets.QComboBox(self.tab_3)
        self.combo_space.setGeometry(130, 80, 160, 30)
        self.combo_space.addItems(['Brasil', 'Estado', 'Municipio'])


    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "pydbGUI"))
        self.r_sim.setText(_translate("MainWindow", "SIM"))
        self.r_sinan.setText(_translate("MainWindow", "SINAN"))
        self.r_sinasc.setText(_translate("MainWindow", "SINASC"))
        self.b_generate.setText(_translate("MainWindow", "GENERATE CSV"))
        self.b_download.setText(_translate("MainWindow", "DOWNLOAD DB"))
        self.abas.setTabText(self.abas.indexOf(self.tab), _translate("MainWindow", "Downloads"))
        self.spark.setText(_translate("MainWindow", "START SPARK"))
        self.label.setText(_translate("MainWindow", "SET Nº PROCESSORS"))
        self.label_2.setText(_translate("MainWindow", "SET MEMORY IN GB"))
        self.comboBox.setCurrentText(_translate("MainWindow", "SELECT DATA"))
        self.comboBox.setItemText(-1, _translate("MainWindow", "SELECT DATA"))
        self.label_3.setText(_translate("MainWindow", "SELECT RANGE DATA TIME"))
        self.label_4.setText(_translate("MainWindow", "2010"))
        self.label_5.setText(_translate("MainWindow", "2010"))
        self.b_readdata.setText(_translate("MainWindow", "READ DATA"))
        self.abas.setTabText(self.abas.indexOf(self.tab_2), _translate("MainWindow", "Spark"))
        self.abas.setTabText(self.abas.indexOf(self.tab_3), _translate("MainWindow", "Manipulate"))
        self.abas.setTabText(self.abas.indexOf(self.tab_4), _translate("MainWindow", "Preview"))
        self.abas.setTabText(self.abas.indexOf(self.tab_5), _translate("MainWindow", "Export"))
