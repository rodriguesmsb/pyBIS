# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'pergunta.ui'
#
# Created by: PyQt5 UI code generator 5.14.0
#
# WARNING! All changes made in this file will be lost!


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_pergunta(object):
    def setupUi(self, pergunta):
        pergunta.setObjectName("pergunta")
        pergunta.resize(224, 130)
        self.formLayoutWidget = QtWidgets.QWidget(pergunta)
        self.formLayoutWidget.setGeometry(QtCore.QRect(10, 90, 201, 33))
        self.formLayoutWidget.setObjectName("formLayoutWidget")
        self.formLayout = QtWidgets.QFormLayout(self.formLayoutWidget)
        self.formLayout.setContentsMargins(0, 0, 0, 0)
        self.formLayout.setObjectName("formLayout")
        self.do_arquivo = QtWidgets.QPushButton(self.formLayoutWidget)
        font = QtGui.QFont()
        font.setPointSize(12)
        self.do_arquivo.setFont(font)
        self.do_arquivo.setObjectName("do_arquivo")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.do_arquivo)
        self.da_memoria = QtWidgets.QPushButton(self.formLayoutWidget)
        font = QtGui.QFont()
        font.setPointSize(12)
        self.da_memoria.setFont(font)
        self.da_memoria.setObjectName("da_memoria")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.FieldRole, self.da_memoria)
        self.label = QtWidgets.QLabel(pergunta)
        self.label.setGeometry(QtCore.QRect(10, 10, 201, 61))
        font = QtGui.QFont()
        font.setPointSize(12)
        self.label.setFont(font)
        self.label.setObjectName("label")

        self.retranslateUi(pergunta)
        QtCore.QMetaObject.connectSlotsByName(pergunta)

    def retranslateUi(self, pergunta):
        _translate = QtCore.QCoreApplication.translate
        pergunta.setWindowTitle(_translate("pergunta", "File or Memory"))
        self.do_arquivo.setText(_translate("pergunta", "File"))
        self.da_memoria.setText(_translate("pergunta", "Memory"))
        self.label.setText(_translate("pergunta", "Get the files from where?"))
