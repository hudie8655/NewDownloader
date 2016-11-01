# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'offlineDownloader.ui'
#
# Created by: PyQt5 UI code generator 5.4.2
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(400, 300)
        self.progressBar = QtWidgets.QProgressBar(Dialog)
        self.progressBar.setGeometry(QtCore.QRect(50, 240, 311, 23))
        self.progressBar.setProperty("value", 24)
        self.progressBar.setObjectName("progressBar")
        self.startDateEdit = QtWidgets.QDateEdit(Dialog)
        self.startDateEdit.setGeometry(QtCore.QRect(60, 20, 111, 22))
        self.startDateEdit.setDateTime(QtCore.QDateTime(QtCore.QDate(2016, 1, 1), QtCore.QTime(0, 0, 0)))
        self.startDateEdit.setCalendarPopup(True)
        self.startDateEdit.setObjectName("startDateEdit")
        self.endDateEdit_2 = QtWidgets.QDateEdit(Dialog)
        self.endDateEdit_2.setGeometry(QtCore.QRect(190, 20, 110, 22))
        self.endDateEdit_2.setDateTime(QtCore.QDateTime(QtCore.QDate(2016, 8, 5), QtCore.QTime(0, 0, 0)))
        self.endDateEdit_2.setCalendarPopup(True)
        self.endDateEdit_2.setObjectName("endDateEdit_2")
        self.pushButton = QtWidgets.QPushButton(Dialog)
        self.pushButton.setGeometry(QtCore.QRect(220, 190, 75, 23))
        self.pushButton.setObjectName("pushButton")

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "离线下载器"))
        self.pushButton.setText(_translate("Dialog", "PushButton"))

