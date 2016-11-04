# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'one.ui'
#
# Created by: PyQt5 UI code generator 5.4.2
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_Frame(object):
    def setupUi(self, Frame):
        Frame.setObjectName("Frame")
        Frame.resize(936, 99)
        Frame.setFrameShape(QtWidgets.QFrame.NoFrame)
        Frame.setFrameShadow(QtWidgets.QFrame.Plain)
        self.groupBox = QtWidgets.QGroupBox(Frame)
        self.groupBox.setGeometry(QtCore.QRect(0, 0, 931, 91))
        self.groupBox.setObjectName("groupBox")
        self.layoutWidget = QtWidgets.QWidget(self.groupBox)
        self.layoutWidget.setGeometry(QtCore.QRect(10, 30, 911, 51))
        self.layoutWidget.setObjectName("layoutWidget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.layoutWidget)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.PaperNamelabel = QtWidgets.QLabel(self.layoutWidget)
        self.PaperNamelabel.setObjectName("PaperNamelabel")
        self.horizontalLayout.addWidget(self.PaperNamelabel)
        self.SdateEdit = QtWidgets.QDateEdit(self.layoutWidget)
        self.SdateEdit.setCalendarPopup(True)
        self.SdateEdit.setObjectName("SdateEdit")
        self.horizontalLayout.addWidget(self.SdateEdit)
        self.EdateEdit = QtWidgets.QDateEdit(self.layoutWidget)
        self.EdateEdit.setCalendarPopup(True)
        self.EdateEdit.setObjectName("EdateEdit")
        self.horizontalLayout.addWidget(self.EdateEdit)
        self.label = QtWidgets.QLabel(self.layoutWidget)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        self.BanlineEdit = QtWidgets.QLineEdit(self.layoutWidget)
        self.BanlineEdit.setEnabled(False)
        self.BanlineEdit.setObjectName("BanlineEdit")
        self.horizontalLayout.addWidget(self.BanlineEdit)
        self.progressBar = QtWidgets.QProgressBar(self.layoutWidget)
        self.progressBar.setProperty("value", 0)
        self.progressBar.setObjectName("progressBar")
        self.horizontalLayout.addWidget(self.progressBar)
        self.pushButton = QtWidgets.QPushButton(self.layoutWidget)
        self.pushButton.setObjectName("pushButton")
        self.horizontalLayout.addWidget(self.pushButton)

        self.retranslateUi(Frame)
        QtCore.QMetaObject.connectSlotsByName(Frame)

    def retranslateUi(self, Frame):
        _translate = QtCore.QCoreApplication.translate
        Frame.setWindowTitle(_translate("Frame", "Frame"))
        self.groupBox.setTitle(_translate("Frame", "GroupBox"))
        self.PaperNamelabel.setText(_translate("Frame", "日期"))
        self.label.setText(_translate("Frame", "版次"))
        self.pushButton.setText(_translate("Frame", "下载"))

