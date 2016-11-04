# -*- coding: utf-8 -*-
from PyQt5.QtWidgets import QFrame
from PyQt5 import QtWidgets
from PyQt5.QtCore import QDate, pyqtSignal
from one import Ui_Frame
from DownloadThread import *
import logging
__author__ = 'user'


class SingleDownloaderFrm(QFrame,Ui_Frame):
    sendSettingSignal = pyqtSignal(QDate,QDate,str)
    EndDownloadSignal = pyqtSignal(str)
    def __init__(self,parent=None,classname='DownloadThread()'):
        super(QFrame,self).__init__(parent)
        self.setupUi(self)
        self._class_name = classname

        #日期默认为今天
        self.SdateEdit.setDate(QDate.currentDate())
        self.EdateEdit.setDate(QDate.currentDate())



        #set pushButton gray after download started
        self.pushButton.clicked.connect(self.download)

    def fill_form(self,paperName,b):
        self.groupBox.setTitle(paperName)
        self.BanlineEdit.setText(b)

    def logmsg(self,s):
        logging.info(s)
        #TODO:借用一下，向主窗口传消息。命名有错误
        #self.EndDownloadSignal.emit(s)



    def download(self):
        self.pushButton.setEnabled(False)
        self.t = eval(self._class_name)#DownloadThread()
        self.t.moveToThread(self.t)
        self.t.progressSignal.connect(self.progressBar.setValue)
        self.t.setMaximumSignal.connect(self.progressBar.setMaximum)
        self.t.tellSignal.connect(self.logmsg)
        self.t.endDownload.connect(self.end_download)
        #self.sendSettingSignal.connect(self.t.get_settings)
        #self.sendSettingSignal.emit(self.SdateEdit.date(),self.EdateEdit.date(),self.BanlineEdit.text())
        self.t.start_download(self.SdateEdit.date(),self.EdateEdit.date(),self.BanlineEdit.text())

    def end_download(self):
        #self.pushButton.setEnabled(True)
        self.EndDownloadSignal.emit(self.groupBox.title()+'下载完成.共下载%d条'%self.progressBar.value())

if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    win = SingleDownloaderFrm()
    win.show()
    sys.exit(app.exec_())
