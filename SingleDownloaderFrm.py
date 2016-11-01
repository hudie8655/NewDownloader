# -*- coding: utf-8 -*-
from PyQt5.QtWidgets import QFrame
from PyQt5 import QtWidgets
from PyQt5.QtCore import QDate, pyqtSignal
from one import Ui_Frame
from DownloadThread import DownloadThread

__author__ = 'user'


class SingleDownloaderFrm(QFrame,Ui_Frame):
    startDownload = pyqtSignal()
    def __init__(self,parent=None):
        super(QFrame,self).__init__(parent)
        self.setupUi(self)

        #日期默认为今天
        self.SdateEdit.setDate(QDate.currentDate())
        self.EdateEdit.setDate(QDate.currentDate())

        #set pushButton gray after download started
        self.pushButton.clicked.connect(self.download)

    def fill_form(self,paperName,b):
        self.groupBox.setTitle(paperName)
        self.BanlineEdit.setText(b)

    def download(self):
        self.pushButton.setEnabled(False)
        self.t = DownloadThread()
        self.t.progressSignal.connect(self.progressBar.setValue)
        self.t.start_download()

if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    win = SingleDownloaderFrm()
    win.show()
    sys.exit(app.exec_())
