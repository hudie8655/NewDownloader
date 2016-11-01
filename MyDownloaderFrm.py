# -*- coding: utf-8 -*-
__author__ = 'user'

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QDialog
from downloader import Ui_Dialog
from SingleDownloaderFrm import SingleDownloaderFrm

settings=[{'name':'人民日报','ban':'1-4'},
          {'name':'光明日报','ban':'1-4'},
          {'name':'经济日报','ban':'1-4'},
          {'name':'天津日报','ban':'1-4'},
          {'name':'北京日报','ban':'1-4'},
          {'name':'学习时报','ban':'1-4'}]

class MainFrmMy(QDialog,Ui_Dialog):
    def __init__(self,parent=None):
        super(QDialog,self).__init__(parent)
        self.setupUi(self)

        self.paperlist=[]
        for s in settings:
            sf =  SingleDownloaderFrm(self)
            self.paperlist.append(sf)
            sf.fill_form(s['name'],s['ban'])
            sf.EndDownloadSignal.connect(self.textEdit.insertPlainText)
            self.verticalLayout.addWidget(sf)

            #set onekeydown
            self.pushButton.clicked.connect(sf.pushButton.click)
        self.pushButton.clicked.connect(self.download)


    def download(self):
        self.pushButton.setEnabled(False)


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    win = MainFrmMy()
    win.show()
    sys.exit(app.exec_())
