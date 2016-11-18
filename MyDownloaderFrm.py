# -*- coding: utf-8 -*-
import time

__author__ = 'user'

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QDialog
from downloader import Ui_Dialog
from SingleDownloaderFrm import SingleDownloaderFrm
from DownloadThread import WriteThread,workingThreadsCount,mutex


settings=[{'name':'人民日报','class':'Rmrb','ban':'1-4'},
          {'name':'光明日报','class':'Gmrb','ban':'1-4'},
          {'name':'经济日报','class':'Jjrb','ban':'1-4'},
          {'name':'天津日报','class':'Tjrb','ban':'1-4'},
          {'name':'北京日报','class':'Bjrb','ban':'1-4'},
          {'name':'学习时报','class':'Xxsb','ban':'1-4'}]

class MainFrmMy(QDialog,Ui_Dialog):
    def __init__(self,parent=None):
        super(QDialog,self).__init__(parent)
        self.setupUi(self)

        self.paperlist=[]
        for s in settings:
            sf =  SingleDownloaderFrm(self,s['class'])
            self.paperlist.append(sf)
            sf.fill_form(s['name'],s['ban'])
            sf.EndDownloadSignal.connect(self.textEdit.insertPlainText)
            
            self.verticalLayout.addWidget(sf)

            #set onekeydown
            #self.pushButton.clicked.connect(sf.pushButton.click)
            sf.pushButton.clicked.connect(self.__writedb__)
        self.pushButton.clicked.connect(self.download)
        #self.pushButton.clicked.connect(self.__writedb__)


    def download(self):
        self.pushButton.setEnabled(False)
        for sf in self.paperlist:
            sf.download()
            #time.sleep(1)
        self.__writedb__()

    
    #开启单线程写数据库，可通过每个下载或一键下载按钮启动
    #只有一个写线程，如果已经有了，就什么也不做
    def __writedb__(self):
        global workingThreadsCount
        mutex.lock()
        if workingThreadsCount == 0:
            self.w = WriteThread()
            self.w.moveToThread(
                self.w
            )
            #self.w.endWrite.connect(self.pushButton.setEnabled)
            for sf in self.paperlist:
                self.w.endWrite.connect(sf.pushButton.setEnabled)
            #self.w.tellSignal.connect(self.textEdit.insertPlainText)
            self.w.start()
        mutex.unlock()


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    win = MainFrmMy()
    win.show()
    sys.exit(app.exec_())
