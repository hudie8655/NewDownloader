# -*- coding: utf-8 -*-
import time

__author__ = 'user'

from PyQt5.QtCore import QThread, pyqtSignal


class DownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    def __init__(self):
        super().__init__()
        #self.startDownload.connect(self.start)

    def start_download(self):
        self.start()

    def run(self):
        for i in range(1,10000):
            self.progressSignal.emit(i)
            time.sleep(1)



