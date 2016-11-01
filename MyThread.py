# -*- coding: utf-8 -*-
import sys

from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import QDialog, QApplication, QVBoxLayout, QPushButton, QLabel, QProgressBar, QTextEdit
import time

__author__ = 'user'

class MyThread(QThread):
    mysignal = pyqtSignal(str)
    countSignal = pyqtSignal(int)
    def __init__(self):
        super().__init__()
        self.name=''

    def set_name(self,s):
        self.name = s
        self.mysignal.emit(s)
        self.start()

    def run(self):
        for i in range(1,10000):
            self.countSignal.emit(i)
            time.sleep(1)





class myDialog(QDialog):
    def __init__(self, parent=None):
        super(myDialog, self).__init__(parent)
        l = QLabel('hello')
        b = QPushButton('test')
        p = QProgressBar()
        t = QTextEdit()
        mainLayout = QVBoxLayout()
        mainLayout.addWidget(l)
        mainLayout.addWidget(b)
        mainLayout.addWidget(p)
        mainLayout.addWidget(t)
        self.setLayout(mainLayout)

        self.__t = MyThread()
        b.clicked.connect(lambda b:self.__t.set_name(str(b)))
        self.__t.mysignal.connect(l.setText)
        self.__t.countSignal.connect(p.setValue)




if __name__ == '__main__':
    app = QApplication(sys.argv)
    dialog = myDialog()
    dialog.show()
    sys.exit(app.exec_())
