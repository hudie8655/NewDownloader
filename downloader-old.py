# -*- coding: utf-8 -*-
from PyQt5 import QtWidgets
from PyQt5.QtCore import QDate, QThread, pyqtSignal
from PyQt5.QtWidgets import QDialog
import sys
from offlineDownloader import Ui_Dialog

__author__ = 'user'
import datetime
from testOfflineDB import News
from bs4 import BeautifulSoup
import urllib.request
import re
import logging

logging.basicConfig(level=logging.ERROR)
# TODO why @log cann't use
import zipfile

import pickle

class WorkThread(QThread):
    trigger = pyqtSignal()
    def __int__(self):
        super(WorkThread,self).__init__()

    def run(self,f):
        f()
        self.trigger.emit()         #循环完毕后发出信号

class Downloader(QDialog,Ui_Dialog):
    def __init__(self, parent=None):
        super(QDialog, self).__init__(parent)
        self.setupUi(self)
        self.starturls = []
        self.helperurls = []
        self.contenturls = []

        self.pushButton.clicked.connect(self.start_downloads)
        self.startDateEdit.setDate(QDate.currentDate())
        self.endDateEdit_2.setDate(QDate.currentDate())

        self.progressBar.setValue(0)

    def multi_downloads(self):
        w = WorkThread()
        w.start()

    def start_downloads(self):
        # self.progressBar.setMaximum(10000)
        # for i in range(0,10001):
        #   self.progressBar.setValue(i)
        self.gen_starturl()
        self.get_contenturls()
        self.parse_content()

    def gen_starturl(self):
        self.starturls.clear()
        startDate = self.startDateEdit.date()  # .toString ('yyyy-MM/dd')
        endDate = self.endDateEdit_2.date()  # .toString ('yyyy-MM/dd')
        logging.info('start date%s', str(startDate))
        i = startDate
        # TODO 需要设定最后日期为今日，否则此处逻辑会乱
        # 为了减少判断次数
        while i < endDate:
            logging.info('start date%s', str(i))
            urls = ['http://paper.people.com.cn/rmrb/html/' + i.toString(
                'yyyy-MM/dd') + '/nbs.D110000renmrb_{:02d}.htm'.format(x) for x in range(1, 5)]
            self.starturls.extend(urls)
            i = i.addDays(1)
        # if i == QDate.currentDate():
        # TODO 不再判断，最多最后一个页面错误抓取几次
        urls = [
            'http://paper.people.com.cn/rmrb/html/' + i.toString('yyyy-MM/dd') + '/nbs.D110000renmrb_{:02d}.htm'.format(
                x) for x in range(1, 25)]
        self.starturls.extend(urls)


    def get_contenturls(self):
        for url in self.starturls:
            try:
                logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html)
                rp = re.compile('nbs.*$')
                for link in bsobj.select('#titleList a'):
                    logging.info('extract %s' % url)
                    self.contenturls.append(rp.sub(link['href'], url))
            except:
                continue

    def parse_content(self):
        # i['name'] = response.xpath('//h1/text()').extract()[0]
        # i['ban']=response.xpath('//div[@class="lai"]/text()').extract()[0].split()[4]
        # i['date']=response.xpath('//div[@class="lai"]/text()').extract()[0].split()[3]
        # i['content']=u''.join(response.xpath('//div[@id="articleContent"]/descendant::text()').extract())
        total = len(self.contenturls)
        self.progressBar.setMaximum(total)
        with open('data.bin', 'wb') as f:
            for i, contenturl in enumerate(self.contenturls, 1):
                try:
                    logging.info('parse %s' % contenturl)
                    html = urllib.request.urlopen(contenturl)
                    bsobj = BeautifulSoup(html)
                    title = bsobj.h1.get_text()
                    _, kind, _, date, ban = bsobj.select('div[class="lai"]')[0].get_text().split()[0:5]
                    content = bsobj.select('div[id="articleContent"]')[0].get_text()
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except:
                    logging.error('parse %s error' % contenturl)
                finally:
                    self.progressBar.setValue(i)

    def zip_file(self):
        with zipfile.ZipFile.open('data.zip', compression=zipfile.ZIP_BZIP2) as f:
            f.write('data.bin')


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    win = Downloader()
    win.show()
    sys.exit(app.exec_())
