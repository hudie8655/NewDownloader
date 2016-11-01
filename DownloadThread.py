# -*- coding: utf-8 -*-
import pickle
import time
import urllib.request
from testOfflineDB import News

__author__ = 'user'

from PyQt5.QtCore import QThread, pyqtSignal, QCoreApplication, QDate
from bs4 import BeautifulSoup
import re
#import logging

#logging.basicConfig(level=logging.INFO)


class DownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)
    def __init__(self):
        super(DownloadThread,self).__init__()
        #self.moveToThread(self)

        self.starturls = []
        self.helperurls = []
        self.contenturls = []
        #self.startDownload.connect(self.start)

    def start_download(self,sDate,eDate,Ban):
        self.startDate = sDate
        self.endDate = eDate
        self.start()

    def run(self):
        self.gen_starturl()
        self.get_contenturls()
        self.parse_content()


    def gen_starturl(self):
        self.tellSignal.emit('gen')
        self.starturls.clear()

        #logging.info('start date%s', str(self.startDate))
        self.tellSignal.emit(str(self.startDate))
        self.tellSignal.emit(str(self.endDate))
        i = self.startDate
        # TODO 需要设定最后日期为今日，否则此处逻辑会乱
        # 为了减少判断次数
        #self.endDate = self.endDate if self.endDate > QDate.currentDate() else QDate.currentDate()
        while i <= self.endDate:
            #logging.info('start date%s', str(i))
            urls = ['http://paper.people.com.cn/rmrb/html/' + i.toString(
                'yyyy-MM/dd') + '/nbs.D110000renmrb_{:02d}.htm'.format(x) for x in range(1, 5)]
            self.starturls.extend(urls)
            i = i.addDays(1)
        if self.endDate == QDate.currentDate():
        # TODO 不再判断，最多最后一个页面错误抓取几次
            urls = [
            'http://paper.people.com.cn/rmrb/html/' + i.toString('yyyy-MM/dd') + '/nbs.D110000renmrb_{:02d}.htm'.format(
                x) for x in range(5, 25)]
            self.starturls.extend(urls)


    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        self.tellSignal.emit(''.join(self.starturls))
        for url in self.starturls:
            try:
                #logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('nbs.*$')
                for link in bsobj.select('#titleList a'):
                    #logging.info('extract %s' % url)
                    self.contenturls.append(rp.sub(link['href'], url))
            except Exception as e:
                print(e)

    def parse_content(self):
        self.tellSignal.emit('parse_content')
        # i['name'] = response.xpath('//h1/text()').extract()[0]
        # i['ban']=response.xpath('//div[@class="lai"]/text()').extract()[0].split()[4]
        # i['date']=response.xpath('//div[@class="lai"]/text()').extract()[0].split()[3]
        # i['content']=u''.join(response.xpath('//div[@id="articleContent"]/descendant::text()').extract())
        total = len(self.contenturls)
        self.setMaximumSignal.emit(total)
        self.tellSignal.emit(str(total))
        with open('rmdata.bin', 'wb') as f:
            for i, contenturl in enumerate(self.contenturls, 1):
                try:
                    #logging.info('parse %s' % contenturl)
                    html = urllib.request.urlopen(contenturl)
                    bsobj = BeautifulSoup(html,'lxml')
                    title = bsobj.h1.get_text()+bsobj.h2.get_text()+bsobj.h3.get_text()
                    #_, kind, _, date, ban = bsobj.select('div[class="lai"]')[0].get_text().split()[0:5]
                    kind = '人民日报'
                    date = contenturl.split('_')[1]
                    ban = contenturl.split('.')[-2].split('-')[-1]
                    content = bsobj.select('div[id="articleContent"]')[0].get_text()
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except:
                    pass#logging.error('parse %s error' % contenturl)
                finally:
                    self.progressSignal.emit(i)
        self.endDownload.emit()


class GmrbDownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)
    def __init__(self):
        super(GmrbDownloadThread,self).__init__()
        #self.moveToThread(self)

        self.starturls = []
        self.helperurls = []
        self.contenturls = []
        #self.startDownload.connect(self.start)

    def start_download(self,sDate,eDate,Ban):
        self.startDate = sDate
        self.endDate = eDate
        self.start()

    def run(self):
        self.gen_starturl()
        self.get_contenturls()
        self.parse_content()


    def gen_starturl(self):
        self.tellSignal.emit('gen')
        self.starturls.clear()

        #logging.info('start date%s', str(self.startDate))
        self.tellSignal.emit(str(self.startDate))
        self.tellSignal.emit(str(self.endDate))
        i = self.startDate
        # TODO 需要设定最后日期为今日，否则此处逻辑会乱
        # 为了减少判断次数
        #self.endDate = self.endDate if self.endDate > QDate.currentDate() else QDate.currentDate()
        #http://epaper.gmw.cn/gmrb/html/2016-11/01/nbs.D110000gmrb_01.htm
        while i <= self.endDate:
            #logging.info('start date%s', str(i))
            urls = ['http://epaper.gmw.cn/gmrb/html/' + i.toString(
                'yyyy-MM/dd') + '/nbs.D110000gmrb_{:02d}.htm'.format(x) for x in range(1, 17)]
            self.starturls.extend(urls)
            i = i.addDays(1)



    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        self.tellSignal.emit(''.join(self.starturls))
        for url in self.starturls:
            try:
                #logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('nbs.*$')
                for link in bsobj.select('#titleList a'):
                    #logging.info('extract %s' % url)
                    self.contenturls.append(rp.sub(link['href'], url))
            except Exception as e:
                print(e)

    def parse_content(self):
        self.tellSignal.emit('parse_content')
        # i['name'] = response.xpath('//h1/text()').extract()[0]
        # i['ban']=response.xpath('//div[@class="lai"]/text()').extract()[0].split()[4]
        # i['date']=response.xpath('//div[@class="lai"]/text()').extract()[0].split()[3]
        # i['content']=u''.join(response.xpath('//div[@id="articleContent"]/descendant::text()').extract())
        total = len(self.contenturls)
        self.setMaximumSignal.emit(total)
        self.tellSignal.emit(str(total))
        with open('gmdata.bin', 'wb') as f:
            for i, contenturl in enumerate(self.contenturls, 1):
                try:
                    #logging.info('parse %s' % contenturl)
                    html = urllib.request.urlopen(contenturl)
                    bsobj = BeautifulSoup(html,'lxml')
                    title = bsobj.h1.get_text()+bsobj.h2.get_text()+bsobj.h3.get_text()
                    #kind, date, ban = bsobj.select('div[class="lai"] b')[0].get_text().split()[0:2]
                    kind = '光明日报'
                    ban = contenturl.split('/')
                    date = contenturl.split('_')[1]
                    ban = contenturl.split('.')[-2].split('-')[-1]
                    content = bsobj.select('div[id="articleContent"]')[0].get_text()
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except Exception as e:
                    print(e)#logging.error('parse %s error' % contenturl)
                finally:
                    self.progressSignal.emit(i)
        self.endDownload.emit()

if __name__=='__main__':
    qapp=QCoreApplication([])
    w = DownloadThread()
    w.start_download(QDate.currentDate(),QDate.currentDate(),'1')
    qapp.exec()


