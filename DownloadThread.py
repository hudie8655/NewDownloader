# -*- coding: utf-8 -*-
import pickle
import time
from urllib.error import HTTPError,URLError
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
                    date = '-'.join(contenturl.split('/')[5:7])
                    ban = contenturl.split('.')[-2].split('-')[-1]
                    content = '\n'.join([p.get_text() for p in bsobj.select('div[id="articleContent"] p')])
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
                    #ban = contenturl.split('/')
                    date = '-'.join(contenturl.split('/')[5:7])
                    ban = contenturl.split('.')[-2].split('-')[-1]
                    content = '\n'.join([p.get_text() for p in bsobj.select('div[id="articleContent"] p')])
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except Exception as e:
                    print(e)#logging.error('parse %s error' % contenturl)
                finally:
                    self.progressSignal.emit(i)
        self.endDownload.emit()

class JjrbDownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)
    def __init__(self):
        super(JjrbDownloadThread,self).__init__()
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
            urls = ['http://paper.ce.cn/jjrb/html/' + i.toString(
                'yyyy-MM/dd') + '/node_{:d}.htm'.format(x) for x in range(2, 18)]
            self.starturls.extend(urls)
            i = i.addDays(1)



    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        self.tellSignal.emit('\n'.join(self.starturls))
        for url in self.starturls:
            try:
                #logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('node.*$')
                for link in bsobj.select('td.default a[href^="content"]'):
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
                    title = ' '.join([s.get_text() for s in bsobj.select('td.STYLE32 td')[0:3]])
                    #kind, date, ban = bsobj.select('div[class="lai"] b')[0].get_text().split()[0:2]
                    kind = '经济日报'
                    ban = bsobj.find(text=re.compile('(第\d{2}版)：$'))[1:-2]
                    date = '-'.join(contenturl.split('/')[5:7])
                    #ban = contenturl.split('.')[-2].split('-')[-1]
                    content = '\n'.join([p.get_text() for p in bsobj.select('founder-content p')])
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except Exception as e:
                    print(e)#logging.error('parse %s error' % contenturl)
                finally:
                    self.progressSignal.emit(i)
        self.endDownload.emit()

class TjrbDownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)
    def __init__(self):
        super(TjrbDownloadThread,self).__init__()
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

        self.tellSignal.emit(str(self.startDate))
        self.tellSignal.emit(str(self.endDate))
        i = self.startDate
        while i <= self.endDate:
            urls = ['http://epaper.tianjinwe.com/tjrb/tjrb/' + i.toString(
                'yyyy-MM/dd') + '/node_{:d}.htm'.format(x) for x in range(2, 3)]
            self.starturls.extend(urls)
            i = i.addDays(1)



    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        self.tellSignal.emit('\n'.join(self.starturls))
        for url in self.starturls:
            try:
                #logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('node.*$')
                for link in bsobj.select('td.default a[href^="content"]'):
                    #logging.info('extract %s' % url)
                    self.contenturls.append(rp.sub(link['href'], url))
            except Exception as e:
                print(e)

    def parse_content(self):
        self.tellSignal.emit('parse_content')
        total = len(self.contenturls)
        self.setMaximumSignal.emit(total)
        self.tellSignal.emit(str(total))
        with open('gmdata.bin', 'wb') as f:
            for i, contenturl in enumerate(self.contenturls, 1):
                try:
                    html = urllib.request.urlopen(contenturl)
                    bsobj = BeautifulSoup(html,'lxml')
                    title = ' '.join([bsobj.select('.font01')[0].get_text(),bsobj.select('.font02')[0].get_text(),bsobj.select('.font02')[1].get_text()])
                    kind = '天津日报'
                    ban = bsobj.find(text=re.compile('(第\d{2}版)：$'))[1:-2]
                    date = '-'.join(contenturl.split('/')[5:7])
                    #ban = contenturl.split('.')[-2].split('-')[-1]
                    content = '\n'.join([p.get_text() for p in bsobj.select('founder-content p')])
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except Exception as e:
                    print(e)#logging.error('parse %s error' % contenturl)
                finally:
                    self.progressSignal.emit(i)
        self.endDownload.emit()

class BjrbDownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)
    def __init__(self):
        super(BjrbDownloadThread,self).__init__()
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
        #TODO:稍后有时间修改优化其他报纸，index均从首页提取，更为科学准确
        def extract_indexs(url):
            tmp=set([url])
            try:
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('node.*$')
                for link in bsobj.select('a[href^="node_"]'):
                    tmp.add(rp.sub(link['href'], url))
            except Exception as e:
                print(e)
            return list(tmp)

        self.tellSignal.emit('gen')
        self.starturls.clear()

        self.tellSignal.emit(str(self.startDate))
        self.tellSignal.emit(str(self.endDate))
        i = self.startDate
        #北京日报编码规律比较乱，需要从首页获取所有版面地址
        while i <= self.endDate:
            url = 'http://bjrb.bjd.com.cn/html/' + i.toString(
                'yyyy-MM/dd') + '/node_{:d}.htm'.format(1)
            self.starturls.extend(extract_indexs(url))
            i = i.addDays(1)



    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        self.tellSignal.emit('\n'.join(self.starturls))
        for url in self.starturls:
            try:
                #logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('node.*$')
                for link in bsobj.select('div.main-list a[href^="content"]'):
                    self.contenturls.append(rp.sub(link['href'], url))
            except Exception as e:
                print(e)

    def parse_content(self):
        self.tellSignal.emit('parse_content')
        total = len(self.contenturls)
        self.setMaximumSignal.emit(total)
        self.tellSignal.emit(str(total))
        with open('gmdata.bin', 'wb') as f:
            for i, contenturl in enumerate(self.contenturls, 1):
                try:
                    html = urllib.request.urlopen(contenturl)
                    bsobj = BeautifulSoup(html,'lxml')
                    title = bsobj.h1.string.strip()+' '.join(bsobj.select('h2')[0].get_text()+bsobj.select('h2')[1].get_text()).strip()
                    kind = '北京日报'
                    ban = bsobj.select('#list span')[3].get_text().split()[-1]
                    date = '-'.join(contenturl.split('/')[4:6])
                    #ban = contenturl.split('.')[-2].split('-')[-1]
                    content = '\n'.join([p.get_text() for p in bsobj.select('div.text p')])
                    news = News(title, content, kind, date, ban)
                    saved = False
                    pickle.dump(news, f, True)
                except Exception as e:
                    print(e)#logging.error('parse %s error' % contenturl)
                finally:
                    self.progressSignal.emit(i)
        self.endDownload.emit()

class XxsbDownloadThread(QThread):

    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)
    def __init__(self):
        super(XxsbDownloadThread,self).__init__()
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
        #TODO:稍后有时间修改优化其他报纸，index均从首页提取，更为科学准确
        def extract_indexs(url):
            tmp=set([url])
            try:
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                rp = re.compile('node.*$')
                for link in bsobj.select('a[href^="node_"]'):
                    tmp.add(rp.sub(link['href'], url))
            except URLError:
                self.tellSignal.emit(url+'无学习时报报纸')
                return []
            return list(tmp)

        self.tellSignal.emit('gen')
        self.starturls.clear()

        self.tellSignal.emit(str(self.startDate))
        self.tellSignal.emit(str(self.endDate))
        i = self.startDate
        #北京日报编码规律比较乱，需要从首页获取所有版面地址
        while i <= self.endDate:
            url = 'http://dzb.studytimes.cn/shtml/xxsb/' + i.toString(
                'yyyyMMdd/')
            self.starturls.extend(extract_indexs(url))
            i = i.addDays(1)



    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        self.tellSignal.emit('\n'.join(self.starturls))
        for url in self.starturls:
            try:
                #logging.info('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                #rp = re.compile('node.*$')
                for link in bsobj.select('a[href*="vA"]'):
                    self.contenturls.append('http://dzb.studytimes.cn'+link['href'])
            except Exception as e:
                print(e)

    def parse_content(self):
        self.tellSignal.emit('parse_content')
        total = len(self.contenturls)
        self.setMaximumSignal.emit(total)
        self.tellSignal.emit(str(total))
        with open('xxdata.bin', 'wb') as f:
            for i, contenturl in enumerate(self.contenturls, 1):
                try:
                    html = urllib.request.urlopen(contenturl)
                    bsobj = BeautifulSoup(html,'lxml')
                    title = bsobj.h3.string.strip()+' '.join(bsobj.select('h4')[0].get_text()+bsobj.select('h4')[1].get_text()).strip()
                    kind = '学习时报'
                    ban = bsobj.find(text=re.compile('第.*版')).split(' ')[-1].split(':')[0]
                    date = contenturl.split('/')[-2]
                    #ban = contenturl.split('.')[-2].split('-')[-1]
                    content = '\n'.join([p.get_text() for p in bsobj.select('div.text p')])
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


