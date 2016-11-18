# -*- coding: utf-8 -*-
import logging
import pickle
import time
from urllib.error import HTTPError, URLError
import urllib.request
from testOfflineDB import News
import queue
import sqlite3
from abc import *

__author__ = 'user'

from PyQt5.QtCore import QThread, pyqtSignal, QCoreApplication, QDate, QMutex, QWaitCondition
from bs4 import BeautifulSoup
import re
import traceback
# import logging

# logging.basicConfig(level=logging.INFO)

pipline = queue.Queue()
mutex = QMutex()
workingThreadsCount = 0
workStart = QWaitCondition()


class WriteThread(QThread):
    endWrite = pyqtSignal(bool)
    tellSignal = pyqtSignal(str)

    def __init__(self):
        super(WriteThread, self).__init__()

        self.starturls = []
        self.helperurls = []
        self.contenturls = []
        self.myqueue = pipline

    def run(self):
        global workingThreadsCount

        conn = sqlite3.connect('test.db')
        mutex.lock()
        if self.myqueue.empty():
            self.tellSignal.emit('write thread:wait')
            workStart.wait(mutex)
        mutex.unlock()

        while True:
            newslist = []
            count = 1
            try:
                while count <= 200:
                    newslist.append(self.myqueue.get(timeout=5))
                    count += 1
                x = [(news.title, news.content, news.type, news.date, news.banci) for news in newslist]
                conn.executemany("INSERT INTO NEWS (TITLE,CONTENT,TYPE,DATE,BANCI)   VALUES (?,?,?,?,?)", x)
                conn.commit()
                self.tellSignal.emit('write thread:write 100')
            except queue.Empty:
                if len(newslist) > 0:
                    x = [(news.title, news.content, news.type, news.date, news.banci) for news in newslist]
                    conn.executemany("INSERT INTO NEWS (TITLE,CONTENT,TYPE,DATE,BANCI)   VALUES (?,?,?,?,?)", x)
                    conn.commit()
                    self.tellSignal.emit('write thread:write left')
                mutex.lock()
                if workingThreadsCount == 0:
                    self.endWrite.emit(True)
                    break
                else:
                    pass
                mutex.unlock()

        conn.close()


class Paper():
    @abstractmethod
    def gen_url(self):
        pass
        # self.tellSignal.emit('gen')
        # self.starturls.clear()
        #
        # self.tellSignal.emit(str(self.startDate))
        # self.tellSignal.emit(str(self.endDate))

    @abstractmethod
    def extract_items(self,url, bsobj):
        pass

    def __init__(self,sDate, eDate, Ban):
        self.postion = ''
        self.replace = ''
        self.sDate = sDate
        self.eDate = eDate
        self.Ban = Ban


class DownloadThread(QThread):
    endDownload = pyqtSignal()
    progressSignal = pyqtSignal(int)
    setMaximumSignal = pyqtSignal(int)
    tellSignal = pyqtSignal(str)

    def __init__(self, P):
        super(DownloadThread, self).__init__()


        self.starturls = P.gen_url()
        self.__extract_items_from_page__ = P.extract_items
        self.postion = P.postion
        self.replace = P.replace
        self.contenturls = []
        self.myqueue = pipline

    def run(self):
        global workingThreadsCount

        mutex.lock()
        workingThreadsCount += 1
        mutex.unlock()
        self.get_contenturls()
        self.parse_content()
        mutex.lock()
        workingThreadsCount -= 1
        mutex.unlock()
        self.endDownload.emit()

    def get_contenturls(self):
        self.tellSignal.emit('get_contenturls')
        # self.tellSignal.emit(''.join(self.starturls))
        for url in self.starturls:
            try:
                self.tellSignal.emit('open %s' % url)
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html, 'lxml')
                rp = re.compile(self.replace)  # 'nbs.*$')
                for link in bsobj.select(self.postion):  # '#titleList a'):
                    # logging.info('extract %s' % url)
                    self.contenturls.append(rp.sub(link['href'], url))
            except:
                traceback.print_exc()

    def parse_content(self):
        self.tellSignal.emit('parse_content')
        total = len(self.contenturls)
        self.setMaximumSignal.emit(total)
        self.tellSignal.emit(str(total))
        for i, contenturl in enumerate(self.contenturls, 1):
            try:
                self.tellSignal.emit('parse %s' % contenturl)
                html = urllib.request.urlopen(contenturl)
                bsobj = BeautifulSoup(html, 'lxml')
                news = News(*self.__extract_items_from_page__(contenturl, bsobj))
                self.myqueue.put(news)

                mutex.lock()
                workStart.wakeAll()
                mutex.unlock()
            except:
                traceback.print_exc()
                self.tellSignal.emit('parse %s error' % contenturl)
            finally:
                self.progressSignal.emit(i)





class Rmrb(Paper):
    def __init__(self,sDate, eDate, Ban):
        super(Rmrb,self).__init__(sDate, eDate, Ban)
        self.postion = '#titleList a'
        self.replace = 'nbs.*$'



    def gen_url(self):
        super().gen_url()
        i = self.sDate
        while i < self.eDate:
            logging.info('start date%s', str(i))
            for url in ['http://paper.people.com.cn/rmrb/html/' + i.toString(
                    'yyyy-MM/dd') + '/nbs.D110000renmrb_{:02d}.htm'.format(x) for x in range(1, 5)]:
                yield url
            i = i.addDays(1)
        if self.eDate == QDate.currentDate():
            # TODO 不再判断，最多最后一个页面错误抓取几次
            for url in [
                                'http://paper.people.com.cn/rmrb/html/' + i.toString(
                            'yyyy-MM/dd') + '/nbs.D110000renmrb_{:02d}.htm'.format(
                    x) for x in range(1, 25)]:
                yield url

    def extract_items(self,url, bsobj):
        title = bsobj.h1.get_text() + bsobj.h2.get_text() + bsobj.h3.get_text()
        kind = '人民日报'
        date = '-'.join(url.split('/')[5:7])
        ban = url.split('.')[-2].split('-')[-1]
        content = '\\n'.join([p.get_text() for p in bsobj.select('div[id=\"articleContent\"] p')])
        return title, content, kind, date, ban

class Gmrb(Paper):

    def __init__(self,sDate, eDate, Ban):
        super(Gmrb,self).__init__(sDate, eDate, Ban)
        self.replace= 'nbs.*$'
        self.postion = '#titleList a'
#
    def extract_items(self,url, bsobj):
        title=bsobj.h1.get_text()+bsobj.h2.get_text()+bsobj.h3.get_text()
        kind = '光明日报'
        date = '-'.join(url.split('/')[5:7])
        ban = url.split('.')[-2].split('-')[-1]
        content ='\\n'.join([p.get_text() for p in bsobj.select('div[id=\"articleContent\"] p')])
        return title, content, kind, date, ban
#
    def gen_url(self):
        super().gen_url()
        i = self.sDate
        while i <= self.eDate:
            for url in ['http://epaper.gmw.cn/gmrb/html/' + i.toString(
                'yyyy-MM/dd') + '/nbs.D110000gmrb_{:02d}.htm'.format(x) for x in range(1, 17)]:
                yield  url
            i = i.addDays(1)

class Jjrb(Paper):
    def __init__(self,sDate, eDate, Ban):
        super().__init__(sDate, eDate, Ban)
        self.replace= 'node.*$'
        self.postion = 'td.default a[href^="content"]'

    def gen_url(self):
        super().gen_url()
        i = self.sDate
        while i <= self.eDate:
            #logging.info('start date%s', str(i))
            for url in  ['http://paper.ce.cn/jjrb/html/' + i.toString(
                'yyyy-MM/dd') + '/node_{:d}.htm'.format(x) for x in range(2, 18)]:
                yield  url
            i = i.addDays(1)


    def extract_items(self,contenturl, bsobj):
        title = ' '.join([s.get_text() for s in bsobj.select('td.STYLE32 td')[0:3]])
        kind = '经济日报'
        ban = bsobj.find(text=re.compile('(第\d{2}版)：$'))[1:-2]
        date = '-'.join(contenturl.split('/')[5:7])
        content = '\n'.join([p.get_text() for p in bsobj.select('founder-content p')])
        return title, content, kind, date, ban



class Tjrb(Rmrb):

    def __init__(self,sDate, eDate, Ban):
        super().__init__(sDate, eDate, Ban)
        self.replace= 'node.*$'
        self.postion = 'a[href^="content"]'

    def gen_url(self):
        i = self.sDate
        while i <= self.eDate:
            for url in ['http://epaper.tianjinwe.com/tjrb/tjrb/' + i.toString('yyyy-MM/dd') + '/node_{:d}.htm'.format(x) for x in range(2, 3)]:
                yield  url
            i = i.addDays(1)

    def extract_items(self,contenturl, bsobj):
        title = ' '.join([bsobj.select('.font01')[0].get_text(),bsobj.select('.font02')[0].get_text(),bsobj.select('.font02')[1].get_text()])
        kind = '天津日报'
        ban = bsobj.find(text=re.compile('(第\d{2}版)：$'))[1:-2]
        date = '-'.join(contenturl.split('/')[5:7])
        content = '\n'.join([p.get_text() for p in bsobj.select('founder-content p')])
        return title, content, kind, date, ban

class Bjrb(Paper):
    def __init__(self,sDate, eDate, Ban):
        super().__init__(sDate, eDate, Ban)
        self.replace= 'node.*$'
        self.postion = 'div.main-list a[href^="content"]'

    def gen_url(self):
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

        i = self.sDate
        #北京日报编码规律比较乱，需要从首页获取所有版面地址
        while i <= self.eDate:
            url = 'http://bjrb.bjd.com.cn/html/' + i.toString(
                'yyyy-MM/dd') + '/node_{:d}.htm'.format(1)
            for u in extract_indexs(url):
                yield u
            i = i.addDays(1)

    def extract_items(self,contenturl, bsobj):
        title = bsobj.h1.string.strip()+' '.join(bsobj.select('h2')[0].get_text()+bsobj.select('h2')[1].get_text()).strip()
        kind = '北京日报'
        ban = bsobj.select('#list span')[3].get_text().split()[-1]
        date = '-'.join(contenturl.split('/')[4:6])
        content = '\n'.join([p.get_text() for p in bsobj.select('div.text p')])
        return title, content, kind, date, ban

class Xxsb(Paper):
    def __init__(self,sDate, eDate, Ban):
        super().__init__(sDate, eDate, Ban)
        self.replace= '/shtml/xxsb/\d{8}/v.*shtml'
        self.postion = 'div.pic a[href^="/shtml/xxsb/"]'

    def gen_url(self):
        #TODO:稍后有时间修改优化其他报纸，index均从首页提取，更为科学准确
        #TODO:view-source:http://dzb.studytimes.cn/shtml/xxsb/calendar.shtml re.findall('xxsb/(\d{8})',urlopen)
        def extract_indexs(url):
            tmp=set()
            try:
                html = urllib.request.urlopen(url)
                bsobj = BeautifulSoup(html,'lxml')
                for link in bsobj.select('a[href*="vA"]'):
                    tmp.add('http://dzb.studytimes.cn'+link['href'])
            except URLError:
                return []
            return list(tmp)


        i = self.sDate
        while i <= self.eDate:
            url = 'http://dzb.studytimes.cn/shtml/xxsb/' + i.toString(
                'yyyyMMdd/')
            for u in extract_indexs(url):
                yield u
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
                for link in bsobj.select('div.pic a[href^="/shtml/xxsb/"]'):
                    self.contenturls.append('http://dzb.studytimes.cn'+link['href'])
            except Exception as e:
                print(e)

    def extract_items(self,contenturl, bsobj):
        title = bsobj.select('div.details h3')[0].get_text().strip()+' '.join([x.get_text().strip() for x in bsobj.select('h4')])
        title=re.sub(' ','',title)
        kind = '学习时报'
        ban = bsobj.find(text=re.compile('第.*版')).split(' ')[-1].split('：')[0]
        date = contenturl.split('/')[-2]
        content = '\n'.join(bsobj.select('div#content_div p')[0].get_text().split('\u3000\u3000'))
        return title, content, kind, date, ban

#
#
class Xwcb(Rmrb):

    def __init__(self,sDate, eDate, Ban):
        super().__init__(sDate, eDate, Ban)
        self.replace= '^.*Index.html$'
        self.postion = 'div.btli a'

    def gen_url(self):
        # i = self.sDate
        # while i <= self.eDate:
        #     for url in ['http://epaper.tianjinwe.com/tjrb/tjrb/' + i.toString('yyyy-MM/dd') + '/node_{:d}.htm'.format(x) for x in range(2, 3)]:
        #         yield  url
        #     i = i.addDays(1)
        for url in  ['http://data.chinaxwcb.com/epaper2016/epaper/d{}/Index.html'.format(x) for x in range(6175,6389)]:
            yield  url

    def extract_items(self,contenturl, bsobj):
        title =  ''.join([h.get_text() for h in bsobj.select('div.mainL h1')])
        kind = '新闻出版报'
        ban = re.findall('d(\d*?)b',contenturl)[0]
        date =contenturl.split('/')[-2]
        #TODO:
        # INFO:root:parse http://data.chinaxwcb.com/epaper2016/epaper/d6388/d7b/201611/72949.html
        # encoding error : input conversion failed due to input error, bytes 0xAC 0x6D 0x27 0x20
        # encoding error : input conversion failed due to input error, bytes 0xAC 0x6D 0x27 0x20
        # encoding error : input conversion failed due to input error, bytes 0xAC 0x6D 0x27 0x20
        # INFO:root:parse http://data.chinaxwcb.com/epaper2016/epaper/d6388/d7b/201611/72950.html
        content = '\n'.join([p.get_text() for p in bsobj.select('div.content p')])
        return title, content, kind, date, ban