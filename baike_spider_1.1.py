#!/usr/bin/python
# -*-coding:utf-8-*-
# Platform:              Linux
# Python:                2.6.6
# Author:                wucl(wucl-20@163.com)
# Program:               使用MySQL建表并存储数据。
# History:               2016.7.21
#                        2016.7.22 加入多线程爬取，提高效率
#                        2016.7.25 读写分离，加入Queue，提高效率


import MySQLdb, os, re, urlparse, urllib2, threading, time
from bs4 import BeautifulSoup
from Queue import Queue


class MYSQL(object):
    def __init__(self):
        self.read_cur = MySQLdb.connect(host="127.0.0.1", user="root", passwd="123456", db='BaiKe', port=3306).cursor()
        self.write_cur = MySQLdb.connect(host="127.0.0.1", user="root", passwd="123456", db='BaiKe', port=3306).cursor()
        self.read_lock = threading.Lock()
        self.write_lock = threading.Lock()


    def add_new_url(self, new_url):
        self.write_lock.acquire()
#        insert_url = [(x) for x in new_url]
        sql =  "insert into  url_list(url) value(%s);"
        self.write_cur.executemany(sql, new_url)
        self.write_lock.release()

    def has_new_url(self):
        self.read_lock.acquire()
        result = self.read_cur.execute("select * from url_list where crawed=0") != 0
        self.read_lock.release()
        return result

    def get_new_urls(self):
        self.read_lock.acquire()
        new_url = []
        if self.read_cur.execute("select url from url_list where crawed=0") == 0:
            print "No new URL Found!\n"
            self.read_lock.release()
            return
        else:
            urls = self.read_cur.fetchmany(30)
            new_url = [ i[0] for i in urls ]
            sql = "update url_list set crawed=1 where url=%s;"
            self.read_cur.executemany(sql, new_url)
            self.read_lock.release()
            return new_url

    def add_data(self, datas):
        self.write_lock.acquire()
        insert_data = []
        for data in datas:
            url_ = data['url'].encode('utf-8')
            title_ = data['title'].encode('utf-8')
            summary_ = data['summary'].encode('utf-8')
            insert_data.append((title_, url_, summary_))
        sql = "insert baike_list(title, url, summary) values(%s, %s, %s)" 
        self.write_cur.executemany(sql, insert_data)
        self.write_lock.release()

class HtmlDownloader(object):
    def download(self, url):
        response = urllib2.urlopen(url)
        if response.getcode() != 200:
            print "downloading Failed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            return None
        return response.read()


class HtmlParser(object):
    def _get_new_urls(self, page_url, soup):
        new_urls = set()
        links = soup.find_all('a', href=re.compile(r"/view/\d+\.htm"))
        for link in links:
            new_url = link['href']
            new_full_url = urlparse.urljoin(page_url, new_url)
            new_urls.add(new_full_url)
        return new_urls

    def _get_new_data(self, page_url, soup):
        res_data = {}
        res_data['url'] = page_url
        title_node = soup.find('dd', class_="lemmaWgt-lemmaTitle-title").find('h1')
        res_data['title'] = title_node.get_text()
        summary_node = soup.find('div', class_="lemma-summary")
        res_data['summary'] = summary_node.get_text()
        return res_data

    def parse(self, page_url, html_cont):
        soup = BeautifulSoup(html_cont, 'html.parser', from_encoding='utf-8')
        new_urls = self._get_new_urls(page_url, soup)
        new_data = self._get_new_data(page_url, soup)
        return new_urls, new_data

class SpiderMain(object):
    def __init__(self):
        self.mysql = MYSQL()
        self.downloader = HtmlDownloader()
        self.parser = HtmlParser()
        self.url_queue = Queue()
        self.data_queue = Queue()
        self.urls = []

    def run(self):
        try:
            new_url = self.urls.pop()
#            print "Crawing " + new_url
            html_cont = self.downloader.download(new_url)
            new_urls, new_data = self.parser.parse(new_url, html_cont)
            for url_ in new_urls:
                self.url_queue.put(url_)
            self.data_queue.put(new_data)
        except Exception, e:
            print str(e)

    def data_process(self):
        url_pool = []
        data_pool = []
        while True:
            time.sleep(0.1)
            try:
                url = self.url_queue.get()
                data = self.data_queue.get()
            except:
                pass
            else:
                url_pool.append(url)
                data_pool.append(data)
            if len(url_pool) > 20:
                self.mysql.add_new_url(url_pool)
                url_pool = []
            if len(data_pool) > 20:
                self.mysql.add_data(data_pool)
                data_pool = []  

    def craw(self, root_url):
        count = 1
        self.mysql.add_new_url(root_url)
        threading.Thread(target=self.data_process).start()
        while True:
            if len(self.urls) < 10:
                self.urls = self.mysql.get_new_urls()
           

            threads = []
            for x in xrange(0, 30):
                threads.append(threading.Thread(target=self.run))
                print "Crawing %d " % count 
                count += 1
            for t in threads:
                t.start()
            for t in threads:
                t.join()


if __name__ == "__main__":
    root_url = ['http://baike.baidu.com/view/21087.htm']
    spider = SpiderMain()
    spider.craw(root_url)
