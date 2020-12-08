#!/usr/bin/env python
# coding: utf-8

import requests
from Logger import logger
from urllib.parse import quote
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Text, DateTime, DECIMAL, Table, MetaData, UniqueConstraint, ForeignKey
from sqlalchemy.dialects.mysql import LONGTEXT
from datetime import datetime
from datetime import date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.mysql import insert
import time
import random
import re
import configparser


class CConfg:
    def __init__(self, name):
        self._name = name
        self._db = ""

        # 读取配置文件
        cfg = configparser.ConfigParser()
        filename = cfg.read(filenames=self._name)
        if not filename:
            raise Exception('配置文件不存在，请检查后重启!')

        self._db = cfg.get('GLOBAL', 'db')

    @property
    def db(self):
        return self._db


cConfg = CConfg('config.ini')
engine = create_engine(cConfg.db, encoding='utf-8', echo=True)
conn = engine.connect()

metadata = MetaData()
t_bid = Table('bid', metadata,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('title', Text()),
              Column('time', DateTime()),
              Column('supplier', Text()),
              Column('agent', Text()),
              Column('area', Text()),
              Column('href', String(255)),
              Column('type', Text()),
              Column('createtime', DateTime()),
              UniqueConstraint('href', 'time', name='idx_href_time')
              )


class Bid(object):
    def __init__(self, title, time, supplier, agent, area, href, type):
        self.title = title
        self.time = time
        self.supplier = supplier
        self.agent = agent
        self.area = area
        self.href = href
        self.type = type
        self.createtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bid_upsert(bid):
    insert_stmt = insert(t_bid).values(
        id=bid.id,
        title=bid.title,
        time=bid.time,
        supplier=bid.supplier,
        agent=bid.agent,
        area=bid.area,
        href=bid.href,
        type=bid.type,
        createtime=bid.createtime)
    # print(insert_stmt)

    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        title=insert_stmt.inserted.title,
        time=insert_stmt.inserted.time,
        supplier=insert_stmt.inserted.supplier,
        agent=insert_stmt.inserted.agent,
        area=insert_stmt.inserted.area,
        type=insert_stmt.inserted.type,
        createtime=insert_stmt.inserted.createtime,
        status='U')
    conn.execute(on_duplicate_key_stmt)


t_bid_content = Table('bid_content', metadata,
                      Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('href', String(255), unique=True),
                      Column('budgetprice', DECIMAL()),
                      Column('highprice', DECIMAL()),
                      Column('winningprice', DECIMAL()),
                      Column('content', LONGTEXT()),
                      Column('createtime', DateTime()),
                      )


class BidContent(object):
    def __init__(self, href, budgetprice, highprice, winningprice, content):
        self.href = href
        self.budgetprice = budgetprice
        self.highprice = highprice
        self.winningprice = winningprice
        self.content = content
        self.createtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def bid_content_upsert(bid_content):
    insert_stmt = insert(t_bid_content).values(
        id=bid_content.id,
        href=bid_content.href,
        budgetprice=bid_content.budgetprice,
        highprice=bid_content.highprice,
        winningprice=bid_content.winningprice,
        content=bid_content.content,
        createtime=bid_content.createtime)
    # print(insert_stmt)

    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        budgetprice=insert_stmt.inserted.budgetprice,
        highprice=insert_stmt.inserted.highprice,
        winningprice=insert_stmt.inserted.winningprice,
        content=insert_stmt.inserted.content,
        createtime=insert_stmt.inserted.createtime,
        status='U')
    conn.execute(on_duplicate_key_stmt)


t_sync_log = Table('sync_log', metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
                   Column('starttime', DateTime(), unique=True),
                   Column('endtime', DateTime()),
                   Column('curpage', Integer),
                   Column('totalpage', Integer),
                   Column('curcount', Integer),
                   Column('totalCount', Integer),
                   Column('createtime', DateTime())
                   # UniqueConstraint('starttime', 'endtime', name='idx_starttime_endtime')
                   )


class SyncLog(object):
    def __init__(self, starttime, endtime, curpage, totalpage, curcount, totalCount):
        self.starttime = starttime
        self.endtime = endtime
        self.curpage = curpage
        self.totalpage = totalpage
        self.curcount = curcount
        self.totalCount = totalCount
        self.createtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sync_log_upsert(sync_log):
    insert_stmt = insert(t_sync_log).values(
        id=sync_log.id,
        starttime=sync_log.starttime,
        endtime=sync_log.endtime,
        curpage=sync_log.curpage,
        totalpage=sync_log.totalpage,
        curcount=sync_log.curcount,
        totalCount=sync_log.totalCount,
        createtime=sync_log.createtime)
    # print(insert_stmt)

    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        endtime=insert_stmt.inserted.endtime,
        curpage=insert_stmt.inserted.curpage,
        totalpage=insert_stmt.inserted.totalpage,
        curcount=insert_stmt.inserted.curcount,
        totalCount=insert_stmt.inserted.totalCount,
        createtime=insert_stmt.inserted.createtime,
        status='U')
    conn.execute(on_duplicate_key_stmt)


mapper(Bid, t_bid)
mapper(BidContent, t_bid_content)
mapper(SyncLog, t_sync_log)
metadata.create_all(engine)


class CcgpSpider:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.cur = 0  # 当前读取记录数
        self.total = 10  # 每页记录数
        self.cur_page = 0  # 当前读取页数
        self.total_page = 1  # 总页数
        self.wait = 10  # 等待时间
        self.timeout = 10  # 超时时间

        self.set_sync_log()

    def set_sync_log(self):
        Session_class = sessionmaker(bind=engine)
        session = Session_class()
        info = session.query(SyncLog).filter(SyncLog.starttime == self.start).all()
        if info:
            self.cur = info[0].curcount
            self.total = info[0].totalCount
            self.cur_page = info[0].curpage
            self.total_page = 88888888  # info[0].totalpage
            logger.info('表有记录，当前读取记录{}/{}，页数{}/{}，继续更新'.format(self.cur, self.total, self.cur_page, self.total_page))
        else:
            logger.info('全新同步')

    def get_href_by_overload_price(self, o_price):
        Session_class = sessionmaker(bind=engine)
        session = Session_class()
        infos = session.query(BidContent).filter(BidContent.budgetprice > o_price).all()
        lihrefs = []
        for info in infos:
            lihrefs.append(info.href)

        infos = session.query(BidContent).filter(BidContent.highprice > o_price).all()
        lihrefs = []
        for info in infos:
            lihrefs.append(info.href)

        infos = session.query(BidContent).filter(BidContent.winningprice > o_price).all()
        lihrefs = []
        for info in infos:
            lihrefs.append(info.href)

        return lihrefs

    def get_href_by_0_price(self):
        Session_class = sessionmaker(bind=engine)
        session = Session_class()
        lihrefs = []
        infos = session.query(BidContent).filter(BidContent.budgetprice == -1, BidContent.highprice == -1,
                                                 BidContent.winningprice == -1).all()
        for info in infos:
            lihrefs.append(info.href)
        return lihrefs

    def get_all(self):
        while self.cur_page < self.total_page:
            self.cur_page += 1
            self.get_one(self.cur_page)
            wait = random.random() * self.wait
            time.sleep(wait)
            logger.info(
                '读取记录{}/{}, 读取页数{}/{} 等待{}秒后读取'.format(self.cur, self.total, self.cur_page, self.total_page, wait))
            sync_log = SyncLog(self.start, self.end, self.cur_page, self.total_page, self.cur, self.total)
            sync_log_upsert(sync_log)
        else:
            logger.info('获取结束')

    def get_one(self, index=1):
        searchtype = 1
        page_index = index
        start_time = quote(self.start.strftime("%Y:%m:%d"))  # quote("2020:11:12")
        end_time = quote(self.end.strftime("%Y:%m:%d"))  # quote("2020:11:19")
        time_type = 6
        display_zone = quote('上海市')
        zone_id = 31

        url = r"http://search.ccgp.gov.cn/bxsearch?searchtype={}&page_index={}&bidSort=&buyerName=&projectId=&pinMu=" \
              r"&bidType=&dbselect=bidx&kw=&start_time={}&end_time={}&timeType={}" \
              r"&displayZone={}&zoneId={}&pppStatus=0&agentName=".format(searchtype, page_index, start_time, end_time,
                                                                         time_type,
                                                                         display_zone if display_zone != None else '',
                                                                         zone_id if zone_id != None else '')
        logger.info('获取查询链接：{}'.format(url))
        headers = {
            'User-Agent': 'Mozilla/5.0(Windows NT 10.0; WOW64)AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/53.0.2785.104 Safari/537.36 Core/1.53.3427.400 QQBrowser/9.6.12513.400',
            'Content-Type': 'application/json'
        }

        try:
            r = requests.get(url, headers=headers, timeout=self.timeout, verify=False)
            r.encoding = 'utf-8'
            r.raise_for_status()
        except requests.RequestException as e:
            logger.error(e)
        else:
            # print(r.text)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                # print(soup.title)
                obj = soup.find('ul', class_="vT-srch-result-list-bid")
                obj2 = obj.select('li > a')
                liobj2 = []
                for item in obj2:
                    # print(item.text.strip())
                    # print(item.get('href'))
                    liobj2.append((item.text.strip(), item.get('href')))
                    pass

                obj3 = obj.select('li > span')
                liobj3 = []
                for item in obj3:
                    itemstr = item.text.strip().split('|')
                    timestr = itemstr[0].strip()
                    man_index = itemstr[1].strip().find("采购人")
                    man = itemstr[1].strip()[man_index + 4:]
                    organ_index = itemstr[2].strip().find("代理公司")
                    organ_index_end = itemstr[2].strip().find(" ")
                    organ = itemstr[2].strip()[organ_index + 6:organ_index_end - 1]
                    area = itemstr[3].strip()
                    type = itemstr[4].strip()
                    timedt = datetime.strptime(timestr, "%Y.%m.%d %H:%M:%S")
                    # print(timedt, man, organ, area, type)
                    liobj3.append((timedt, man, organ, area, type))
                    pass

                # 保存数据总数
                objCond = soup.select('div > div > div > p > span')
                if objCond:
                    liobjCond = []
                    for item in objCond:
                        liobjCond.append(item.text.strip())
                    cond_total = liobjCond[1]
                    cond_start = liobjCond[2]
                    cond_end = liobjCond[3]
                    # print(cond_total, cond_start, cond_end)
                    self.total = int(cond_total)
                else:
                    self.total = 0

                # 保存当前页数
                objpage = soup.find('p', class_="pager")
                if objpage:
                    objpage2 = objpage.select('script')
                    itemstr = objpage2.__str__().strip().split(',')
                    size_index = itemstr[0].strip().find("size:")
                    page = itemstr[0].strip()[size_index + 5:].strip()
                    self.total_page = int(page)
                else:
                    self.total_page = 0

                for i in range(0, len(liobj2)):
                    bid = Bid(title=liobj2[i][0], href=liobj2[i][1], time=liobj3[i][0], supplier=liobj3[i][1],
                              agent=liobj3[i][2], area=liobj3[i][3], type=liobj3[i][4])

                    bid_upsert(bid)

                    # wait = random.random() * self.wait
                    # time.sleep(wait)

                    content, budgetprice, highprice, winningprice = self.get_detail(bid.href)
                    bid_content = BidContent(href=bid.href, budgetprice=budgetprice, highprice=highprice,
                                             winningprice=winningprice, content=content)
                    bid_content_upsert(bid_content)

                    # 保存当前记录
                    self.cur += 1
                    sync_log = SyncLog(self.start, self.end, self.cur_page, self.total_page, self.cur, self.total)
                    sync_log_upsert(sync_log)
                # Session.commit()

    def get_price(self, heads, text):
        text = re.sub(r'<.*?>', '', text)
        # print(text)

        li_head = ['(?<={})'.format(head) for head in heads]
        str_head = "|".join(li_head)
        # print(str_head)

        # str_regexp = r'({})(\s*\d*\.\d*\s*)(.\S*)'.format(str_head)
        # str_regexp = r'({})(\s*\d*(\.\d+)?\s*)(.\S*)'.format(str_head)
        str_regexp = r'({})(\s*\d*(\.\d+)?\s*)(.\S*)'.format(str_head)

        print(str_regexp)
        p = re.compile(str_regexp)
        m = p.search(text)
        # print(m)
        if not m:
            return -1
        logger.info('m={},0={}'.format(m, m.group(0)))
        logger.info('1={},2={},3={},4={}'.format(m.group(1), m.group(2), m.group(3), m.group(4)))

        price = float(m.group(2).strip()) if m.group(2) and m.group(2).strip() else 0.0

        if m.group(4) and not '万' in m.group(4):
            price = price / 10000
        # print(price)
        return price

    def get_detail(self, url):
        logger.info('获取明细链接：{}'.format(url))
        headers = {
            'User-Agent': 'Mozilla/5.0(Windows NT 10.0; WOW64)AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/53.0.2785.104 Safari/537.36 Core/1.53.3427.400 QQBrowser/9.6.12513.400',
            'Content-Type': 'application/json'
        }

        try:
            r = requests.get(url, headers=headers, timeout=self.timeout, verify=False)
            r.encoding = 'utf-8'
            r.raise_for_status()
        except requests.RequestException as e:
            logger.error(e)
        else:
            # print(r.text)
            if r.status_code == 200:
                # 获取内容
                soup = BeautifulSoup(r.text, 'html.parser')
                obj = soup.find('div', class_="vF_detail_content")
                content = obj.__str__()

                budgetprice = highprice = winningprice = 0
                budgetprice = self.get_price(['预算金额：'], r.text)
                highprice = self.get_price(['最高限价（如有）：'], r.text)
                winningprice = self.get_price(['中标（成交）金额：', '中标金额：', '成交金额：'], r.text)

                return content, budgetprice, highprice, winningprice

    def init(self):
        pass

    def insert(self):
        pass


def test_bid_content(spider, href):
    content, budgetprice, highprice, winningprice = spider.get_detail(href)
    bid_content = BidContent(href=href, budgetprice=budgetprice, highprice=highprice, winningprice=winningprice,
                             content=content)
    bid_content_upsert(bid_content)


def test_bid_contents(spider):
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202010/t20201022_15279948.htm') #中标金额：6869027.26
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202010/t20201022_15280128.htm') #预算金额：6700000.00元
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202010/t20201022_15279984.htm') #预算金额：5100000.00元
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202010/t20201021_15276130.htm') #预算金额：11881.0000000 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/zygg/gkzb/202011/t20201112_15415058.htm') #预算金额：1200.0000000 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/zygg/zbgg/202011/t20201120_15469190.htm') #中标（成交）金额：93.8823910（万元）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/zygg/gkzb/202011/t20201109_15392309.htm') #预算金额：817.0000000 万元（人民币）,最高限价（如有）：736.0000000 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200831_14930191.htm') #预算金额：350.0 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200831_14927426.htm') #预算金额：180.0 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202007/t20200710_14627882.htm') #中标金额：9124138.25
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202004/t20200403_14104867.htm') #总中标金额：35.6062 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/cjgg/202004/t20200402_14099526.htm') # 成交金额：16.80万元
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202003/t20200331_14089753.htm') # 总中标金额：44.9134 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202003/t20200330_14082485.htm') # 总中标金额：45.7 万元（人民币）
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202006/t20200618_14501754.htm') # 中标金额：23912800元
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202007/t20200717_14674164.htm') # 预算金额：4872.75 万元（人民币）  最高限价（如有）：2769.3249 万元（人民币）
    test_bid_content(spider,
                     'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202005/t20200513_14286828.htm')  # 中标金额：16278800元  含万达信息
    test_bid_content(spider,
                     'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202006/t20200629_14562999.htm')  # 中标金额：11000000元,  含万达信息
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200819_14865129.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200820_14871972.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200825_14899044.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200818_14856005.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/qtgg/202010/t20201009_15191804.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200824_14888087.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200831_14929396.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200831_14930370.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/jzxcs/202008/t20200827_14912767.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/gkzb/202008/t20200831_14930366.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/jzxcs/202009/t20200901_14934991.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/dfgg/jzxcs/202008/t20200827_14912396.htm')
    # test_bid_content(spider,'http://www.ccgp.gov.cn/cggg/zygg/gzgg/202010/t20201014_15227290.htm')
    # test_bid_content(spider, 'http://www.ccgp.gov.cn/cggg/dfgg/zbgg/202010/t20201022_15279717.htm')
    pass


def test_sync_log(spider):
    spider.set_sync_log()


def test_overload_price_recycle(spider):
    lihrefs = spider.get_href_by_overload_price(10000)
    for href in lihrefs:
        test_bid_content(spider, href)


def test_0_price_recycle(spider):
    lihrefs = spider.get_href_by_0_price()
    print('查询条数：{}'.format(len(lihrefs)))
    for href in lihrefs:
        test_bid_content(spider, href)


if __name__ == '__main__':
    start = date.today()
    end = date.today()

    # start = datetime(2020, 12, 7)
    # end = datetime(2020, 12, 7)

    spider = CcgpSpider(start, end)
    spider.get_all()

    # test_bid_contents(spider)

    # test_sync_log(spider)

    # test_overload_price_recycle(spider)

    # test_0_price_recycle(spider)
