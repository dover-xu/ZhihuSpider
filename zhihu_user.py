#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-07-25 19:51:38
# Project: zhihu_users5

from pyspider.libs.base_handler import *
from pyspider.database.mysql.mysqldb import SQL
import time

baseUrl = "https://www.zhihu.com/"
answers_url_fmt = "https://www.zhihu.com/people/{name}/answers"
followers_url_fmt = "https://www.zhihu.com/api/v4/members/{user_id}/followers?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={offset}&limit=20"
kwargs = { 'host':'localhost', 'user':'xudong', 'passwd':'7222992dong', 'db':'zhihu', 'charset':'utf8'}
sql = SQL(**kwargs)

class Handler(BaseHandler):
    
    headers= {
        "Cache-Control":"no-cache",
        "Connection":"keep-alive",
        "Host":"www.zhihu.com",
        "User-Agent":"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.89 Safari/537.36",
        "authorization":"oauth c3cef7c66a1843f8b3a9e6a1e3160e20"
    }

    crawl_config = {
        "headers" : headers,
        "timeout" : 5000
    }
    
    def __init__(self):
        pass
        #self.sql.cleartb('users')
        #self.sql.cleartb('uncrawl')
        #self.sql.cleartb('crawled')

    #@every(minutes=24 * 60)
    def on_start(self):
        uncrawl_userid = Handler.PopUncrawlUserIDFromDB()
        if uncrawl_userid:
            self.crawl(answers_url_fmt.format(name=uncrawl_userid), headers=Handler.headers, callback=self.index_page, allow_redirects=False, validate_cert=False)
        else:
            print('Task(Zhihu_users) Stop when Uncrawl is empty.')

    #@config(age=10 * 24 * 60 * 60)
    @catch_status_code_error
    def index_page(self, response):
        if response.error is None:
            print(response.url)
            user_id = response.url.split('/')[4]
            sql.insert('crawled', {"url_token": user_id})
            user_name = response.doc('div.ProfileHeader-contentHead > h1 > span.ProfileHeader-name').text().strip()
            try:
                answer_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(2) > a > span').text().strip())
                question_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(3) > a > span').text().strip())
                article_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(4) > a > span').text().strip())
                collection_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(5) > a > span').text().strip())
            except Exception:
                answer_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(2) > li > span > span').text().strip())
                question_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(3) > li > span > span').text().strip())
                article_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(4) > li > span > span').text().strip())
                collection_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(5) > li > span > span').text().strip())

            uncrawl_userid = Handler.PopUncrawlUserIDFromDB()
            if uncrawl_userid:
                self.crawl(answers_url_fmt.format(name=uncrawl_userid), headers=Handler.headers, callback=self.index_page, allow_redirects=False, validate_cert=False)
            else:
                print('Task(Zhihu_users) Stop when Uncrawl is empty.')

            return {
                "url_token":user_id,
                "user_name":user_name,
                "answer_num":answer_num,
                "question_num":question_num,
                "article_num":article_num,
                "collection_num":collection_num
                }
        else:
            uncrawl_userid = Handler.PopUncrawlUserIDFromDB()
            if uncrawl_userid:
                self.crawl(answers_url_fmt.format(name=uncrawl_userid), headers=Handler.headers, callback=self.index_page, allow_redirects=False, validate_cert=False)
            else:
                print('Task(Zhihu_users) Stop when Uncrawl is empty.')
                
    @staticmethod   
    def PopUncrawlUserIDFromDB():
        count = 0
        uncrawl_url = None
        while True:
            count += 1
            if count > 100000000:
                break
            uncrawl = sql.pop('uncrawl')
            if isinstance(uncrawl, tuple):
                uncrawl_userid = uncrawl[1]
            else:
                uncrawl_userid = None
            if not sql.contain('crawled', 'url_token', uncrawl_userid):
                break
        if count > 100000000:
            return None
        else:
            return uncrawl_userid
                    
    def IsUsers(self, result):
        if isinstance(result, dict) and "user_name" in result and "answer_num" in result and "question_num" in result and "article_num" in result and "collection_num" in result:
            return True
        
    def on_result(self, result):
        #print('result',result)
        if not result:
            return
        if self.IsUsers(result):
            sql.insert('users',result)