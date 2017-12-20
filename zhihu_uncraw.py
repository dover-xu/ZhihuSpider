#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-07-25 17:50:32
# Project: zhihu_uncrawl4

from pyspider.libs.base_handler import *
from pyspider.database.mysql.mysqldb import SQL
import time

baseUrl = "https://www.zhihu.com/"
answers_url_fmt = "https://www.zhihu.com/people/{name}/answers"
followers_url_fmt = "https://www.zhihu.com/api/v4/members/{user_id}/followers?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={offset}&limit=20"
kwargs = { 
    'host':'localhost', 
    'user':'xudong', 
    'passwd':'7222992dong', 
    'db':'zhihu', 
    'charset':'utf8'}
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

    #@every(minutes=24 * 60)
    def on_start(self):
        user_id = Handler.GetUncrawlUserIDFromDB()
        if user_id is None:
            user_id = 'kaifulee'
        self.crawl(followers_url_fmt.format(user_id=user_id, offset="0"), headers=Handler.headers, callback=self.followers_json_parser, allow_redirects=False, validate_cert=False, save={"theLast":user_id})

    #@config(age=10 * 24 * 60 * 60)
    def index_page(self, response): 
        pass
    
    #@config(priority=2)  
    @catch_status_code_error
    def followers_json_parser(self, response):
        if response.error is None:
            theLast = response.save["theLast"]
            paging = response.json["paging"]
            data = response.json["data"]
            is_start = paging["is_start"]
            is_end = paging["is_end"]
            next_page = paging["next"]
            urls = [{"url_token":each["url_token"]} for each in data]
            #t1= time.time()
            if len(urls) > 0:
                sql.insert('uncrawl',urls)
            #print('111111111',time.time()-t1)
            if is_end:
                while True:
                    uncrawl_userid = Handler.GetUncrawlUserIDFromDB()
                    if uncrawl_userid is None:
                        print('Task(Zhihu_uncrawl) Stop when Uncrawl is empty.')
                    elif theLast==uncrawl_userid:
                        time.sleep(2)
                    else:
                        theLast = uncrawl_userid
                        break
                self.crawl(followers_url_fmt.format(user_id=uncrawl_userid, offset="0"), headers=Handler.headers, callback=self.followers_json_parser, allow_redirects=False, validate_cert=False, save={"theLast":theLast})
            else:
                self.crawl(next_page, headers=Handler.headers, callback=self.followers_json_parser, allow_redirects=False, validate_cert=False, save={"theLast":theLast})
        else:
            theLast = None
            while True:
                uncrawl_userid = Handler.GetUncrawlUserIDFromDB()
                if uncrawl_userid is None:
                    print('Task(Zhihu_uncrawl) Stop when Uncrawl is empty.')
                elif theLast==uncrawl_userid:
                    time.sleep(2)
                else:
                    theLast = uncrawl_userid
                    break
            self.crawl(followers_url_fmt.format(user_id=uncrawl_userid, offset="0"), headers=Handler.headers, callback=self.followers_json_parser, allow_redirects=False, validate_cert=False, save={"theLast":theLast})

    @staticmethod   
    def GetUncrawlUserIDFromDB():
        uncrawl = sql.get('uncrawl')
        if isinstance(uncrawl, tuple):
            uncrawl_userid = uncrawl[1]
        else:
            uncrawl_userid = None
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