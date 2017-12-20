#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-07-26 20:43:11
# Project: zhihu1

from pyspider.libs.base_handler import *
from pyspider.database.mysql.mysqldb import SQL
from pybloom import BloomFilter
import time
import json
import re

baseUrl = "https://www.zhihu.com/"
answers_url_fmt = "https://www.zhihu.com/people/{name}/answers"
followers_url_fmt = "https://www.zhihu.com/api/v4/members/{user_id}/followers?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={offset}&limit=20"
kwargs = { 'host':'localhost', 'user':'xudong', 'passwd':'7222992dong', 'db':'zhihu', 'charset':'utf8'}

sql = SQL(**kwargs)
#crawled = BloomFilter(10000000, 0.0001)


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

    #@every(minutes=24 * 60)
    def on_start(self):
        user_id = 'kaifulee'
        self.crawl(answers_url_fmt.format(name=user_id), headers=Handler.headers, callback=self.index_page, allow_redirects=False, validate_cert=False)
        

    @config(age=10 * 24 * 60 * 60)
    @catch_status_code_error
    def index_page(self, response):
        if not response.error:
            user_id = response.url.split('/')[4]
            
            # add

            self.crawl(followers_url_fmt.format(user_id=user_id, offset="0"), headers=Handler.headers, callback=self.followers_json_parser, allow_redirects=False, validate_cert=False)

            user_name = response.doc('div.ProfileHeader-contentHead > h1 > span.ProfileHeader-name').text().strip()
            try:
                answer_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(2) > a > span').text().strip())
                question_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(3) > a > span').text().strip())
                article_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(4) > a > span').text().strip())
                collection_num = int(response.doc('div.ProfileMain-header > ul > li:nth-child(5) > a > span').text().strip())
            except Exception:
                try:
                    answer_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(2) > li > span > span').text().strip())
                    question_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(3) > li > span > span').text().strip())
                    article_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(4) > li > span > span').text().strip())
                    collection_num = int(response.doc('div.ProfileMain-header > ul > div:nth-child(5) > li > span > span').text().strip())
                except:
                    return
                    
            try:    
                following_num = int(response.doc('div[class="NumberBoard FollowshipCard-counts"] > a:nth-child(1) > div.NumberBoard-value').text().strip())
                follower_num = int(response.doc('div[class="NumberBoard FollowshipCard-counts"] > a:nth-child(3) > div.NumberBoard-value').text().strip())
            except:
                following_num = 0
                follower_num = 0
            
            try:
                marked_answer_num = int(re.search('知乎收录.(\d+).个回答', response.content).group(1).strip())
            except:
                marked_answer_num = 0

            try:
                acquire_agree_num = int(re.search('获得.(\d+).次赞同', response.content).group(1).strip())
            except:
                acquire_agree_num = 0
                
            try:
                acquire_grateful_num, acquire_collection_num = re.search('获得.(\d+).次感谢，(\d+).次收藏', response.content).groups()
                acquire_grateful_num = int(acquire_grateful_num.strip())
                acquire_collection_num = int(acquire_collection_num.strip())
            except:
                acquire_grateful_num = 0
                acquire_collection_num = 0
                
            try:
                common_edit_num = int(re.search('参与.(\d+).次公共编辑', response.content).group(1))
            except:
                common_edit_num = 0


            return {
                "url_token":user_id,
                "user_name":user_name,
                "answer_num":answer_num,
                "question_num":question_num,
                "article_num":article_num,
                "collection_num":collection_num,
                "following_num":following_num,
                "follower_num":follower_num,
                "marked_answer_num":marked_answer_num,
                "acquire_agree_num":acquire_agree_num,
                "acquire_grateful_num":acquire_grateful_num,
                "acquire_collection_num":acquire_collection_num,
                "common_edit_num":common_edit_num,
                }
        else:
            print(response.error)
    
    @config(age=10 * 60)
    @catch_status_code_error
    def followers_json_parser(self, response):
        if not response.error:
            paging = response.json["paging"]
            data = response.json["data"]
            is_start = paging["is_start"]
            is_end = paging["is_end"]
            next_page = paging["next"]
            
            url_tokens = [each["url_token"] for each in data]
            if len(url_tokens) > 0:
                #tk_filter = set(url_tokens).difference(crawled)
                # filter
                for tk in url_tokens:
                    self.crawl(answers_url_fmt.format(name=tk), headers=Handler.headers, callback=self.index_page, allow_redirects=False, validate_cert=False)

            if not is_end:
                self.crawl(next_page, headers=Handler.headers, callback=self.followers_json_parser, allow_redirects=False, validate_cert=False)
                
    @staticmethod   
    def PopUncrawlUserIDFromDB():
        uncrawl = sql.pop('uncrawl')
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