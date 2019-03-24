#-*-coding=utf-8-*-

import os
import re
import sys
import json
import csv
import heapq
from decimal import *
import jieba

import sys
sys.path.append('../../../')
from config import *
from time_utils import *
from global_utils import ESIterator

ABS_PATH = os.path.dirname(os.path.abspath(__file__))

#weibo_data_dict{"day1":[微博数据列表],"day2":[微博数据列表]}
def get_uid_weibo(uid, start_date,end_date):
    weibo_data_dict = {}
    #对每一天进行微博数据获取
    for day in get_datelist_v2(start_date, end_date):
        weibo_data_dict[day] = []
        index_name = "flow_text_" + str(day)
        query_body ={"query": {"bool": {"must":[{"term": {"uid": uid}}]}}}
        sort_dict = {'_id':{'order':'asc'}}
        ESIterator1 = ESIterator(0,sort_dict,1000,index_name,"text",query_body,es_weibo)
        while True:
            try:
                #一千条es数据
                es_result = next(ESIterator1)
                if len(weibo_data_dict[day]):
                    weibo_data_dict[day].extend(es_result)
                else:
                    weibo_data_dict[day] = es_result
                   
            except StopIteration:
                #遇到StopIteration就退出循环
                break
    return weibo_data_dict


def get_user_weibo_type(uid,uid_weibo,start_date,end_date):
    
    if uid_weibo == {}:
        date_list = get_datelist_v2(start_date, end_date)
        for date in date_list:
            timestamp = date2ts(date)
            weibo_dict = {1:0,2:0,3:0}
            query_body = {
                "timestamp":timestamp,
                "date":date,
                "uid":uid,
                "original":weibo_dict[1],
                "retweet":weibo_dict[3],
                "comment":weibo_dict[2],
                }
            es.index(index="user_weibo_type", doc_type="text",id=str(uid)+"_"+str(timestamp) ,body=query_body)
    
    else:
        for k,v in uid_weibo.items():#每一天的微博列表
            timestamp = date2ts(k)
            weibo_dict = {1:0,2:0,3:0}
            if v != []:#某天微博列表是否为空
                for weibo in v:
                    if weibo["_source"]["message_type"] in weibo_dict:
                        weibo_dict[weibo["_source"]["message_type"]] += 1
                    else:
                        pass
            else:
                pass
            
            query_body = {
            "timestamp":timestamp,
            "date":k,
            "uid":uid,
            "original":weibo_dict[1],
            "retweet":weibo_dict[3],
            "comment":weibo_dict[2],
            }
            es.index(index="user_weibo_type", doc_type="text",id=str(uid)+"_"+str(timestamp) ,body=query_body)


if __name__ == '__main__':
    pass
 
    # weibo_data_dict = get_uid_weibo("1804279772", "2016-11-13","2016-11-27")

    # get_user_weibo_type("1804279772",weibo_data_dict,"2016-11-13","2016-11-27")

