# -*- coding: UTF-8 -*-


import os
import time
import numpy as np
import math
import json
import sys
import re

sys.path.append('../../../')
from config import *
from time_utils import *
from global_utils import *




def judge_user_weibo_rel_exist(uid):#判断此条微博用户 微博关系信息是否存在
    try:#判断此条微博用户 微博关系信息是否存在 
        weibo_rel_exist = es.get(index="user_weibo_relation_info", doc_type="text",id = uid)#ruocunzai
        return True
    except:
        es.index(index = "user_weibo_relation_info",doc_type="text",id = uid,body = {"uid":uid,"fans_num":0,"friends_num":0,\
  "origin_weibo":[],"origin_weibo_be_comment":[],"origin_weibo_be_retweet":[],"comment_weibo":[],\
  "retweeted_weibo":[],"retweeted_weibo_be_retweet":[],"retweeted_weibo_be_comment":[]})
        return True

def get_queue_index(timestamp):
    time_struc = time.gmtime(float(timestamp))
    hour = time_struc.tm_hour
    minute = time_struc.tm_min
    index = hour * 4 + math.ceil(minute / 15.0)   # Every 15 minutes
    return int(index)

def cal_user_weibo_relation_info(item):
    try:
        uid = int(item['uid'])#用户uid
        print(item['uid'],ts2date(item['timestamp']))
    except:
        return
    if judge_user_weibo_rel_exist(uid):#此条微博用户已经具有表 拿出此条数据
        uid_es_result =es.get(index="user_weibo_relation_info", doc_type="text",id = uid)["_source"]
        fans_num = item['user_fansnum']
        #friends_count = item.get("user_friendsnum", 0)关注数没有
        es.update(index="user_weibo_relation_info", doc_type="text",id =uid,body= {"doc":{"fans_num":fans_num}})
        retweeted_uid = item['root_uid']
        retweeted_mid = item['root_mid']

        message_type = int(item['message_type'])
        mid = item['mid']
        timestamp = item['timestamp']
        text = item['text']

        if message_type == 1:
            origin_weibo_dict = {}
            origin_weibo_dict["mid"] = mid
            origin_weibo_dict["retweet_num"] = 0
            origin_weibo_dict["comment_num"] = 0
            origin_weibo_dict["timestamp"] = timestamp
            uid_es_result["origin_weibo"].append(origin_weibo_dict)
            es.update(index="user_weibo_relation_info", doc_type="text",id =uid,body= {"doc":{"origin_weibo":uid_es_result["origin_weibo"]}})
 
            
        elif message_type == 2: # comment weibo 评论微博
            
            #if cluster_redis.sismember(user + '_comment_weibo', retweeted_mid):
                #return 
            if retweeted_mid in uid_es_result["comment_weibo"]:#此条微博已经被记录过 pass
                return
            if retweeted_uid =="":
                return 
            uid_es_result["comment_weibo"].append(retweeted_mid)
            es.update(index="user_weibo_relation_info", doc_type="text",id =uid,body= {"doc":{"comment_weibo":uid_es_result["comment_weibo"]}})
            #cluster_redis.sadd(user + '_comment_weibo', retweeted_mid)
            RE = re.compile(u'//@([a-zA-Z-_⺀-⺙⺛-⻳⼀-⿕々〇〡-〩〸-〺〻㐀-䶵一-鿃豈-鶴侮-頻並-龎]+):', re.UNICODE)
            nicknames = RE.findall(text)
            queue_index = get_queue_index(timestamp)
            #cluster_redis.hincrby(user, 'comment_weibo', 1) len(uid_es_result["comment_weibo"])

            #if 1:
            if len(nicknames) == 0:#评论的为他人原创 需更新root_uid的信息 
                if judge_user_weibo_rel_exist(retweeted_uid):#判断此人是否有信息表 给其建立表
                    retweeted_uid_es_result = es.get(index="user_weibo_relation_info", doc_type="text",id = retweeted_uid)["_source"]
                    mid_list = []
                    for origin_item in retweeted_uid_es_result["origin_weibo"]:#dict
                        mid_list.append(origin_item["mid"])#获取当前全部mid
                    if retweeted_mid not in mid_list: #当前帖子没被记录过
                        origin_weibo_dict = {}
                        origin_weibo_dict["mid"] = retweeted_mid
                        origin_weibo_dict["retweet_num"] = 0
                        origin_weibo_dict["comment_num"] = 1
                        origin_weibo_dict["timestamp"] = timestamp
                        retweeted_uid_es_result["origin_weibo"].append(origin_weibo_dict)

                        origin_weibo_be_comment_dict = {}
                        origin_weibo_be_comment_dict["queue_index"] = queue_index
                        origin_weibo_be_comment_dict["value"] = 1
                        origin_weibo_be_comment_dict["timestamp"] = timestamp
                        retweeted_uid_es_result["origin_weibo_be_comment"].append(origin_weibo_be_comment_dict)
                        es.update(index="user_weibo_relation_info", doc_type="text",id =retweeted_uid,body= {"doc":{"origin_weibo":retweeted_uid_es_result["origin_weibo"],"origin_weibo_be_comment":retweeted_uid_es_result["origin_weibo_be_comment"]}})
                    else :#当前帖子被记录过
                        update_list_1 = []
                        update_list_2 = []
                        for j,_ in enumerate(retweeted_uid_es_result["origin_weibo"]):
                            if _["mid"] != retweeted_mid:
                                update_list_1.append(_)
                            else:
                                new_comment_num = _["comment_num"] + 1
                                _.update(comment_num=new_comment_num,timestamp=timestamp)
                                update_list_1.append(_)
                        es.update(index="user_weibo_relation_info", doc_type="text",id =retweeted_uid ,body = {"doc":{"origin_weibo":update_list_1}})
                        for i,_ in enumerate(retweeted_uid_es_result["origin_weibo_be_comment"]):
                            if _["queue_index"]!= queue_index:
                                update_list_2.append(_)
                            else:
                                new_queue_index = _["queue_index"] + 1
                                _.update(queue_index=new_queue_index)
                                update_list_2.append(_)
                        es.update(index="user_weibo_relation_info", doc_type="text",id =retweeted_uid ,body = {"doc":{"origin_weibo_be_comment":update_list_2}})
                       
                
            else: #中间被人转发过
                nick_id = nicknames[0]#转发人名称
                query_body = {"query": {"bool": {"must": [{"term": {"name": nick_id}}]}},"size": 10}
                result = es.search(index="weibo_user", doc_type="type1", body=query_body)["hits"]["hits"]
                if len(result)!= 0:#查到此用户
                    middle_uid = result[0]["_id"]
                    #cluster_redis.hincrby(str(_id), retweeted_mid + '_retweeted_weibo_comment', 1) 
                    #cluster_redis.hincrby(str(_id), 'retweeted_weibo_comment_timestamp_%s' % queue_index, 1)
                    #cluster_redis.hset(str(_id), retweeted_mid + '_retweeted_weibo_comment_timestamp', timestamp)
                    if judge_user_weibo_rel_exist(middle_uid):#判断此人是否有信息表 给其建立表
                        middle_uid_es_result = es.get(index="user_weibo_relation_info", doc_type="text",id = middle_uid)["_source"]
                        mid_list = []
                        for retweet_item in middle_uid_es_result["retweeted_weibo"]:#dict
                            mid_list.append(retweet_item["mid"])#获取当前全部mid
                        if retweeted_mid not in mid_list: #当前帖子没被记录过
                            retweeted_weibo_dict = {}
                            retweeted_weibo_dict["mid"] = retweeted_mid
                            retweeted_weibo_dict["retweet_num"] = 0
                            retweeted_weibo_dict["comment_num"] = 1
                            retweeted_weibo_dict["timestamp"] = timestamp
                            middle_uid_es_result["retweeted_weibo"].append(retweeted_weibo_dict)
                            retweeted_weibo_be_comment_dict = {}
                            retweeted_weibo_be_comment_dict["queue_index"] = queue_index
                            retweeted_weibo_be_comment_dict["value"] = 1
                            retweeted_weibo_be_comment_dict["timestamp"] = timestamp
                            middle_uid_es_result["retweeted_weibo_be_comment"].append(retweeted_weibo_be_comment_dict)
                            es.update(index="user_weibo_relation_info", doc_type="text",id =middle_uid,body= {"doc":{"retweeted_weibo":middle_uid_es_result["retweeted_weibo"],"retweeted_weibo_be_comment":middle_uid_es_result["retweeted_weibo_be_comment"]}})
                        else :#当前帖子被记录过
                            update_list_1 = []
                            update_list_2 = []
                            for j,_ in enumerate(middle_uid_es_result["retweeted_weibo"]):
                                if _["mid"] != retweeted_mid:
                                    update_list_1.append(_)
                                else:
                                    new_comment_num = _["comment_num"] + 1
                                    _.update(comment_num=new_comment_num,timestamp=timestamp)
                                    update_list_1.append(_)
                            es.update(index="user_weibo_relation_info", doc_type="text",id =middle_uid ,body = {"doc":{"retweeted_weibo":update_list_1}})
                            for i,_ in enumerate(middle_uid_es_result["retweeted_weibo_be_comment"]):
                                if _["queue_index"]!= queue_index:
                                    update_list_2.append(_)
                                else:
                                    new_queue_index = _["queue_index"] + 1
                                    _.update(queue_index=new_queue_index)
                                    update_list_2.append(_)
                            es.update(index="user_weibo_relation_info", doc_type="text",id =middle_uid ,body = {"doc":{"retweeted_weibo_be_comment":update_list_2}})


        elif message_type == 3:
            #cluster_redis.sadd('user_set', user)
            #if cluster_redis.sismember(user + '_retweeted_weibo', retweeted_mid):
                #return
            
            mid_list = []
            for retweet_item in uid_es_result["retweeted_weibo"]:#dict
                mid_list.append(retweet_item["mid"])#获取当前全部mid
            if retweeted_mid in mid_list:#此条微博已经被记录过 pass
                return
            if retweeted_uid =="":
                return

            #cluster_redis.sadd(user + '_retweeted_weibo', retweeted_mid)
            #cluster_redis.hset(user, retweeted_mid + '_retweeted_weibo_timestamp', timestamp) 
            #cluster_redis.hset(user, retweeted_mid + '_retweeted_weibo_retweeted', 0)
            #cluster_redis.hset(user, retweeted_mid + '_retweeted_weibo_comment', 0)

            retweeted_weibo_dict = {}
            retweeted_weibo_dict["mid"] = retweeted_mid
            retweeted_weibo_dict["retweet_num"] = 0
            retweeted_weibo_dict["comment_num"] = 0
            retweeted_weibo_dict["timestamp"] = timestamp
            uid_es_result["retweeted_weibo"].append(retweeted_weibo_dict)
            es.update(index="user_weibo_relation_info", doc_type="text",id =uid,body= {"doc":{"retweeted_weibo":uid_es_result["retweeted_weibo"]}})
            

            queue_index = get_queue_index(timestamp)

            #cluster_redis.hincrby(retweeted_uid, 'origin_weibo_retweeted_timestamp_%s' % queue_index, 1)
            #cluster_redis.hincrby(retweeted_uid, retweeted_mid + '_origin_weibo_retweeted', 1) 
            
            if judge_user_weibo_rel_exist(retweeted_uid):#判断此人是否有信息表 给其建立表
                retweeted_uid_es_result = es.get(index="user_weibo_relation_info", doc_type="text",id = retweeted_uid)["_source"]
                mid_list = []
                for origin_item in retweeted_uid_es_result["origin_weibo"]:#dict
                    mid_list.append(origin_item["mid"])#获取当前全部mid
                if retweeted_mid not in mid_list: #当前帖子没被记录过
                    origin_weibo_dict = {}
                    origin_weibo_dict["mid"] = retweeted_mid
                    origin_weibo_dict["retweet_num"] = 1
                    origin_weibo_dict["comment_num"] = 0
                    origin_weibo_dict["timestamp"] = timestamp
                    retweeted_uid_es_result["origin_weibo"].append(origin_weibo_dict)

                    origin_weibo_be_retweet_dict = {}
                    origin_weibo_be_retweet_dict["queue_index"] = queue_index
                    origin_weibo_be_retweet_dict["value"] = 1
                    origin_weibo_be_retweet_dict["timestamp"] = timestamp
                    retweeted_uid_es_result["origin_weibo_be_retweet"].append(origin_weibo_be_retweet_dict)
                    es.update(index="user_weibo_relation_info", doc_type="text",id =retweeted_uid,body= {"doc":{"origin_weibo":retweeted_uid_es_result["origin_weibo"],"origin_weibo_be_retweet":retweeted_uid_es_result["origin_weibo_be_retweet"]}})
                else :#当前帖子被记录过
                    update_list_1 = []
                    update_list_2 = []
                    for j,_ in enumerate(retweeted_uid_es_result["origin_weibo"]):
                        if _["mid"] != retweeted_mid:
                            update_list_1.append(_)
                        else:
                            new_retweet_num = _["retweet_num"] + 1
                            _.update(retweet_num=new_retweet_num,timestamp=timestamp)
                            update_list_1.append(_)
                    es.update(index="user_weibo_relation_info", doc_type="text",id =retweeted_uid ,body = {"doc":{"origin_weibo":update_list_1}})
                    for i,_ in enumerate(retweeted_uid_es_result["origin_weibo_be_retweet"]):
                        if _["queue_index"]!= queue_index:
                            update_list_2.append(_)
                        else:
                            new_queue_index = _["queue_index"] + 1
                            _.update(queue_index=new_queue_index)
                            update_list_2.append(_)
                    es.update(index="user_weibo_relation_info", doc_type="text",id =retweeted_uid ,body = {"doc":{"origin_weibo_be_retweet":update_list_2}})
                       
            RE = re.compile(u'//@([a-zA-Z-_⺀-⺙⺛-⻳⼀-⿕々〇〡-〩〸-〺〻㐀-䶵一-鿃豈-鶴侮-頻並-龎]+):', re.UNICODE)
            nicknames = RE.findall(text)
            if len(nicknames) != 0:
                for nick_id in nicknames:
                    query_body = {"query": {"bool": {"must": [{"term": {"name": nick_id}}]}},"size": 10}
                    result = es.search(index="weibo_user", doc_type="type1", body=query_body)["hits"]["hits"]
                    if len(result)!= 0:#查到此用户
                        middle_uid = result[0]["_id"]
                    
                        #cluster_redis.hincrby(str(_id), retweeted_mid+'_retweeted_weibo_retweeted', 1) 
                        #cluster_redis.hset(str(_id), retweeted_mid+'_retweeted_weibo_retweeted_timestamp', timestamp)
                        #cluster_redis.hincrby(str(_id), 'retweeted_weibo_retweeted_timestamp_%s' % queue_index, 1)
                        if judge_user_weibo_rel_exist(middle_uid):#判断此人是否有信息表 给其建立表
                            middle_uid_es_result = es.get(index="user_weibo_relation_info", doc_type="text",id = middle_uid)["_source"]
                            mid_list = []
                            for retweet_item in middle_uid_es_result["retweeted_weibo"]:#dict
                                mid_list.append(retweet_item["mid"])#获取当前全部mid
                            if retweeted_mid not in mid_list: #当前帖子没被记录过
                                retweeted_weibo_dict = {}
                                retweeted_weibo_dict["mid"] = retweeted_mid
                                retweeted_weibo_dict["retweet_num"] = 1
                                retweeted_weibo_dict["comment_num"] = 0
                                retweeted_weibo_dict["timestamp"] = timestamp
                                middle_uid_es_result["retweeted_weibo"].append(retweeted_weibo_dict)
                                retweeted_weibo_be_retweet_dict = {}
                                retweeted_weibo_be_retweet_dict["queue_index"] = queue_index
                                retweeted_weibo_be_retweet_dict["value"] = 1
                                retweeted_weibo_be_retweet_dict["timestamp"] = timestamp
                                middle_uid_es_result["retweeted_weibo_be_retweet"].append(retweeted_weibo_be_retweet_dict)
                                es.update(index="user_weibo_relation_info", doc_type="text",id =middle_uid,body= {"doc":{"retweeted_weibo":middle_uid_es_result["retweeted_weibo"],"retweeted_weibo_be_retweet":middle_uid_es_result["retweeted_weibo_be_retweet"]}})
                            else :#当前帖子被记录过
                                update_list_1 = []
                                update_list_2 = []
                                for j,_ in enumerate(middle_uid_es_result["retweeted_weibo"]):
                                    if _["mid"] != retweeted_mid:
                                        update_list_1.append(_)
                                    else:
                                        new_retweet_num = _["retweet_num"] + 1
                                        _.update(retweet_num=new_retweet_num,timestamp=timestamp)
                                        update_list_1.append(_)
                                es.update(index="user_weibo_relation_info", doc_type="text",id =middle_uid ,body = {"doc":{"retweeted_weibo":update_list_1}})
                                for i,_ in enumerate(middle_uid_es_result["retweeted_weibo_be_retweet"]):
                                    if _["queue_index"]!= queue_index:
                                        update_list_2.append(_)
                                    else:
                                        new_queue_index = _["queue_index"] + 1
                                        _.update(queue_index=new_queue_index)
                                        update_list_2.append(_)
                                es.update(index="user_weibo_relation_info", doc_type="text",id =middle_uid ,body = {"doc":{"retweeted_weibo_be_retweet":update_list_2}})


def run_cal(today,lte_time):   #算过去五分钟的微博
    es_index = "flow_text_" + str(today)
    #print(es_index)
    query_body = {
                "query": {
                    "bool": {
                    "must":[{
                        "range": {
                            "timestamp":{
                                "gt": int(lte_time) - 300,
                                "lte": lte_time
                            }
                        }}]
                    }},
                "size":10000   #因为速度原因只能先计算一万
            }
    result_weibo = es_weibo.search(index=es_index, doc_type="text", body=query_body)["hits"]["hits"]
    print(len(result_weibo))
    if len(result_weibo) != 0:
            #统计用户每条微博的转发关系 为了影响力
        es.index(index="flow_timestamp", doc_type="text",body = {"date":today,"timestamp":lte_time})
        for i ,weiboinfo in enumerate(result_weibo):
            cal_user_weibo_relation_info(weiboinfo["_source"])#统计用户每条微博的转发关系


def run_cal_user(uid_list, start_date, end_date):
    es_index_list = ["flow_text_%s" % date for date in get_datelist_v2(start_date,end_date)]
    query_body = {
        "query": {
            "bool": {
            "must":[{
                "terms": {
                    "uid":uid_list
                }}]
            }},
        "size":1000   #因为速度原因只能先计算一千
    }
    result_weibo = es_weibo.search(index=es_index_list, doc_type="text", body=query_body)["hits"]["hits"]
    print(len(result_weibo))
    if len(result_weibo) != 0:
        #统计用户每条微博的转发关系 为了影响力
        for i ,weiboinfo in enumerate(result_weibo):
            cal_user_weibo_relation_info(weiboinfo["_source"])#统计用户每条微博的转发关系


def get_infludece_index():
    while True:
        today = ts2date(time.time())
        
        query_dict = {
            "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "date": today
                  }
                }
              ]
            }
            },
            "size": 0,
            "aggs": {
            "groupby": {
              "terms": {
                "field": "date"
              },
              "aggs": {
                "max_index": {
                  "max": {
                    "field": "timestamp"
                  }
                }
              }
            }
        }
        }
        es_result = es.search(index="flow_timestamp", doc_type="text", body = query_dict)["aggregations"]["groupby"]["buckets"]
        if len(es_result):#此天以执行程序
            lte_time = es_result[0]["max_index"]["value"]
        else:
            lte_time = date2ts(today)

        if lte_time > date2ts(today):   #如果在五分钟内算完了五分钟的数据则等待30秒直到到了下个五分钟
            time.sleep(30)
        else:
            run_cal(today,lte_time)
            break

if __name__ == '__main__':
    # get_infludece_index()
    # for today in get_datelist_v2("2019-03-30","2019-04-10"):
    #     timestamp = date2ts(today)
    #     for i in range(4, 288):
    #         print(i)
    #         run_cal(today, timestamp + 300 * i)

    for date in get_datelist_v2("2019-03-30","2019-04-10"):
        for i in range(int(86400/600)):
            es.index(index='flow_timestamp',doc_type='text',body={"date":date,"timestamp":date2ts(date) + 600*i})