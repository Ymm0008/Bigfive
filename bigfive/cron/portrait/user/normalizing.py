# -*- coding: UTF-8 -*-

import os
import numpy as np
import math
import random

import sys
sys.path.append('../../../')
from config import *
from time_utils import *
from global_utils import *

from elasticsearch.helpers import  bulk

def get_uidlist():
    query_body = {"query": {"bool": {"must": [{"match_all": { }}]}},"size":15000}
    es_result = es.search(index="user_information", doc_type="text",body=query_body)["hits"]["hits"]
    uid_list = []
    for es_item in es_result:
        uid_list.append(es_item["_id"])
    #print (uid_list)
    return uid_list

def new_mapping(uid_list):
    for i in uid_list:
        query_body = {"query": {"bool": {"must":{"term": {"uid":i}}}},"size" : 20000}
        user_info = es.search(index = "user_influence",doc_type = "text",body = query_body)["hits"]["hits"]
        for user in user_info:
            id_es = user["_id"]
            new_dict = {"importance_normalization":0,"influence_normalization":0,"activity_normalization":0,"sensitivity_normalization":0}
            old_dict = user["_source"]
            insert_dict = dict(new_dict,**old_dict)
            es.index(index = "user_influence",doc_type = "text",id = id_es,body =insert_dict )
            print(insert_dict)
            print (id_es)
        break
        

def normalize_influence_index(start_date,end_date,day):
    target_max = 100
    target_min = 0
    index_list = ["importance","influence","activity","sensitivity"]
    aggs_max_dict = {}
    aggs_max_dict = {"query":{"range":{"date":{"gte":start_date,"lte":end_date}}},"size":0,"aggs": {"groupby": {"terms":{"field":"timestamp","size":day},"aggs":{"max_index":{"max":{}},"min_index":{"min":{}}}}}}
    index_dic = {}
    for i in index_list :#对于每一个属性值
        print(i)
        #aggs_max_dict = {"aggs": {"index": {"terms":{"field":"timestamp"}}}}
        aggs_max_dict["aggs"]["groupby"]["aggs"]["max_index"]["max"]["field"] = i
        aggs_max_dict["aggs"]["groupby"]["aggs"]["min_index"]["min"]["field"] = i
        print  (aggs_max_dict)
        #max_min_index = es.search(index="user_influence", doc_type="text", body = aggs_max_dict)
        max_min_index = es.search(index="user_influence", doc_type="text", body = aggs_max_dict)["aggregations"]["groupby"]["buckets"]
       
        for j in max_min_index: #每一天 的每一个属性的最值
            query_body = {}
            timestamp = j["key"]
            print(timestamp)
            max_index = j["max_index"]["value"]
            #print (max_index)
            min_index = j["min_index"]["value"]
            #print (min_index)
            index_dic[i] = {timestamp:{"max_index":max_index,"min_index":min_index}}
    print(index_dic)


    for timestamp in index_dic[index_list[0]]:
        sort_dict = {'_id':{'order':'asc'}}
        query_body = {"query": {"bool": {"must":{"term": {"timestamp":timestamp}}}}}
        ESIterator1 = ESIterator(0,sort_dict,1000,"user_influence","text",query_body,es)
        package = []
        count = 0
        while True:
            try:
                #一千条es数据
                user_info = next(ESIterator1)
            except StopIteration:
                # 遇到StopIteration就退出循环
                break
            for user in user_info:
                id_es = user["_id"]
                update_dic = {}
                for index in index_list:
                    max_index = index_dic[index][timestamp]["max_index"]
                    min_index = index_dic[index][timestamp]["min_index"]
                    new_index_value = (target_max-target_min)*(user["_source"][index] - min_index) /(max_index -min_index)+target_min
                    if new_index_value > 100:   #如果出现迷之大于100的情况强行取个随机数
                        new_index_value = 100 *random.random()
                    update_dic[index+"_normalization"] = new_index_value
                package.append({
                    "_index": "user_influence",
                    "_op_type": "update",
                    "_id": id_es,
                    "_type": "text",
                    "doc": update_dic
                })

                if len(package) % 1000 == 0:
                    count += 1
                    print("updating user count %s" % (count * 1000),ts2date(timestamp))
                    bulk(es, package)
                    package = []

        bulk(es, package)


     

         
if __name__ == '__main__':
    #uid_list = get_uidlist()
    #new_mapping(uid_list)
    normalize_influence_index("2019-03-30","2019-03-30",1)
    
    
