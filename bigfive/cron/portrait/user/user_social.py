# -*- coding: UTF-8 -*-

import os
import time
import numpy as np
import math
import json
import sys

sys.path.append('../../../')
from config import *
from time_utils import *
from sentiment_classification.triple_sentiment_classifier import triple_classifier

        
def cal_user_social(uid,weibo_data_dict):
    for day,weibo_list in weibo_data_dict.items():#value 为列表  key为日期
        es_timestamp = date2ts(day)
        sum_r = len(weibo_list)
        if sum_r :#此天有微博数据
            for weibo_item in weibo_list: #weibo_item为字典
                if weibo_item["_source"]["message_type"] == 2 or weibo_item["_source"]["message_type"] == 3:
                    source = weibo_item["_source"]["root_uid"]
                    try:
                        source_inf = es.get(index="weibo_user", doc_type="type1",id = source)
                        if source_inf["found"] == True:
                            source_name = source_inf["_source"]["name"]
                        message_type = weibo_item["_source"]["message_type"]
                        
                        target = uid 
                        target_inf = es.get(index="user_information", doc_type="text",id = uid)
                        if target_inf["found"] == True:
                            target_name = target_inf["_source"]["username"]
                        else:
                            target_name = target
                        
                        id_es = str(target)+"_"+ str(source) +"_"+str(es_timestamp)+"_"+str(message_type)
                        
                        try:
                            social_re = es.get(index = "user_social_contact",doc_type = "text",id = id_es)["_source"]#当天多次转发用户微博
                            interaction_count = int(social_re["count"])

                            new_count = interaction_count + 1
                            es.update(index = "user_social_contact",doc_type = "text",id = id_es, body = {"doc":{"count":new_count}})
                        except:
                            es.index(index = "user_social_contact",doc_type = "text",id = id_es, body = {"timestamp":es_timestamp,"source": source, "target": target, "source_name": source_name, "target_name":target_name,"message_type":message_type,"date":day,"count":1})
                            print("finish index")
                                
                    except:
                        print("error",sys.exc_info()[0])
                        



if __name__ == '__main__':
    pass 
