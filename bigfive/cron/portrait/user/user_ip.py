#-*-coding=utf-8-*-
import os
import re
import sys
import json

import geoip2.database

import sys
sys.path.append('../../../')
from config import *
from time_utils import *

ABS_PATH = os.path.dirname(os.path.abspath(__file__))

#IP映射到 国家-省份-城市
def from_ip_get_info(sip):
    reader = geoip2.database.Reader(os.path.join(ABS_PATH,'GeoLite2-City/GeoLite2-City.mmdb'))
    try:
        response = reader.city(sip)
        try:
            city = response.city.names.get(u'zh-CN', response.city.name)
            country = response.registered_country.names.get(u'zh-CN')
            subdivisions = response.subdivisions.most_specific.names.get(u'zh-CN', "")

            if not city:
                city = u'其他'
                # city = response.country.names.get(u'zh-CN')
            if not subdivisions:
                subdivisions = u'其他'
        except Exception as e:
            logging.debug("read Geolite2-City.mmdb error:%s" % e)
            city = ''

        # subdivisions = response.subdivisions.most_specific.names.get(u'zh-CN', "")
        return country+"&"+subdivisions+"&"+city
    except Exception as ex:
        return u'其他'

#汇总用户IP频次
def get_recent_ip(uid,index_list,date): #n_days,

    ip_dict = dict()

    query_body = {"query": {
        "bool": {
            "must": [
                    {"term": {
                        "uid": uid
                        }  
                        }
              
            ]
        }
    },
        "from": 0,
        "size": 0,
        "aggs": {"ip_aggs":{"terms":{"field":"ip","size":300,"order":{"_count":"desc"}}} }
    }

    ip_list = []
    search_result = es_weibo.search(index=index_list,body=query_body)["aggregations"]["ip_aggs"]["buckets"] #doc_type=event_name,
    for i in search_result:
        i["uid"] = uid
        i["timestamp"] = date2ts(date)
        ip_list.append(i)

    return ip_list

#汇总IP
def ip_rank(ip_list):

    for i in ip_list:
        location = from_ip_get_info(i["key"])

        es.index(index="user_activity",doc_type="text",
        body={
            "timestamp": i["timestamp"],
            "uid":i["uid"],
            "ip":i["key"],
            "geo":location,
            "count":int(i["doc_count"]),
            "date":ts2date(i["timestamp"])
        },timeout=50)

  
def get_user_activity(uid, start_date,end_date):

    for day in get_datelist_v2(start_date, end_date):
        
        index_name = "flow_text_" + str(day)

        try: 
            ip_list = get_recent_ip(uid,index_name,day)

            if ip_list == []:
                es.index(index="user_activity",doc_type="text",
                        body={
                        "timestamp": date2ts(day),
                        "uid":uid,
                        "ip":"",
                        "geo":"",
                        "count":0,
                        "date":day},timeout=50)
            else:
                ip_rank(ip_list)

        except:
            pass


if __name__ == '__main__':
    get_user_activity(uid, start_date,end_date)
