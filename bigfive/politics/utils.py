import json
import re
import time
from collections import OrderedDict
from elasticsearch.helpers import scan
from xpinyin import Pinyin

from bigfive.config import *
from bigfive.cache import cache
from bigfive.time_utils import *

def get_hot_politics_list(keyword, page, size, order_name, order_type):
    query = {"query": {"bool": {"must": [], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}}
    page = page if page else '1'
    size = size if size else '10'
    order_name = 'politics_name' if order_name == 'name' else order_name
    order_name = order_name if order_name else 'politics_name'
    order_type = order_type if order_type else 'asc'
    query['sort'] += [{order_name: {"order": order_type}}]
    query['from'] = str((int(page) - 1) * int(size))
    query['size'] = str(size)
    if keyword:
        query['query']['bool']['should'] += [{"wildcard":{"politics_name": "*{}*".format(keyword)}},{"match":{"keywords": "{}".format(keyword)}}]
    # print(query)
    hits = es.search(index='politics_information', doc_type='text', body=query)['hits']

    result = {'rows': [], 'total': hits['total']}
    for item in hits['hits']:
        try:
            del item['_source']['userlist']
        except:
            pass
        item['_source']['name'] = item['_source']['politics_name']
        result['rows'].append(item['_source'])
    return result

def post_create_hot_politics(politics_name, keywords, location, start_date, end_date):
    politics_pinyin = Pinyin().get_pinyin(politics_name, '')
    create_date = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))
    create_time = int(time.time())
    progress = 0
    politics_id = '{}_{}'.format(politics_pinyin, str(create_time))
    hot_politics = {
        "politics_name": politics_name,
        "politics_pinyin": politics_pinyin,
        "create_time": create_time,
        "create_date": create_date,
        "keywords": keywords,
        "progress": progress,
        "politics_id": politics_id,
        "location": location,
        "start_date": start_date,
        "end_date": end_date
    }
    es.index(index='politics_information', doc_type='text', body=hot_politics, id=politics_id)

def post_delete_hot_politics(politics_id):
    es.delete(index='politics_information', doc_type='text', id=politics_id)

def get_politics_personality(politics_id,sentiment):
    query_body = {"query":{"bool":{"must":[{"term":{"politics_id":politics_id}},{"term":{"sentiment":sentiment}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}}
    hits = es.search(index='politics_personality',doc_type='text',body=query_body)['hits']['hits']
    result = {'BigV':{'high': {}, 'low': {}}, 'ordinary': {'high': {}, 'low': {}}}
    for hit in hits:
        item = hit['_source']
        for k,v in item.items():
            if 'label' in k:
                if v['high'] != 0:
                    # print(item['user_type'])
                    result[item['user_type']]['high'].update({PERSONALITY_EN_CH[k.split('_')[0]]:v['high']})
                if v['low'] !=0:
                    result[item['user_type']]['low'].update({PERSONALITY_EN_CH[k.split('_')[0]]:v['low']})
    return result

def get_politics_topic(politics_id,sentiment):
    query_body = {"query":{"bool":{"must":[{"term":{"politics_id":politics_id}},{"term":{"sentiment":sentiment}}],"must_not":[],"should":[]}},"from":0,"size":1000,"sort":[],"aggs":{}}
    hits = es.search(index='politics_topic',doc_type='text',body=query_body)['hits']['hits']
    result = {'BigV':{},'ordinary':{}}
    for hit in hits:
        item = hit['_source']
        for k,v in item.items():
            if 'topic' not in k:
                continue
            if k.split('_')[-1] not in result[item['user_type']].keys():
                result[item['user_type']][k.split('_')[-1]] = {}
            if 'key_words_topic' in k:
                result[item['user_type']][k.split('_')[-1]].update({'ciyun':{i['keyword']:i['value'] for i in v}})
            elif 'weibo_topic' in k:
                mid_list = [i['mid'] for i in v]
                query = {"query":{"bool":{"must":[{"terms":{"mid":mid_list}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}}
                r = es.search(index='politics_'+politics_id,doc_type='text',body=query)['hits']['hits']
                if r:
                    result[item['user_type']][k.split('_')[-1]].update({'weibo':[j['_source'] for j in r]})
                else:
                    result[item['user_type']][k.split('_')[-1]].update({'weibo':[]})
    return result

def get_politics_statistics(politics_id):
    query_body = {"size":0,"aggs":{"statistics": {"terms": {"field": "sentiment"}}}}
    r = es.search(index='politics_'+ politics_id,doc_type='text',body=query_body)
    buckets = r['aggregations']['statistics']['buckets']
    result = {'total':0,"negative": 0,"negative_pro": '0%',"positive": 0,"positive_pro": '0%'}
    for bucket in buckets:
        if int(bucket['key']) == 1:
            result['positive'] = bucket['doc_count']
        elif int(bucket['key'])>1:
            result['negative'] += bucket['doc_count']
    result['total'] = r['hits']['total']
    result['negative_pro'] = "%d%%" % (result['negative']/result['total'] * 100)
    result['positive_pro'] = "%d%%" % (result['positive']/result['total'] * 100)
    return result