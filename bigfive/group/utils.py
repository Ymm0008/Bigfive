# coding=utf-8
import json
import re

from xpinyin import Pinyin

from bigfive.time_utils import *
from bigfive.config import es, MAX_VALUE, USER_RANKING, labels_dict, topic_dict, THREE_MONTH_AGO, TODAY
from bigfive.cache import cache


def index_to_score_rank(index):
    index_to_score_rank_dict = {
        0: [0, 101],
        1: [0, 20],
        2: [20, 40],
        3: [40, 60],
        4: [60, 80],
        5: [80, 101],
    }
    return index_to_score_rank_dict[int(index)]


def create_group_task(data):
    """创建组任务"""
    p = Pinyin()
    data['group_pinyin'] = p.get_pinyin(data['group_name'], '')
    data['create_time'] = nowts()
    data['create_date'] = ts2date(data['create_time'])
    group_id = '{}_{}'.format(data['group_pinyin'], data['create_time'])
    for k, v in data['create_condition'].items():
        data['create_condition'][k] = int(v)
    # 计算进度 0未完成 1计算中 2完成
    data['progress'] = 0
    # 建立计算任务group_task
    es.index(index='group_task', doc_type='text', id=group_id, body=data)
    return data


def search_group_task(group_name, remark, create_time, page, size, order_name, order, index):
    """通过group名称,备注,创建时间查询"""
    """因为字段基本一样,使用index 用于区分task 和info 表,不再复写该函数"""
    # 判断page的合法性
    if page.isdigit():
        page = int(page)
        if page <= 0:
            return {}
    else:
        return {}
    # 基础查询语句
    query = {"query": {"bool": {"must": [], "must_not": [], "should": []}},"from": (int(page) - 1) * int(size), "size": size, "sort": [{"create_time":{"order":"desc"}}]}
    if order and order_name:
        query['sort'].append({order_name: {"order": order}})
    # 添加组名查询
    if group_name:
        query['query']['bool']['should'].append({"wildcard": {"group_name": "*{}*".format(group_name.lower())}})
        query['query']['bool']['should'].append({"wildcard": {"keyword": "*{}*".format(group_name.lower())}})
    # 添加备注查询
    if remark:
        query['query']['bool']['must'].append(
            {"wildcard": {"remark": "*{}*".format(remark.lower())}})
    # 添加时间查询
    if create_time:
        # 转换前端传的日期为时间戳
        st = date2ts(create_time)
        et = st + 86400
        query['query']['bool']['must'].append(
            {"range": {"create_time": {"gt": st, "lt": et}}})
    if index == 'task':
        index = 'group_task'
    elif index == 'info':
        index = 'group_information'
    else:
        raise ValueError("index is error!")
    r = es.search(index=index, doc_type='text', body=query, _source_include=['group_name,create_time,remark,keyword,progress,create_condition'])
    # 结果为空
    if not r:
        return {}
    total = r['hits']['total']
    # 正常返回
    result = []
    for hit in r['hits']['hits']:
        item = hit['_source']
        # 为前端返回es的_id字段
        item['id'] = hit['_id']
        item['create_time'] = ts2date(item['create_time'])
        result.append(item)
    return {'rows': result, 'total': total}


def delete_by_id(index, doc_type, id):
    """通过es的_id删除一条记录"""
    if index == 'task':
        r = es.get(index='group_task', doc_type=doc_type, id=id)
        if r['_source']['progress'] not in  [0,3]:
            raise ValueError('progress is not 0 or 3')
        es.delete(index='group_task', doc_type=doc_type, id=id)
    elif index == 'info':
        es.delete(index='group_ranking', doc_type=doc_type, id=id)
        es.delete(index='group_information', doc_type=doc_type, id=id)
        es.delete(index='group_task', doc_type=doc_type, id=id)

def search_group_ranking(keyword, page, size, order_name, order_type, order_dict):
    page = page if page else '1'
    size = size if size else '10'
    sort_list = []
    if order_dict:
        for o_n, o_t in json.loads(order_dict).items():
            sort_list.append({o_n: {"order": "desc"}}) if o_t else sort_list.append(
                {o_n: {"order": "asc"}})

    if order_name == 'name':
        order_name = 'group_name'
    order_name = order_name if order_name else 'influence_index'
    order_type = order_type if order_type else 'desc'
    sort_list.append({order_name: {"order": order_type}})

    # 数据库中有些数据没有label字段，过滤掉这些数据
    query = {"query": {"bool": {"must": [{"match_all": {}}], "must_not": [{"constant_score": {"filter": {"missing": {"field": "extroversion_label"}}}}], "should": []}}, "from": 0, "size": 6, "sort": [], "aggs": {}}

    if keyword:
        query['query']['bool']['must'].append({"wildcard":{"group_name": "*%s*" % keyword}})

    query['from'] = str((int(page) - 1) * int(size))
    query['size'] = str(size)
    query['sort'] = sort_list

    r = es.search(index='group_ranking', doc_type='text', body=query)

    total = r['hits']['total']
    # 结果为空
    if not total:
        return {}
    r = r['hits']['hits']
    # 正常返回
    result = []
    for hit in r:

        hit['_source']['big_five_list'] = []
        hit['_source']['dark_list'] = []

        if hit['_source']['extroversion_label'] == 0:
            hit['_source']['big_five_list'].append({'外倾性': '0'})  # 0代表极端低
        if hit['_source']['extroversion_label'] == 2:
            hit['_source']['big_five_list'].append({'外倾性': '1'})  # 1代表极端高
        if hit['_source']['openn_label'] == 0:
            hit['_source']['big_five_list'].append({'开放性': '0'})
        if hit['_source']['openn_label'] == 2:
            hit['_source']['big_five_list'].append({'开放性': '1'})
        if hit['_source']['agreeableness_label'] == 0:
            hit['_source']['big_five_list'].append({'宜人性': '0'})
        if hit['_source']['agreeableness_label'] == 2:
            hit['_source']['big_five_list'].append({'宜人性': '1'})
        if hit['_source']['conscientiousness_label'] == 0:
            hit['_source']['big_five_list'].append({'尽责性': '0'})
        if hit['_source']['conscientiousness_label'] == 2:
            hit['_source']['big_five_list'].append({'尽责性': '1'})
        if hit['_source']['nervousness_label'] == 0:
            hit['_source']['big_five_list'].append({'神经质': '0'})
        if hit['_source']['nervousness_label'] == 2:
            hit['_source']['big_five_list'].append({'神经质': '1'})

        if hit['_source']['machiavellianism_label'] == 0:
            hit['_source']['dark_list'].append({'马基雅维里主义': '0'})
        if hit['_source']['machiavellianism_label'] == 2:
            hit['_source']['dark_list'].append({'马基雅维里主义': '1'})
        if hit['_source']['psychopathy_label'] == 0:
            hit['_source']['dark_list'].append({'精神病态': '0'})
        if hit['_source']['psychopathy_label'] == 2:
            hit['_source']['dark_list'].append({'精神病态': '1'})
        if hit['_source']['narcissism_label'] == 0:
            hit['_source']['dark_list'].append({'自恋': '0'})
        if hit['_source']['narcissism_label'] == 2:
            hit['_source']['dark_list'].append({'自恋': '1'})

        hit['_source']['name'] = hit['_source']['group_name']

        item = hit['_source']
        # 为前端返回es的_id字段,为删除功能做支持
        item['id'] = hit['_id']
        item['name'] = item['group_name']
        result.append(item)
    return {'rows': result, 'total': total}


def get_group_user_list(gid, page, size, order_name, order_type):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "group_id": gid
                        }
                    }
                ]
            }
        }
    }
    user_ranking_query = {"query": {"bool": {"should": []}}}
    user_list = es.search(index='group_information', doc_type='text', body=query)[
        'hits']['hits'][0]['_source']['userlist']
    for uid in user_list:
        user_ranking_query['query']['bool']['should'].append({"term": {"uid": uid}})

    sort_list = []
    order_name = 'username' if order_name == 'name' else order_name
    order_name = order_name if order_name else 'influence_index'
    order_type = order_type if order_type else 'desc'
    sort_list.append({order_name: {"order": order_type}})

    user_ranking_query['from'] = str((int(page) - 1) * int(size))
    user_ranking_query['size'] = str(size)
    user_ranking_query['sort'] = sort_list

    hits = es.search(index='user_ranking', doc_type='text', body=user_ranking_query)['hits']

    result = {'rows': [], 'total': hits['total']}
    for item in hits['hits']:
        item['_source']['big_five_list'] = []
        item['_source']['dark_list'] = []

        if item['_source']['extroversion_label'] == 0:
            item['_source']['big_five_list'].append({'外倾性': '0'})  # 0代表极端低
        if item['_source']['extroversion_label'] == 2:
            item['_source']['big_five_list'].append({'外倾性': '1'})  # 1代表极端高
        if item['_source']['openn_label'] == 0:
            item['_source']['big_five_list'].append({'开放性': '0'})
        if item['_source']['openn_label'] == 2:
            item['_source']['big_five_list'].append({'开放性': '1'})
        if item['_source']['agreeableness_label'] == 0:
            item['_source']['big_five_list'].append({'宜人性': '0'})
        if item['_source']['agreeableness_label'] == 2:
            item['_source']['big_five_list'].append({'宜人性': '1'})
        if item['_source']['conscientiousness_label'] == 0:
            item['_source']['big_five_list'].append({'尽责性': '0'})
        if item['_source']['conscientiousness_label'] == 2:
            item['_source']['big_five_list'].append({'尽责性': '1'})
        if item['_source']['nervousness_label'] == 0:
            item['_source']['big_five_list'].append({'神经质': '0'})
        if item['_source']['nervousness_label'] == 2:
            item['_source']['big_five_list'].append({'神经质': '1'})

        if item['_source']['machiavellianism_label'] == 0:
            item['_source']['dark_list'].append({'马基雅维里主义': '0'})
        if item['_source']['machiavellianism_label'] == 2:
            item['_source']['dark_list'].append({'马基雅维里主义': '1'})
        if item['_source']['psychopathy_label'] == 0:
            item['_source']['dark_list'].append({'精神病态': '0'})
        if item['_source']['psychopathy_label'] == 2:
            item['_source']['dark_list'].append({'精神病态': '1'})
        if item['_source']['narcissism_label'] == 0:
            item['_source']['dark_list'].append({'自恋': '0'})
        if item['_source']['narcissism_label'] == 2:
            item['_source']['dark_list'].append({'自恋': '1'})

        item['_source']['name'] = item['_source']['username']
        result['rows'].append(item['_source'])

    return result


def get_index_rank(personality_value, personality_name, label_type):
    result = 0
    query_body = {
        'query':{
            'bool':{
                'must':[
                    {'range':{
                        personality_name:{
                            'from':personality_value,
                            'to': MAX_VALUE
                            }
                        }
                    }
                ]
            }
        }
    }
    index_rank = es.count(index=USER_RANKING, doc_type='text', body=query_body)
    if index_rank['_shards']['successful'] != 0:
       result = index_rank['count']
    else:
        # print('es index rank error')
        result = 0
    all_user_count = es.count(index=USER_RANKING, doc_type='text', body={'query':{'match_all':{}}})['count']
    if label_type == 'low':
        return result / all_user_count
    elif label_type == 'high':
        return (all_user_count - result) / all_user_count
    else:
        raise ValueError


def get_group_basic_info(gid):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "group_id": gid
                        }
                    }
                ]
            }
        }
    }
    group_item = {}
    # print(query)
    result = es.search(index='group_information', doc_type='text', body=query)[
        'hits']['hits'][0]['_source']
    group_ranking_result = es.search(index='group_ranking', doc_type='text', body=query)[
        'hits']['hits'][0]['_source']
    # print(group_ranking_result)

    # 黑暗人格字段
    group_item['machiavellianism'] = group_ranking_result['machiavellianism_index']
    group_item['narcissism'] = group_ranking_result['narcissism_index']
    group_item['psychopathy'] = group_ranking_result['psychopathy_index']

    # 大五人格字段
    group_item['extroversion'] = group_ranking_result['extroversion_index']
    group_item['conscientiousness'] = group_ranking_result[
        'conscientiousness_index']
    group_item['agreeableness'] = group_ranking_result['agreeableness_index']
    group_item['openn'] = group_ranking_result['openn_index']
    group_item['nervousness'] = group_ranking_result['nervousness_index']

    # 群组基本信息
    group_item['group_name'] = result['group_name']
    group_item['user_count'] = len(result['userlist'])
    group_item['keyword'] = result['keyword']
    group_item['create_time'] = result['create_time']
    group_item['remark'] = result['remark']

    # 群组星级
    group_item['liveness_star'] = group_ranking_result['liveness_star']
    group_item['importance_star'] = group_ranking_result['importance_star']
    group_item['sensitive_star'] = group_ranking_result['sensitive_star']
    group_item['influence_star'] = group_ranking_result['influence_star']
    group_item['compactness_star'] = group_ranking_result['compactness_star']

    machiavellianism_low_count = 0
    machiavellianism_high_count = 0
    narcissism_low_count = 0
    narcissism_high_count = 0
    psychopathy_low_count = 0
    psychopathy_high_count = 0

    extroversion_low_count = 0
    extroversion_high_count = 0
    conscientiousness_low_count = 0
    conscientiousness_high_count = 0
    agreeableness_low_count = 0
    agreeableness_high_count = 0
    openn_low_count = 0
    openn_high_count = 0
    nervousness_low_count = 0
    nervousness_high_count = 0
    user_list = result['userlist']
    for user_id in user_list:
        try:
            user_ranking_dict = es.get(index='user_ranking', id=user_id, doc_type='text')['_source']
        except:
            continue
        if user_ranking_dict['machiavellianism_label'] == 0:
            machiavellianism_low_count += 1
        if user_ranking_dict['machiavellianism_label'] == 2:
            machiavellianism_high_count += 1
        if user_ranking_dict['narcissism_label'] == 0:
            narcissism_low_count += 1
        if user_ranking_dict['narcissism_label'] == 2:
            narcissism_high_count += 1
        if user_ranking_dict['psychopathy_label'] == 0:
            psychopathy_low_count += 1
        if user_ranking_dict['psychopathy_label'] == 2:
            psychopathy_high_count += 1
        if user_ranking_dict['extroversion_label'] == 0:
            extroversion_low_count += 1
        if user_ranking_dict['extroversion_label'] == 2:
            extroversion_high_count += 1
        if user_ranking_dict['conscientiousness_label'] == 0:
            conscientiousness_low_count += 1
        if user_ranking_dict['conscientiousness_label'] == 2:
            conscientiousness_high_count += 1
        if user_ranking_dict['agreeableness_label'] == 0:
            agreeableness_low_count += 1
        if user_ranking_dict['agreeableness_label'] == 2:
            agreeableness_high_count += 1
        if user_ranking_dict['openn_label'] == 0:
            openn_low_count += 1
        if user_ranking_dict['openn_label'] == 2:
            openn_high_count += 1
        if user_ranking_dict['nervousness_label'] == 0:
            nervousness_low_count += 1
        if user_ranking_dict['nervousness_label'] == 2:
            nervousness_high_count += 1
        # print(user_ranking_dict)
    group_item['machiavellianism_low_count'] = machiavellianism_low_count
    group_item['machiavellianism_high_count'] = machiavellianism_high_count
    group_item['narcissism_low_count'] = narcissism_low_count
    group_item['narcissism_high_count'] = narcissism_high_count
    group_item['psychopathy_low_count'] = psychopathy_low_count
    group_item['psychopathy_high_count'] = psychopathy_high_count
    group_item['extroversion_low_count'] = extroversion_low_count
    group_item['extroversion_high_count'] = extroversion_high_count
    group_item['conscientiousness_low_count'] = conscientiousness_low_count
    group_item['conscientiousness_high_count'] = conscientiousness_high_count
    group_item['agreeableness_low_count'] = agreeableness_low_count
    group_item['agreeableness_high_count'] = agreeableness_high_count
    group_item['openn_low_count'] = openn_low_count
    group_item['openn_high_count'] = openn_high_count
    group_item['nervousness_low_count'] = nervousness_low_count
    group_item['nervousness_high_count'] = nervousness_high_count
    return group_item


def modify_group_remark(group_id, remark):
    es.update(index='group_information', id=group_id, doc_type='text', body={'doc': {'remark': remark}})


def group_preference(group_id):

    query = {"query":{"bool":{"must":[{"term":{"group_id":group_id}}]}},"from":0,"size":1,"sort":[],"aggs":{}}
    hits = es.search(index='group_domain_topic',doc_type='text',body=query)['hits']['hits']
    sta_hits = es.search(index='group_text_analysis_sta', doc_type='text', body=query)['hits']['hits']

    if not hits or not sta_hits:
        return {}


    item = hits[0]['_source']
    domain_static = {labels_dict[one['domain']]: one['count']/len(es.search(index='group_information', doc_type='text', body={"query":{"bool":{"must":[{"term":{"group_id":group_id}}]}}})[
        'hits']['hits'][0]['_source']['userlist'])
                     for one in sorted(item['domain_static'], key=lambda x: x['count'], reverse=True)[0:5] if one['count']}
    topic_static = {topic_dict[one['topic'].replace('-', '_')]: one['count']/len(es.search(index='group_information', doc_type='text', body={"query":{"bool":{"must":[{"term":{"group_id":group_id}}]}}})[
        'hits']['hits'][0]['_source']['userlist'])
                    for one in sorted(item['topic_static'], key=lambda x: x['count'], reverse=True)[0:5] if one['count']}

    sta_item = sta_hits[0]['_source']
    keywords = {one['keyword']: one['count'] for one in sta_item['keywords']}
    hastags = {one['hastag']: one['count'] for one in sta_item['hastags']}
    sensitive_words = {one['sensitive_word']: one['count']
                       for one in sta_item['sensitive_words']}

    result = {'domain_static': domain_static, 'topic_result': topic_static,
              'keywords': keywords, 'hastags': hastags, 'sensitive_words': sensitive_words}

    return result


def group_influence(group_id, interval):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "group_id": group_id
                        }
                    },
                    # {
                    #     "range": {
                    #         "date": {
                    #             "gte": THREE_MONTH_AGO,
                    #             "lte": TODAY
                    #         }
                    #     }
                    # }
                ]
            }
        },
        "from": 0,
        "size": 0,
        "sort": [],
        "aggs": {
            "groupDate": {
                "date_histogram": {
                    "field": "date",
                    "interval": interval,
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "sensitivity": {
                        "sum": {
                            "field": "sensitivity"
                        }
                    },
                    "influence": {
                        "sum": {
                            "field": "influence"
                        }
                    },
                    "activity": {
                        "sum": {
                            "field": "activity"
                        }
                    },
                    "importance": {
                        "sum": {
                            "field": "importance"
                        }
                    }
                }
            }
        }
    }
    es_result = es.search(index="group_influence", doc_type="text", body=query_body)[
        "aggregations"]["groupDate"]["buckets"]
    result_list = []
    for data in es_result:
        item = {}
        item['sensitivity'] = data['sensitivity']['value']
        item['influence'] = data['influence']['value']
        item['activity'] = data['activity']['value']
        item['importance'] = data['importance']['value']
        item['timestamp'] = data['key'] // 1000
        item['date'] = data['key_as_string']
        result_list.append(item)
    return result_list


def group_emotion(group_id, interval):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "group_id": group_id
                        }
                    },
                    # {
                    #     "range": {
                    #         "date": {
                    #             "gte": THREE_MONTH_AGO,
                    #             "lte": TODAY
                    #         }
                    #     }
                    # }
                ]
            }
        },
        "from": 0,
        "size": 0,
        "sort": [],
        "aggs": {
            "groupDate": {
                "date_histogram": {
                    "field": "date",
                    "interval": interval,
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "nuetral": {
                        "sum": {
                            "field": "nuetral"
                        }
                    },
                    "negtive": {
                        "sum": {
                            "field": "negtive"
                        }
                    },
                    "positive": {
                        "sum": {
                            "field": "positive"
                        }
                    }
                }
            }
        }
    }

    buckets = es.search(index="group_emotion", doc_type="text", body=query_body)[
        'aggregations']['groupDate']['buckets']
    result = {
        'time': [],
        "positive_line": [],
        "negtive_line": [],
        "nuetral_line": []
    }
    for bucket in buckets:
        result['time'].append(bucket['key_as_string'], )
        result["positive_line"].append(bucket['positive']['value'], )
        result["negtive_line"].append(bucket['negtive']['value'], )
        result["nuetral_line"].append(bucket['nuetral']['value'])
    return result


@cache.memoize(60)
def group_social_contact(group_id, map_type):
    user_list = es.get(index='group_information', doc_type='text', id=group_id)[
        '_source']['userlist']
    if map_type in ['1', '2']:
        message_type = 3
    else:
        message_type = 2
    group_create_date = es.get(index='group_information', id=group_id, doc_type='text')['_source']['create_date']
    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "message_type": message_type
                                }
                            },
                            {
                                "terms": {
                                    'target': user_list
                                }
                            },
                            {
                                "terms": {
                                    'source': user_list
                                }
                            },
                            {
                                "range": {
                                    "date": {
                                        "gte": ts2date(date2ts(group_create_date) - 90 * 3600 * 24),
                                        "lte": group_create_date
                                    }
                                }
                            }
                        ]}
                }}
        },
        "size": 10000,
    }
    # print(query_body)
    r = es.search(index="user_social_contact", doc_type="text",
                  body=query_body)["hits"]["hits"]
    node = []
    link = []
    for one in r:
        item = one['_source']
        a = {'id': item['target'], 'name': item['target_name']}
        b = {'id': item['source'], 'name': item['source_name']}
        c = {'source': item['source_name'], 'target': item['target_name']}
        if c not in link and c['source'] != c['target']:
            link.append(c)
            if a not in node:
                node.append(a)
            if b not in node:
                node.append(b)
    social_contact = {'node': node, 'link': link}
    if node:
        return social_contact
    return {}

# def group_social_contact(group_id, map_type):
#     query = {"query":{"bool":{"must":[{"term":{"group_id":group_id}},{"term":{"map_type":map_type}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"aggs":{}}
#     hits = es.search(index='group_social_contact',doc_type='text',body=query)['hits']['hits']
#     if not hits:
#         return {}
#     hit = hits[0]['_source']
#     node = [{'name':i} for i in hit['node']]
#     return {'node':node,'link':hit['link']}
def get_group_activity(group_id):
    query = {"query": {"bool": {"must": [{"term": {"group_id": group_id}}], "must_not": [
    ], "should": []}}, "from": 0, "size": 1, "sort": [], "aggs": {}}

    # print(query)
    hits = es.search(index='group_activity', doc_type='text',
                     body=query)['hits']['hits']
    if not hits:
        return {}
    result = {'one': [], 'two': [], 'three': [], 'four': []}
    item = hits[0]['_source']
    activity_direction = sorted(item['activity_direction'], key=lambda x: x[
                                'count'], reverse=True)[:5]
    for i in activity_direction:
        start_end = i['geo2geo'].split('&')
        result['one'].append(
            {'start': start_end[0], 'end': start_end[1], 'count': i['count']})

    start_geo_item = {}
    end_geo_item = {}
    route_list = []
    for i in sorted(item['activity_direction'], key=lambda x: x['count'], reverse=True):
        try:
            if i['geo2geo'].split('&')[0].split(' ')[1] == '其他' or i['geo2geo'].split('&')[1].split(' ')[1] == '其他':
                continue
            if i['geo2geo'].split('&')[0].split(' ')[0] != '中国' or i['geo2geo'].split('&')[1].split(' ')[0] != '中国':
                continue
        except:
            continue

        start_geo_item.setdefault(re.sub(r'省|市|壮族|维吾尔族|回族|自治区', r'', i['geo2geo'].split('&')[0].split(' ')[1]), 0)
        start_geo_item[re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', i['geo2geo'].split('&')[0].split(' ')[1])] += i['count']
        end_geo_item.setdefault(re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', i['geo2geo'].split('&')[1].split(' ')[1]), 0)
        end_geo_item[re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', i['geo2geo'].split('&')[1].split(' ')[1])] += i['count']
        route_dict = {'s': re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', i['geo2geo'].split('&')[0].split(' ')[1]), 'e': re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', i['geo2geo'].split('&')[1].split(' ')[1])}
        if route_dict not in route_list and route_dict['s'] != route_dict['e']:
            route_list.append(route_dict)

    geo_item = {}
    for ks, vs in start_geo_item.items():
        for ke, ve in end_geo_item.items():
            if ks == ke:
                geo_item[ks] = vs + ve
                break
    result['two'] = sorted(item['main_start_geo'], key=lambda x: x[
                           'count'], reverse=True)[:5]
    result['three'] = sorted(item['main_end_geo'], key=lambda x: x[
                             'count'], reverse=True)[:5]
    result['four'] = {'route_list': route_list, 'geo_count': geo_item}
    return result

