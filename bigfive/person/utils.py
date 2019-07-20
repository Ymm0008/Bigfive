# -*- coding: utf-8 -*-

import json
import re
import time

from elasticsearch.helpers import scan

from bigfive.config import es, labels_dict, topic_dict, MAX_VALUE, USER_RANKING, TODAY, A_WEEK_AGO, THREE_MONTH_AGO, USER_INFORMATION
from bigfive.cache import cache
from bigfive.time_utils import yesterday, datetime2ts, ts2datetime, date2ts, ts2date


def judge_uid_or_nickname(keyword):
    return True if re.findall('^\d+$', keyword) else False


def index_to_score_rank(index):
    # if not index:
    #     index = 0
    index_to_score_rank_dict = {
        0: [0, 101],
        1: [0, 20],
        2: [20, 40],
        3: [40, 60],
        4: [60, 80],
        5: [80, 101],
    }
    return index_to_score_rank_dict[int(index)]


def portrait_table(keyword, page, size, order_name, order_type, machiavellianism_index,
                   narcissism_index, psychopathy_index, extroversion_index, nervousness_index, openn_index,
                   agreeableness_index, conscientiousness_index, order_dict):
    page = page if page else '1'
    size = size if size else '10'
    sort_list = []
    if order_dict:
        for o_n, o_t in json.loads(order_dict).items():
            sort_list.append({o_n: {"order": "desc"}}) if o_t else sort_list.append({o_n: {"order": "asc"}})
    if order_name == 'name':
        order_name = 'username'
    order_name = order_name if order_name else 'influence_index'
    order_type = order_type if order_type else 'desc'
    sort_list.append({order_name: {"order": order_type}})

    machiavellianism_index = machiavellianism_index if machiavellianism_index != '' else 0
    narcissism_index = narcissism_index if narcissism_index != '' else 0
    psychopathy_index = psychopathy_index if psychopathy_index != '' else 0
    extroversion_index = extroversion_index if extroversion_index != '' else 0
    nervousness_index = nervousness_index if nervousness_index != '' else 0
    openn_index = openn_index if openn_index != '' else 0
    agreeableness_index = agreeableness_index if agreeableness_index != '' else 0
    conscientiousness_index = conscientiousness_index if conscientiousness_index != '' else 0

    machiavellianism_rank = index_to_score_rank(machiavellianism_index)
    narcissism_rank = index_to_score_rank(narcissism_index)
    psychopathy_rank = index_to_score_rank(psychopathy_index)
    extroversion_rank = index_to_score_rank(extroversion_index)
    nervousness_rank = index_to_score_rank(nervousness_index)
    openn_rank = index_to_score_rank(openn_index)
    agreeableness_rank = index_to_score_rank(agreeableness_index)
    conscientiousness_rank = index_to_score_rank(conscientiousness_index)

    query = {"query": {"bool": {"must": [],"should": []}}}
    if machiavellianism_index:
        query['query']['bool']['must'].append({"range": {
            "machiavellianism_index": {"gte": str(machiavellianism_rank[0]), "lt": str(machiavellianism_rank[1])}}})
    if narcissism_index:
        query['query']['bool']['must'].append(
            {"range": {"narcissism_index": {"gte": str(narcissism_rank[0]), "lt": str(narcissism_rank[1])}}})
    if psychopathy_index:
        query['query']['bool']['must'].append(
            {"range": {"psychopathy_index": {"gte": str(psychopathy_rank[0]), "lt": str(psychopathy_rank[1])}}})
    if extroversion_index:
        query['query']['bool']['must'].append(
            {"range": {"extroversion_index": {"gte": str(extroversion_rank[0]), "lt": str(extroversion_rank[1])}}})
    if nervousness_index:
        query['query']['bool']['must'].append(
            {"range": {"nervousness_index": {"gte": str(nervousness_rank[0]), "lt": str(nervousness_rank[1])}}})
    if openn_index:
        query['query']['bool']['must'].append(
            {"range": {"openn_index": {"gte": str(openn_rank[0]), "lt": str(openn_rank[1])}}})
    if agreeableness_index:
        query['query']['bool']['must'].append(
            {"range": {"agreeableness_index": {"gte": str(agreeableness_rank[0]), "lt": str(agreeableness_rank[1])}}})
    if conscientiousness_index:
        query['query']['bool']['must'].append({"range": {
            "conscientiousness_index": {"gte": str(conscientiousness_rank[0]), "lt": str(conscientiousness_rank[1])}}})
    if keyword:
        user_query = '{"wildcard":{"uid": "%s*"}}' % keyword if judge_uid_or_nickname(
            keyword) else '{"wildcard":{"username": "*%s*"}}' % keyword
        query['query']['bool']['must'].append(json.loads(user_query))
        # query['query']['bool']['should'] += [{"wildcard":{"uid": "*{}*".format(keyword)}},{"wildcard":{"username": "*{}*".format(keyword)}}]
    query['from'] = str((int(page) - 1) * int(size))
    query['size'] = str(size)
    query['sort'] = sort_list
    # query['sort'] = [{i: {'order': order_type}} for i in order_name.split(',')]
    # query['sort'] = [{order_name: {"order": order_type}}]
    print(query)
    hits = es.search(index='user_ranking', doc_type='text', body=query)['hits']

    result = {'rows': [], 'total': hits['total']}
    for item in hits['hits']:
        item['_source']['big_five_list'] = []
        item['_source']['dark_list'] = []

        # if machiavellianism_index:
        #
        #     if machiavellianism_rank[0] <= int(item['_source']['machiavellianism_index']) < machiavellianism_rank[1]:
        #         del item
        #         continue
        # if narcissism_index:
        #     if narcissism_rank[0] <= int(item['_source']['narcissism_index']) < narcissism_rank[1]:
        #         del item
        #         continue
        # if psychopathy_index:
        #     if psychopathy_rank[0] <= int(item['_source']['psychopathy_index']) < psychopathy_rank[1]:
        #         del item
        #         continue
        # if extroversion_index:
        #     if extroversion_rank[0] <= int(item['_source']['extroversion_index']) < extroversion_rank[1]:
        #         del item
        #         continue
        # if nervousness_index:
        #     if nervousness_rank[0] <= int(item['_source']['nervousness_index']) < nervousness_rank[1]:
        #         del item
        #         continue
        # if openn_index:
        #     if openn_rank[0] <= int(item['_source']['openn_index']) < openn_rank[1]:
        #         del item
        #         continue
        # if agreeableness_index:
        #     if agreeableness_rank[0] <= int(item['_source']['agreeableness_index']) < agreeableness_rank[1]:
        #         del item
        #         continue
        # if conscientiousness_index:
        #     if conscientiousness_rank[0] <= int(item['_source']['conscientiousness_index']) < conscientiousness_rank[1]:
        #         del item
        #         continue

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
            item['_source']['dark_list'].append({'马基雅维利主义': '0'})
        if item['_source']['machiavellianism_label'] == 2:
            item['_source']['dark_list'].append({'马基雅维利主义': '1'})
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


def delete_by_id(index, doc_type, id):
    result = es.delete(index=index, doc_type=doc_type, id=id)
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


def get_basic_info(uid):
    star_query = {
        "query": {
            "bool": {
                "must": {
                    "term": {"uid": uid}
                }
            }
        }
    }
    political_bias_dic = {'left': '左倾', 'mid': '中立', 'right': '右倾'}
    result = es.get(index='user_information', doc_type='text', id=uid)['_source']
    star_result = es.search(index='user_ranking', doc_type='text', body=star_query)['hits']['hits'][-1]['_source']
    result['domain'] = labels_dict[result['domain']]
    result['political_bias'] = political_bias_dic[result['political_bias']]

    result['liveness_star'] = star_result['liveness_star']
    result['importance_star'] = star_result['importance_star']
    result['sensitive_star'] = star_result['sensitive_star']
    result['influence_star'] = star_result['influence_star']

    result['machiavellianism'] = star_result['machiavellianism_index']
    result['psychopathy'] = star_result['psychopathy_index']
    result['narcissism'] = star_result['narcissism_index']

    result['extroversion'] = star_result['extroversion_index']
    result['nervousness'] = star_result['nervousness_index']
    result['openn'] = star_result['openn_index']
    result['agreeableness'] = star_result['agreeableness_index']
    result['conscientiousness'] = star_result['conscientiousness_index']

    personality_status = {}
    # 大五人格
    if star_result['extroversion_label'] == 0:
        personality_status['extroversion'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['extroversion_index'], 'extroversion_index', 'low'))/100))
    if star_result['extroversion_label'] == 1:
        personality_status['extroversion'] = r''
    if star_result['extroversion_label'] == 2:
        personality_status['extroversion'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['extroversion_index'], 'extroversion_index', 'high'))/100))

    if star_result['openn_label'] == 0:
        personality_status['openn'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['openn_index'], 'openn_index', 'low'))/100))
    if star_result['openn_label'] == 1:
        personality_status['openn'] = r''
    if star_result['openn_label'] == 2:
        personality_status['openn'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['openn_index'], 'openn_index', 'high'))/100))

    if star_result['agreeableness_label'] == 0:
        personality_status['agreeableness'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['agreeableness_index'], 'agreeableness_index', 'low'))/100))
    if star_result['agreeableness_label'] == 1:
        personality_status['agreeableness'] = r''
    if star_result['agreeableness_label'] == 2:
        personality_status['agreeableness'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['agreeableness_index'], 'agreeableness_index', 'high'))/100))

    if star_result['conscientiousness_label'] == 0:
        personality_status['conscientiousness'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['conscientiousness_index'], 'conscientiousness_index', 'low'))/100))
    if star_result['conscientiousness_label'] == 1:
        personality_status['conscientiousness'] = r''
    if star_result['conscientiousness_label'] == 2:
        personality_status['conscientiousness'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['conscientiousness_index'], 'conscientiousness_index', 'high'))/100))

    if star_result['nervousness_label'] == 0:
        personality_status['nervousness'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['nervousness_index'], 'nervousness_index', 'low'))/100))
    if star_result['nervousness_label'] == 1:
        personality_status['nervousness'] = r''
    if star_result['nervousness_label'] == 2:
        personality_status['nervousness'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['nervousness_index'], 'nervousness_index', 'high'))/100))

    # 黑暗人格
    if star_result['machiavellianism_label'] == 0:
        personality_status['machiavellianism'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['machiavellianism_index'], 'machiavellianism_index', 'low'))/100))
    if star_result['machiavellianism_label'] == 1:
        personality_status['machiavellianism'] = r''
    if star_result['machiavellianism_label'] == 2:
        personality_status['machiavellianism'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['machiavellianism_index'], 'machiavellianism_index', 'high'))/100))

    if star_result['psychopathy_label'] == 0:
        personality_status['psychopathy'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['psychopathy_index'], 'psychopathy_index', 'low'))/100))
    if star_result['psychopathy_label'] == 1:
        personality_status['psychopathy'] = r''
    if star_result['psychopathy_label'] == 2:
        personality_status['psychopathy'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['psychopathy_index'], 'psychopathy_index', 'high'))/100))

    if star_result['narcissism_label'] == 0:
        personality_status['narcissism'] = r'低于{}%的人'.format(str(int(10000 * get_index_rank(star_result['narcissism_index'], 'narcissism_index', 'low'))/100))
    if star_result['narcissism_label'] == 1:
        personality_status['narcissism'] = r''
    if star_result['narcissism_label'] == 2:
        personality_status['narcissism'] = r'高于{}%的人'.format(str(int(10000 * get_index_rank(star_result['narcissism_index'], 'narcissism_index', 'high'))/100))

    result['personality_status'] = personality_status

    return result


def user_emotion(uid, interval):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": uid
                        }
                    },
                    {
                        "range": {
                            "date": {
                                "gte": THREE_MONTH_AGO,
                                "lte": TODAY
                            }
                        }
                    }
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
    print(query_body)
    buckets = es.search(index="user_emotion", doc_type="text", body=query_body)['aggregations']['groupDate']['buckets']
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

def get_user_behavior(uid, interval):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": uid
                        }
                    },
                    {
                        "range": {
                            "date": {
                                "gte": THREE_MONTH_AGO,
                                "lte": TODAY
                            }
                        }
                    }
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
                    "original": {
                        "sum": {
                            "field": "original"
                        }
                    },
                    "retweet": {
                        "sum": {
                            "field": "retweet"
                        }
                    },
                    "comment": {
                        "sum": {
                            "field": "comment"
                        }
                    }
                }
            }
        }
    }

    buckets = es.search(index="user_weibo_type", doc_type="text", body=query_body)['aggregations']['groupDate']['buckets']
    result = {
        'time': [],
        "original_line": [],
        "retweet_line": [],
        "comment_line": []
    }
    for bucket in buckets:
        result['time'].append(bucket['key_as_string'], )
        result["original_line"].append(bucket['original']['value'], )
        result["retweet_line"].append(bucket['retweet']['value'], )
        result["comment_line"].append(bucket['comment']['value'])
    return result


def get_user_activity(uid):

    result = {}

    # ip一天排名
    one_day_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": str(uid)
                        }
                    },
                    {
                        "term": {
                            "timestamp": str(datetime2ts(yesterday(TODAY)))
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "count": {
                    "order": "desc"
                }
            }
        ]
    }

    one_day_ip_rank = []
    one_day_result = es.search(index='user_activity', doc_type='text', body=one_day_query)['hits']['hits']
    one_day_geo_item = {}
    for i in range(5):
        try:
            one_day_geo_item.setdefault(re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', one_day_result[i]['_source']['geo'].split('&')[-1]), 0)
            one_day_geo_item[re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', one_day_result[i]['_source']['geo'].split('&')[-1])] += one_day_result[i]['_source']['count']
            item = {'rank': i+1, 'count': one_day_result[i]['_source']['count'], 'ip': one_day_result[i]['_source']['ip'], 'geo': one_day_result[i]['_source']['geo']}
        except:
            item = {'rank': i + 1, 'count': '-', 'ip': '-', 'geo': '-'}
        one_day_ip_rank.append(item)
    # print(one_day_ip_rank)

    one_day_geo_rank = []
    one_day_geo_sorted = sorted(one_day_geo_item.items(), key=lambda x: x[1], reverse=True)
    for i in range(5):
        # print(one_day_geo_sorted[i])
        try:
            one_day_geo_rank.append({'rank': i + 1, 'count': int(one_day_geo_sorted[i][1]), 'geo': one_day_geo_sorted[i][0]})
        except:
            one_day_geo_rank.append({'rank': i + 1, 'count': '-', 'geo': '-'})

    # ip一周排名
    one_week_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": str(uid)
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "gte": str(datetime2ts(yesterday(A_WEEK_AGO))),
                                "lte": str(datetime2ts(yesterday(TODAY)))
                            }
                        }
                    }
                ]
            }
        },
        "size": 0,
        "aggs": {
            "ip_count": {
                "terms": {
                    "field": "ip"
                },
                "aggs": {
                    "ip_count": {
                        "stats": {
                            "field": "count"
                        }
                    }
                }
            }
        }
    }
    # print('one_week_query', one_week_query)
    one_week_ip_rank = []
    one_week_result = \
    es.search(index='user_activity', doc_type='text', body=one_week_query)['aggregations']['ip_count']['buckets']
    one_week_dic = {}
    for one_week_data in one_week_result:
        one_week_dic[one_week_data['key']] = one_week_data['ip_count']['sum']
    # print(one_week_dic)
    l = sorted(one_week_dic.items(), key=lambda x: x[1], reverse=True)
    for i in range(5):
        try:
            item = {'rank': i + 1, 'count': int(l[i][1]), 'ip': l[i][0]}
            item['geo'] = re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', es.search(index='user_activity', doc_type='text', body={"query":{"bool":{"must":[{"term":{"ip":l[i][0]}}]}},"size":1})['hits']['hits'][0]['_source']['geo'])
        except:
            item = {'rank': i + 1, 'count': '-', 'ip': '-', 'geo': '-'}
        one_week_ip_rank.append(item)
    # print(one_week_ip_rank)
    # 活跃度分析
    geo_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": str(uid)
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "gte": str(datetime2ts(yesterday(A_WEEK_AGO))),
                                "lte": str(datetime2ts(yesterday(TODAY)))
                            }
                        }
                    }
                ],
                "must_not": [],
                "should": []
            }
        },
        "size": 1000,
        "sort": [
            {
                "timestamp": {
                    "order": "asc"
                }
            }
        ]
    }

    geo_result = es.search(index='user_activity', doc_type='text', body=geo_query)['hits']['hits']
    one_week_geo_rank = []
    if geo_result:
        geo_dict = {}
        one_week_geo_dict = {}
        for geo_data in geo_result:
            # item = {}
            one_week_geo_dict.setdefault(re.sub(r'省|市|壮族|维吾尔族|回族|自治区', r'', geo_data['_source']['geo']), 0)
            one_week_geo_dict[re.sub(r'省|市|壮族|维吾尔族|回族|自治区', r'', geo_data['_source']['geo'])] += geo_data['_source']['count']
            geo_dict.setdefault(ts2datetime(geo_data['_source']['timestamp']), {})
            try:
                if geo_data['_source']['geo'].split('&')[1] == '其他':
                    continue
                if geo_data['_source']['geo'].split('&')[0] != '中国':
                    continue
                # geo_dict[geo_data['_source']['date']].setdefault(re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', geo_data['_source']['geo'].split('&')[1]), 0)
                geo_dict[ts2datetime(geo_data['_source']['timestamp'])].setdefault(re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', geo_data['_source']['geo']), 0)
            except:
                continue
            # geo_dict[geo_data['_source']['date']][re.sub(r'省|市|壮族|维吾尔族|回族|自治区', r'', geo_data['_source']['geo'].split('&')[1])] += geo_data['_source']['count']
            geo_dict[ts2datetime(geo_data['_source']['timestamp'])][re.sub(r'省|市|壮族|维吾尔族|回族|自治区', '', geo_data['_source']['geo'])] += geo_data['_source']['count']

        one_week_geo_sorted = sorted(one_week_geo_dict.items(), key=lambda x: x[1], reverse=True)
        for i in range(5):
            try:
                one_week_geo_item = {'rank': i + 1, 'count': int(one_week_geo_sorted[i][1]), 'geo': one_week_geo_sorted[i][0]}
            except:
                one_week_geo_item = {'rank': i + 1, 'count': '-', 'geo': '-'}
            one_week_geo_rank.append(one_week_geo_item)

        geo_dict_item = list(geo_dict.items())
        route_list = []
        for i in range(len(geo_dict_item)):
            if not geo_dict_item[i][1]:
                continue
            item = {'s': max(geo_dict_item[i][1], key=geo_dict_item[i][1].get).split('&')[1], 'e': ''}
            route_list.append(item)
            # print('maxmaxmax', max(geo_dict_item[i][1], key=geo_dict_item[i][1].get))
            if i > 0:
                route_list[i - 1]['e'] = max(geo_dict_item[i][1], key=geo_dict_item[i][1].get).split('&')[1]

        if len(route_list) > 1:
            del (route_list[-1])
        elif len(route_list) == 1:
            route_list[0]['e'] = route_list[0]['s']
        # print(route_list)
    else:
        for i in range(5):
            one_week_geo_rank.append({'rank': i + 1, 'count': '-', 'geo': '-'})
        route_list = []

    for item in route_list:
        if not (item['s'] and item['e']):
            route_list.remove(item)
    result['one_day_ip_rank'] = one_day_ip_rank
    result['one_day_geo_rank'] = one_day_geo_rank
    result['one_week_ip_rank'] = one_week_ip_rank
    result['one_week_geo_rank'] = one_week_geo_rank
    result['route_list'] = route_list
    # result['route_list'] = [route for route in route_list if route['s'] != route['e']]

    return result


def get_preference_identity(uid):
    result = {}
    today_ts = int(time.mktime(time.strptime(TODAY, '%Y-%m-%d')))
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": str(uid)
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "lte": str(int(today_ts))
                            }
                        }
                    }
                ],
                "must_not": []
            }
        },
        "size": 1,
        "sort": [
            {
                "timestamp": {
                    "order": "desc"
                }
            }
        ]
    }

    analysis_result = es.search(index='user_text_analysis_sta', doc_type='text', body=query)['hits']['hits']
    if not analysis_result:
        return {'topic_result':{},'domain_dict':{}}
    analysis_result = analysis_result[0]['_source']
    result['keywords'] = {}
    if analysis_result['keywords']:
        for i in analysis_result['keywords']:
            result['keywords'].update({i['keyword']: i['count']})

    result['hastags'] = {}
    if analysis_result['hastags']:
        for i in analysis_result['hastags']:
            result['hastags'].update({i['hastag']: i['count']})

    result['sensitive_words'] = {}
    if analysis_result['sensitive_words']:
        # print(analysis_result['sensitive_words'])
        for i in analysis_result['sensitive_words']:
            result['sensitive_words'].update({i['sensitive_word']: i['count']})

    query['query']['bool']['must'].append({"term": {"has_new_information": "1"}})
    query['query']['bool']['must_not'].append({"constant_score": {"filter": {"missing": {"field": "topic_computer"}}}})
    query['query']['bool']['must_not'].append({"constant_score": {"filter": {"missing": {"field": "main_domain"}}}})
    # print(query)
    preference_and_topic_data = es.search(index='user_domain_topic', doc_type='text', body=query)['hits']['hits']
    # preference_and_topic_data = es.search(index='user_domain_topic', doc_type='text', body=query)['hits']['hits'][0]['_source']
    if not preference_and_topic_data:
        return {'topic_result':{},'domain_dict':{}}
    preference_and_topic_data = preference_and_topic_data[0]['_source']
    preference_item = {}
    topic_result = {}
    for k, v in preference_and_topic_data.items():
        if k.startswith('topic_'):
            preference_item[k] = v
    l = sorted(preference_item.items(), key=lambda x:x[1], reverse=True)[0:5]
    sum_topic = sum([i[1] for i in l])
    for i in range(0,5):
        topic_result[topic_dict[l[i][0].replace('topic_', '')]] = l[i][1]/sum_topic
    node_main = {'name': labels_dict[preference_and_topic_data['main_domain']], 'id': preference_and_topic_data['uid']}
    node_followers = {'name': labels_dict[preference_and_topic_data['domain_followers']]}
    node_verified = {'name': labels_dict[preference_and_topic_data['domain_verified']]}
    node_weibo = {'name': labels_dict[preference_and_topic_data['domain_weibo']]}
    m_to_f_link = {'source': node_main, 'target': node_followers, 'relation': '根据转发结构分类'}
    m_to_v_link = {'source': node_main, 'target': node_verified, 'relation': '根据注册信息分类'}
    m_to_w_link = {'source': node_main, 'target': node_weibo, 'relation': '根据发帖内容分类'}
    node = [node_main, node_followers, node_verified, node_weibo]
    link = [m_to_f_link, m_to_v_link, m_to_w_link]
    domain_dict = {'node': node, 'link': link}

    result['topic_result'] = topic_result
    result['domain_dict'] = domain_dict

    return result


def get_influence_feature(uid,interval):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": uid
                        }
                    },
                    {
                        "range": {
                            "date": {
                                "gte": THREE_MONTH_AGO,
                                "lte": TODAY
                            }
                        }
                    }
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
                            "field": "sensitivity_normalization"
                        }
                    },
                    "influence": {
                        "sum": {
                            "field": "influence_normalization"
                        }
                    },
                    "activity": {
                        "sum": {
                            "field": "activity_normalization"
                        }
                    },
                    "importance": {
                        "sum": {
                            "field": "importance_normalization"
                        }
                    }
                }
            }
        }
    }
    es_result = es.search(index="user_influence", doc_type="text", body=query_body)["aggregations"]["groupDate"]["buckets"]
    result_list = []
    for data in es_result:
        item = {}
        item['sensitivity'] = data['sensitivity']['value']
        item['influence'] = data['influence']['value']
        item['activity'] = data['activity']['value']
        item['importance'] = data['importance']['value']
        item['timestamp'] = data['key']//1000
        item['date'] = data['key_as_string']
        result_list.append(item)
    return result_list


# def user_influence(user_uid):
#     query_body = {
#         "query": {
#                 "filtered": {
#                     "filter": {
#                         "bool": {
#                             "must": [{
#                                 "term":{
#                                     "uid": user_uid
#
#                                 }
#                             }
#                             ]
#                         }
#                     }
#                 }
#             },
#         "size": 1000
#     }
#
#     es_result = es.search(index="user_influence", doc_type="text", body=query_body)["hits"]["hits"]  # 默认取第0条一个用户的最新一条
#
#     return es_result

@cache.memoize(60)
def user_social_contact(uid, map_type):
    # map_type 1 2 3 4 转发 被转发 评论 被评论
    # message_type 1 原创 2 评论 3转发
    if map_type in ['1', '2']:
        message_type = 3
    else:
        message_type = 2
    if map_type in ['1', '3']:
        key = 'target'
        key2 = 'source'
    else:
        key = 'source'
        key2 = 'target'
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool":{
                            "must":[
                                {
                                    "term": {
                                        'source': uid
                                    }
                                },
                                {
                                    "term": {
                                        "message_type": message_type
                                    }
                                },
                                {
                                    "range": {
                                        "date": {
                                            "gte": THREE_MONTH_AGO,
                                            "lte": TODAY
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        'target': uid
                                    }
                                },
                                {
                                    "term": {
                                        "message_type": message_type
                                    }
                                },
                                {
                                    "range": {
                                        "date": {
                                            "gte": THREE_MONTH_AGO,
                                            "lte": TODAY
                                        }
                                    }
                                }
                            ]
                        }
                    },

                ],
                "must": []
            }
        },
        "size": 1000,
    }
    print(query_body)
    r1 = es.search(index="user_social_contact", doc_type="text",
                   body=query_body)["hits"]["hits"]
    node = []
    link = []
    key_list = []
    for one in r1:
        item = one['_source']
        key_list.append(item[key2])
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
                                    key: key_list
                                }
                            }
                        ]}
                }}
        },
        "size": 3000,
    }
    r2 = es.search(index="user_social_contact",
                       doc_type="text", body=query_body)["hits"]["hits"]
    r1 += r2
    for one in r1:
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


def user_preference(user_uid):
    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [{
                            "term": {
                                "uid": user_uid

                            }
                        }
                        ]
                    }
                }
            }
        },
        "size": 1000
    }

    es_result = es.search(index="user_preference", doc_type="text", body=query_body)["hits"]["hits"][
        0]  # 默认取第0条一个用户的最新一条
    return es_result

def user_add_one_task(username, uid, gender, description, user_location, friends_num, create_at, weibo_num, user_birth, isreal, photo_url, fans_num, insert_time, progress):
    try:
        gender = int(gender)
        if gender not in [0,1]:
            gender_check = 0
        else:
            gender_check = 1
    except:
        gender_check = 0
    try:
        isreal = int(isreal)
        if isreal not in [0,1]:
            isreal_check = 0
        else:
            isreal_check = 1
    except:
        isreal_check = 0
    try:
        friends_num = int(friends_num)
        if friends_num < 0:
            friends_num_check = 0
        else:
            friends_num_check = 1
    except:
        friends_num_check = 0
    try:
        weibo_num = int(weibo_num)
        if weibo_num < 0:
            weibo_num_check = 0
        else:
            weibo_num_check = 1
    except:
        weibo_num_check = 0
    try:
        fans_num = int(fans_num)
        if fans_num < 0:
            fans_num_check = 0
        else:
            fans_num_check = 1
    except:
        fans_num_check = 0
    try:
        create_at = date2ts(create_at)
        create_at_check = 1
    except:
        create_at_check = 0

    if not gender_check:
        return {'status':0, 'info':'错误的性别输入'}
    if not isreal_check:
        return {'status':0, 'info':'错误的实名认证输入'}
    if not friends_num_check:
        return {'status':0, 'info':'错误的关注数输入'}
    if not weibo_num_check:
        return {'status':0, 'info':'错误的微博数输入'}
    if not fans_num_check:
        return {'status':0, 'info':'错误的粉丝数输入'}
    if not create_at_check:
        return {'status':0, 'info':'错误的创建时间输入'}

    try:
        es.get(index=USER_INFORMATION, doc_type='text', id=uid)
        return {'status':0, 'info':'用户已在库中'}
    except:
        pass
    dic = {
        'username': username,
        'uid': uid,
        'gender': gender,
        'description': description,
        'user_location': user_location,
        'friends_num': friends_num,
        'create_at': create_at,
        'weibo_num': weibo_num,
        'user_birth': user_birth,
        'isreal': isreal,
        'photo_url': photo_url,
        'fans_num': fans_num,
        'insert_time': insert_time,
        'progress': progress,
    }

    es.index(index=USER_INFORMATION, doc_type='text', id=uid, body=dic)
    return {'status':1, 'info':'成功插入用户，等待计算'}

def get_user_task_show():
    query_body = {
        'query':{
            'terms':{
                'progress':[0, 1]
            }
        },
        'sort':{
            'insert_time':{
                'order':'asc'
            }
        },
        'size':10000
    }
    res = es.search(index=USER_INFORMATION,doc_type='text',body=query_body)['hits']['hits']
    result = []
    for hit in res:
        dic = {}
        dic['username'] = hit['_source']['username']
        dic['uid'] = hit['_source']['uid']
        dic['create_at'] = ts2date(hit['_source']['create_at'])
        dic['progress'] = hit['_source']['progress']
        result.append(dic)

    return result

def delete_user_task(uid):
    try:
        data = es.get(index=USER_INFORMATION, doc_type='text', id=uid)
        if data['_source']['progress'] == 0:
            es.delete(index=USER_INFORMATION, doc_type='text', id=uid)
            return {'status':1, 'info':'删除成功'}
        else:
            return {'status':0, 'info':'删除失败，用户正在计算或已计算完成，请在对应页面删除'}
    except:
        return {'status':0, 'info':'删除失败，用户不在库中'}