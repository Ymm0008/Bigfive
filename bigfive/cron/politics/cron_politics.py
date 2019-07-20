import sys
import json
sys.path.append('../')
sys.path.append('../../')

from elasticsearch.helpers import bulk
from multiprocessing import Pool

from retreet_comment import weibo_retweet_comment

from cron_utils import text_rank_keywords, triple_classifier, from_ip_get_info

from politics_user import get_politics_user
from politics_topic import get_politics_topic

from config import *
from time_utils import *
from global_utils import *

#政策法规的创建，扫描未计算的任务，获得任务的基本参数开始计算，主要为了圈定符合条件的用户群体，利用关键词检索
#输入：从politics_information表中获得所需参数列表
#输出：发布满足条件微博的用户的列表
def politics_create(politics_mapping_name, keywords, start_date, end_date):
    uid_list_keyword = []
    keyword_list = keywords.split('&')
    keyword_query_list = [{"match_phrase":{"text":keyword}} for keyword in keyword_list]
    weibo_query_body = {
        "query":{
            'bool':{
            	'should':keyword_query_list,
                'minimum_should_match':2
            }
        }
    }
    
    date_list = get_datelist_v2(start_date, end_date)
    ###迭代日期进行模糊搜索并将微博存到一个新的事件索引中去
    for date in date_list:
        print(date)
        weibo_index = 'flow_text_%s' % date
        weibo_generator = get_weibo_generator(weibo_index, weibo_query_body, USER_WEIBO_ITER_COUNT)
        package = []
        midlist = []
        uid_list = []
        weibo_num = 0
        for res in weibo_generator:
            for hit in res:
                source = hit['_source']
                ###计算需要在取出事件相关微博数据的时候计算的指标
                keywords_string = '&'.join(text_rank_keywords(source['text']))
                sentiment = triple_classifier(source)
                geo = from_ip_get_info(source['ip'])

                dic = {
                    'root_uid':source['root_uid'],
                    'sentiment':sentiment,
                    'ip':source['ip'],
                    'user_fansnum':source['user_fansnum'],
                    'mid':source['mid'],
                    'message_type':source['message_type'],
                    'geo':geo,
                    'uid':source['uid'],
                    'root_mid':source['root_mid'],
                    'keywords_string':keywords_string,
                    'text':source['text'],
                    'timestamp':source['timestamp'],
                }
                package.append({
                    '_index': politics_mapping_name,  
                    '_type': "text",  
                    '_id':dic['mid'],
                    '_source': dic
                })
                midlist.append(source['mid'])
                uid_list.append(source['uid'])

                if weibo_num % 1000 == 0:
                    #获取用户昵称
                    if len(uid_list):
                        username_results = es.mget(index='weibo_user', doc_type='type1', body={'ids':uid_list})['docs']
                    else:
                        username_results = []
                    username_dic = {}
                    for item in username_results:
                        if item['found']:
                            username_dic[item['_id']] = item['_source']['name']
                        else:
                            username_dic[item['_id']] = item['_id']
                    #会根据这段时间内的微博计算一跳转发和评论数
                    retweet_dic = weibo_retweet_comment(midlist, start_date, end_date)
                    #增添新数据
                    for p in package:
                        p['_source'].update({'username':username_dic[p['_source']['uid']]})
                        try:
                            p['_source'].update(retweet_dic[p['_source']['mid']])
                        except KeyError:
                            p['_source'].update({'comment':0,'retweeted':0})
                    bulk(es, package)  #存入数据库
                    package = []
                    midlist = []
                    uid_list = []
                    
                weibo_num += 1
                uid_list_keyword.append(source['uid'])

        #存入剩下的数据
        if len(uid_list):
            username_results = es.mget(index='weibo_user', doc_type='type1', body={'ids':uid_list})['docs']
        else:
            username_results = []
        username_dic = {}
        for item in username_results:
            if item['found']:
                username_dic[item['_id']] = item['_source']['name']
            else:
                username_dic[item['_id']] = item['_id']
        retweet_dic = weibo_retweet_comment(midlist, start_date, end_date)
        for p in package:
            p['_source'].update({'username':username_dic[p['_source']['uid']]})
            try:
                p['_source'].update(retweet_dic[p['_source']['mid']])
            except KeyError:
                p['_source'].update({'comment':0,'retweeted':0})
        bulk(es, package)

    uid_list_keyword = list(set(uid_list_keyword))
    iter_num = 0
    uid_list = []
    while (iter_num*USER_WEIBO_ITER_COUNT <= len(uid_list_keyword)):
        iter_uid_list_keyword = uid_list_keyword[iter_num*USER_WEIBO_ITER_COUNT : (iter_num + 1)*USER_WEIBO_ITER_COUNT]
        if len(iter_uid_list_keyword):
            iter_user_dict_list = es.mget(index='weibo_user', doc_type='type1', body={'ids':iter_uid_list_keyword})['docs']
        else:
            iter_user_dict_list = []
        uid_list.extend([i['_id'] for i in iter_user_dict_list if i['found']])
        iter_num += 1

    print('Uid_list_keyword num:%d' % len(uid_list))
    return uid_list


# 给定政策事件，计算其正向负向文本，对正负向文本分出大V和普通用户，再对每种用户进行主题模型聚类
def politics_portrait(politics_id, politics_mapping_name, user_list, start_date, end_date):
    print('Start politics user...')  
    mid_dict = get_politics_user(politics_id, politics_mapping_name, user_list)

    print('Start politics topic...') 
    get_politics_topic(politics_id, politics_mapping_name, mid_dict)

if __name__ == "__main__":
	politics_create()