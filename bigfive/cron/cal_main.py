# -*- coding: UTF-8 -*-


import time
import sys
import json
sys.path.append('../')
sys.path.append('portrait/user')
sys.path.append('portrait/group')
sys.path.append('event')
sys.path.append('politics')
sys.path.append('event/event_river')
from xpinyin import Pinyin
#注意：分词工具只需要初始化一次即可，多次初始化会出现线程问题！

from config import *
from time_utils import *
from global_utils import *
from portrait.cron_portrait import user_ranking, cal_user_personality, group_create, group_ranking, cal_group_personality
from portrait.user.cron_user import user_portrait
from portrait.group.cron_group import group_portrait
from cron_event import event_create, get_text_analyze, event_portrait
from event_mapping import create_event_mapping
from portrait.user.user_text_analyze import cal_user_text_analyze
from cron_politics import politics_create, politics_portrait
from politics_mapping import create_politics_mapping
from cron.portrait.user.normalizing import normalize_influence_index

#对用户进行批量计算，流数据接入时会自动入库批量计算
def user_main(uid_list, username_list, start_date, end_date):
    # print('Start calculating user personality...')
    # cal_user_personality(uid_list, start_date, end_date)  #调用模型计算用户人格，并存入USER_PERSONALITY

    # print('Start calculating user text...')
    # cal_user_text_analyze(uid_list, start_date, end_date)#对一群用户计算过去指定时间段内每天的关键词、敏感词和微话题

    print('Start calculating user portrait...')
    for uid in uid_list:  #计算每个用户的位置、话题、领域、活动、政治倾向、词云、情绪、社交、影响力
        # print(uid)
        user_portrait(uid, start_date ,end_date)
    #
    # print('Start calculating user ranking...')
    # user_ranking(uid_list, username_list, end_date)
    #
    # print('Successfully create user...')

#检测任务表，有新任务会进行计算，默认取计算时间段的结束日期为创建日期，开始日期为结束日期前的n天
def group_main(args_dict, keyword, remark, group_name, create_time):
    days = 15
    end_date = ts2date(create_time)
    start_date = ts2date(create_time - days * 24 *3600)
    print('Start finding userlist...')
    group_dic = group_create(args_dict, keyword, remark, group_name, create_time, start_date, end_date)
    if len(group_dic['userlist']) == 0:
    	return 0

    print('Start calculating group personality...')
    cal_group_personality(group_dic['group_id'], group_dic['userlist'], end_date)

    print('Start calculating group portrait...')
    group_portrait(group_dic['group_id'], group_dic['userlist'], start_date, end_date)
    
    print('Start calculating group ranking...')
    group_ranking(group_dic['group_id'], group_dic['group_name'], group_dic['userlist'], end_date)

    print('Successfully create group...')
    return 1


def event_main(keywords, event_id, start_date, end_date):
    print('Start creating event...')
    event_mapping_name = 'event_%s' % event_id
    create_event_mapping(event_mapping_name)
    userlist = event_create(event_mapping_name, keywords, start_date, end_date)
    es.update(index=EVENT_INFORMATION,doc_type='text',body={'doc':{'userlist':userlist}},id=event_id)

    print('Start text analyze...')
    get_text_analyze(event_id, event_mapping_name)
    
    print('Start event portrait...')
    # userlist = es.get(index='event_information',doc_type='text',id=event_id)['_source']['userlist']
    event_portrait(event_id, event_mapping_name, userlist, start_date, end_date)

    print('Successfully create event...')

def politics_main(keywords, politics_id, start_date, end_date):
    print('Start creating politics...')
    politics_mapping_name = 'politics_%s' % politics_id
    create_politics_mapping(politics_mapping_name)
    userlist = politics_create(politics_mapping_name, keywords, start_date, end_date)
    es.update(index=POLITICS_INFORMATION,doc_type='text',body={'doc':{'userlist':userlist}},id=politics_id)
    
    print('Start politics portrait...')
    # userlist = es.get(index='politics_information',doc_type='text',id=politics_id)['_source']['userlist']
    politics_portrait(politics_id, politics_mapping_name, userlist, start_date, end_date)

    print('Successfully create politics...')




if __name__ == '__main__':

    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    # while True:
    #     try:
    #         es_result = next(iter_result)
    #         uid_list = []
    #         username_list = []
    #         for k,v in enumerate(es_result):
    #             uid_list.append(es_result[k]["_source"]["uid"])
    #             username_list.append(es_result[k]["_source"]["username"])
    #         user_main(uid_list,username_list,'2019-03-29','2019-04-10')
    #     except StopIteration:
    #         normalize_influence_index('2019-03-29','2019-04-10',13)
    #         break

    while True:
        try:
            es_result = next(iter_result)
            uid_list = []
            username_list = []
            for k,v in enumerate(es_result):
                uid_list.append(es_result[k]["_source"]["uid"])
                try:
                    username_list.append(es_result[k]["_source"]["username"])
                except:
                    username_list.append("")
            user_main(uid_list,username_list,'2019-03-29','2019-04-10')
        except StopIteration:
            break