# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
sys.path.append('../user/')
from multiprocessing import Pool
from config import *
from time_utils import *
from global_utils import *
from portrait.cron_portrait import user_ranking
from user.user_influence import cal_user_influence
from user.cron_user import get_weibo_data_dict

def daily_user_influence(date):
    date = ts2date(int(date2ts(date)) - DAY)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    while True:
        try:
            es_result = next(iter_result)
        except:
            break
        uid_list = []
        username_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])
            try:
                username_list.append(es_result[k]["_source"]["username"])
            except:
                username_list.append("")
        for uid in uid_list:
            weibo_data_dict = get_weibo_data_dict(uid, date,date)
            cal_user_influence(uid,weibo_data_dict)

    start_date = ts2date(int(date2ts(date)) - 15*DAY)
    normalize_influence_index(start_date,date,15)

    iter_ranking_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 1000)
    while True:
        try:
            ranking_result = next(iter_ranking_result)
        except:
            break
        uid_ranking_list = []
        username_ranking_list = []
        for k,v in enumerate(ranking_result):
            uid_ranking_list.append(ranking_result[k]["_source"]["uid"])
            username_ranking_list.append(ranking_result[k]["_source"]["username"])
        user_ranking(uid_ranking_list, username_ranking_list, date)

def daily_user_influence_only(date):
    date = ts2date(int(date2ts(date)) - DAY)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    iter_num = 0
    while True:
        try:
            es_result = next(iter_result)
            iter_num += 1
            print(iter_num)
            if iter_num <= 74:
                continue
        except:
            break
        uid_list = []
        username_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])
            try:
                username_list.append(es_result[k]["_source"]["username"])
            except:
                username_list.append("")
        for uid in uid_list:
            weibo_data_dict = get_weibo_data_dict(uid, date,date)
            cal_user_influence(uid,weibo_data_dict)

def multi_daily_user_influence(date):    #从Redis取出任务逐个计算
    date = ts2date(int(date2ts(date)) - DAY)
    uid = redis_r.rpop('influence_task_%s' % date)
    weibo_data_dict = get_weibo_data_dict(uid, date,date)
    cal_user_influence(uid,weibo_data_dict)


def multi_main(date):
    #任务推入redis
    redis_r.delete('influence_task_%s' % date)
    user_generator = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    for res in user_generator:
        userlist = [i['_source']['uid'] for i in res]
        redis_r.lpush('influence_task_%s' % date, *userlist)

    #多进程计算加快速度
    p = Pool(5)
    for i in range(5):
        p.apply_async(multi_daily_user_influence, args=(date,))
    p.close()
    p.join()   #主进程推动子进程，如果不加join主进程很快结束，子进程也无法进行，join会让主进程一直等待子进程

    # start_date = ts2date(int(date2ts(date)) - 15*DAY)
    # normalize_influence_index(start_date,date,15)

    # iter_ranking_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 1000)
    # while True:
    #     try:
    #         ranking_result = next(iter_ranking_result)
    #     except:
    #         break
    #     uid_ranking_list = []
    #     username_ranking_list = []
    #     for k,v in enumerate(ranking_result):
    #         uid_ranking_list.append(ranking_result[k]["_source"]["uid"])
    #         username_ranking_list.append(ranking_result[k]["_source"]["username"])
    #     user_ranking(uid_ranking_list, username_ranking_list, date)

def test_print():
    import time
    while True:
        item = redis_r.rpop('test')
        if item == None:
            break
        print(item)
        time.sleep(1)

def multi_test():
    import random
    redis_r.delete('test')
    for i in range(200):
        redis_r.lpush('test',random.random())
    print('over!')

    p = Pool(5)
    for i in range(5):
        p.apply_async(test_print, args=())
    p.close()
    p.join()

if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-29','2019-03-29'):
    #     daily_user_influence_only(date)
    # theday = today()
    # print('Calculating user influence...')
    # daily_user_influence(theday)
    # multi_main('2019-03-30')
    multi_test()