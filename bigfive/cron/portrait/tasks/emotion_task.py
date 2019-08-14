# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
sys.path.append('../user/')
sys.path.append('../user/sentiment_classification')
from multiprocessing import Pool
from config import *
from time_utils import *
from global_utils import *
from user.user_emotion import cal_user_emotion
from user.cron_user import get_weibo_data_dict

def daily_user_emotion(date):
    date = ts2date(int(date2ts(date)) - DAY)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    iter_num = 0
    while True:
        try:
            es_result = next(iter_result)
            iter_num += 1
            print(iter_num)
            if iter_num <= 76:
                continue
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        for uid in uid_list:
            weibo_data_dict = get_weibo_data_dict(uid, date,date)
            cal_user_emotion(uid,weibo_data_dict)

def multi_daily_user_emotion(date):    #从Redis取出任务逐个计算
    while True:
        uid = redis_r.rpop('emotion_task_%s' % date)
        if uid == None:
            break
        uid = uid.decode('utf-8')  # redis存的是utf-8类型，Python3要解码一下
        print(uid,date)
        weibo_data_dict = get_weibo_data_dict(uid, date, date)
        cal_user_emotion(uid, weibo_data_dict)

def multi_main_emotion(date):
    #任务推入redis
    date = ts2date(int(date2ts(date)) - DAY)   #注意这里的取昨天的操作，算历史别算错了
    redis_r.delete('emotion_task_%s' % date)
    user_generator = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 100000)
    for res in user_generator:
        userlist = [i['_source']['uid'] for i in res]
        redis_r.lpush('emotion_task_%s' % date, *userlist)
    print('emotion task is set...')

    #多进程计算加快速度
    p = Pool(8)
    for i in range(8):
        p.apply_async(multi_daily_user_emotion, args=(date,))
    p.close()
    p.join()   #主进程推动子进程，如果不加join主进程很快结束，子进程也无法进行，join会让主进程一直等待子进程
    print('calculate over...')


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-11'):
    #     daily_user_emotion(date)
    # theday = today()
    # print('Calculating user emotion...')
    # daily_user_emotion(theday)
    date = today()
    multi_main_emotion(date)
    #uid = "1878219871"
    #date = "2019-04-16"
    #weibo_data_dict = get_weibo_data_dict(uid, date, date)
    #cal_user_emotion(uid, weibo_data_dict)
