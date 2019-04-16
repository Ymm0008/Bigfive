# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../')
from multiprocessing import Pool
from config import *
from time_utils import *
from global_utils import *
from user.user_ip import get_user_activity

def daily_user_position(date):
    date = ts2date(int(date2ts(date))-86400)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    while True:
        try:
            es_result = next(iter_result)
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        for uid in uid_list:
            get_user_activity(uid,date,date)

def multi_daily_user_position(date):    #从Redis取出任务逐个计算
    while True:
        uid = redis_r.rpop('position_task_%s' % date)
        if uid == None:
            break
        uid = uid.decode('utf-8')  # redis存的是utf-8类型，Python3要解码一下
        print(uid,date)
        get_user_activity(uid,date,date)

def multi_main_position(date):
    #任务推入redis
    date = ts2date(int(date2ts(date)) - DAY)   #注意这里的取昨天的操作，算历史别算错了
    redis_r.delete('position_task_%s' % date)
    user_generator = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 100000)
    for res in user_generator:
        userlist = [i['_source']['uid'] for i in res]
        redis_r.lpush('position_task_%s' % date, *userlist)
    print('position task is set...')

    #多进程计算加快速度
    p = Pool(8)
    for i in range(8):
        p.apply_async(multi_daily_user_position, args=(date,))
    p.close()
    p.join()   #主进程推动子进程，如果不加join主进程很快结束，子进程也无法进行，join会让主进程一直等待子进程
    print('calculate over...')

if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-10'):
    #     daily_user_social(date)
    # theday = today()
    # print('Calculating user position...')
    # daily_user_position(theday)
    date = '2019-04-12'
    multi_main_position(date)