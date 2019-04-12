# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
sys.path.append('../user/')
sys.path.append('../user/sentiment_classification')
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


if __name__ == '__main__':
    for date in get_datelist_v2('2019-03-30','2019-04-11'):
        daily_user_emotion(date)
    # theday = today()
    # print('Calculating user emotion...')
    # daily_user_emotion(theday)