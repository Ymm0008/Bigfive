# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../')
from config import *
from time_utils import *
from global_utils import *
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
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        for uid in uid_list:
            weibo_data_dict = get_weibo_data_dict(uid, date,date)
            cal_user_influence(uid,weibo_data_dict)


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-10'):
    #     daily_user_influence(date)
    theday = today()
    daily_user_influence(theday)