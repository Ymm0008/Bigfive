# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../')
from config import *
from time_utils import *
from global_utils import *
from cron_portrait import cal_user_personality

def monthly_user_emotion(date):
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

        start_date = ts2date(date2ts(date) - 15*DAY)
        cal_user_personality(uid_list, start_date, date)


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-10'):
    #     monthly_user_emotion(date)
    theday = today()
    monthly_user_emotion(theday)