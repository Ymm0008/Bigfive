# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../')
from config import *
from time_utils import *
from global_utils import *
from user.user_text_analyze import cal_user_text_analyze

def daily_user_text(date):   #凌晨计算昨天的文本信息
    date = ts2date(int(date2ts(date))-86400)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"match_all":{}}]}}}, 1000)
    while True:
        try:
            es_result = next(iter_result)
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        print(date)
        cal_user_text_analyze(uid_list, date, date)


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-10'):
    #     daily_user_text(date)
    theday = today()
    daily_user_text(theday)