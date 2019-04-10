# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
from config import *
from time_utils import *
from global_utils import *
from user_text_analyze import cal_user_text_analyze

def get_user_text(start_date):   #凌晨计算昨天的文本信息
    start_date = ts2date(int(date2ts(start_date))-86400)
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

        print(start_date)
        end_date = start_date
        cal_user_text_analyze(uid_list, start_date, end_date)


if __name__ == '__main__':
    for date in get_datelist_v2('2019-03-30','2019-04-10'):
        get_user_text(date)