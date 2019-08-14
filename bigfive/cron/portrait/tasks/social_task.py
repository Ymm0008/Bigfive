# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
sys.path.append('../user/')
from config import *
from time_utils import *
from global_utils import *
from user.user_social import cal_user_social
from user.cron_user import get_weibo_data_dict

def daily_user_social(date):
    date = ts2date(int(date2ts(date))-86400)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 1000)
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
            cal_user_social(uid,weibo_data_dict)


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-10'):
    #     daily_user_social(date)
    theday = today()
    print('Calculating user social...')
    daily_user_social(theday)