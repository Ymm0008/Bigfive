# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../')
import datetime
from config import *
from time_utils import *
from global_utils import *
from user.user_domain import get_user_domain



def weekly_user_domain(date):
    date = ts2date(int(date2ts(date)) - DAY)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 1000)
    iter_num = 0
    while True:
        try:
            es_result = next(iter_result)
            iter_num += 1
            print(iter_num)
            if iter_num <= 130:
                continue
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        for uid in uid_list:
            get_user_domain(uid,date,date)


if __name__ == '__main__':
    for date in get_datelist_v2('2019-04-10','2019-04-10'):
        weekly_user_domain(date)
    # weekday = datetime.datetime.now().weekday()
    # theday = today()
    # if weekday == 2:
    #     print("Calculating user domain...")
    #     weekly_user_domain(theday)
    # else:
    #     print("not reach calculating user doamin time")
    #     pass


    # theday = today()
    # weekly_user_domain(theday)