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
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        for uid in uid_list:
            get_user_domain(uid,date,date)


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-04-10','2019-04-10'):
    #     weekly_user_domain(date)
    weekday = datetime.datetime.now().weekday()
    theday = ts2date(date2ts(today()) - 86400)   #因为domain和topic的逻辑是互相更新，所以周一算topic之后，周二算domain的时候算的是周一的domain，更新到周一里
    if weekday == 2:
        print("Calculating user domain...")
        weekly_user_domain(theday)
    else:
        print("not reach calculating user doamin time")
        pass
    # weekly_user_domain(theday)

    # theday = today()
    # weekly_user_domain(theday)