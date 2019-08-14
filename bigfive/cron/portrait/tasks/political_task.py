# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
import datetime
from config import *
from time_utils import *
from global_utils import *
from user.user_political import get_user_political

def weekly_user_political(date):
    date = ts2date(int(date2ts(date)) - DAY)
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
            get_user_political(uid, date, date)


if __name__ == '__main__':
    # for date in get_datelist_v2('2019-03-30','2019-04-10'):
    #     weekly_user_emotion(date)
    weekday = datetime.datetime.now().weekday()
    theday = today()
    if weekday== 3:
        print("Calculating user political...")
        weekly_user_political(theday)
    else: 
        print("not reach calculating user political time")
        pass

    # theday = today()
    # weekly_user_emotion(theday)