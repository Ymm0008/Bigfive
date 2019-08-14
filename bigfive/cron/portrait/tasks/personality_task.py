# -*- coding: UTF-8 -*-
import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
from config import *
from time_utils import *
from global_utils import *
from cron_portrait import cal_user_personality

def monthly_user_personality(date):
    date = ts2date(int(date2ts(date)) - DAY)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 1000)
    iter_num = 0
    while True:
        try:
            es_result = next(iter_result)
            iter_num += 1
            print(iter_num)
            if iter_num <= 222:
                continue
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        start_date = ts2date(date2ts(date) - 15*DAY)
        cal_user_personality(uid_list, start_date, date)
        for uid in uid_list:   #每月计算完人格之后要把push_status归零以启动推送新人格指数
            es.update(index=USER_INFORMATION,doc_type='text',body={'doc':{'push_status':0}},id=uid)


if __name__ == '__main__':
    date = '2019-04-11'
    monthly_user_personality(date)
    # theday = today()
    # date = time.strftime('%d', time.localtime(time.time()))
    # if date == "01":
    #     print("Calculating user personality...")
    #     monthly_user_personality(theday)
    # else:
    #     print("not reach calculating user personality time")
    #     pass