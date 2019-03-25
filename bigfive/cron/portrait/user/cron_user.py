import sys
sys.path.append('../../../')
from config import *
from time_utils import *
from global_utils import *

from user_ip import get_user_activity
from user_topic import get_user_topic
from user_domain import get_user_domain
#from user_political import get_user_political
from user_text_analyze import get_word_analysis
from user_emotion import cal_user_emotion
from user_social import cal_user_social
from user_influence import cal_user_influence
from user_activity import get_user_weibo_type

#weibo_data_dict{"day1":[微博数据列表],"day2":[微博数据列表]}
def get_weibo_data_dict(uid, start_date,end_date):
    weibo_data_dict = {}
    #对每一天进行微博数据获取
    for day in get_datelist_v2(start_date, end_date):
        #print(day)
        weibo_data_dict[day] = []
        index_name = "flow_text_" + str(day)
        query_body ={"query": {"bool": {"must":[{"term": {"uid": uid}}]}}}
        sort_dict = {'_id':{'order':'asc'}}
        ESIterator1 = ESIterator(0,sort_dict,1000,index_name,"text",query_body,es_weibo)
        while True:
            try:
                #一千条es数据
                es_result = next(ESIterator1)
                if len(weibo_data_dict[day]):
                    weibo_data_dict[day].extend(es_result)
                else:
                    weibo_data_dict[day] = es_result
                   
            except StopIteration:
                #遇到StopIteration就退出循环
                break
    return weibo_data_dict

def user_portrait(uid, start_date,end_date):
    
    weibo_data_dict = get_weibo_data_dict(uid, start_date,end_date)
    #print(weibo_data_dict)
    
    print('Calculating user position...')
    #get_user_activity(uid,start_date,end_date)

    print('Calculating user topic...')
    #get_user_topic(uid,start_date,end_date)

    print('Calculating user domain...')
    #get_user_domain(uid,start_date,end_date)

    print('Calculating user activity...')
    get_user_weibo_type(uid,weibo_data_dict,start_date,end_date)

    print('Calculating user political...')
    #get_user_political(uid, start_date,end_date)

    print('Calculating word analysis...')
    #get_word_analysis(uid,start_date,end_date)

    print('Calculating user emotion...')
    emo_time_s = time.time()
    cal_user_emotion(uid,weibo_data_dict)
    emo_time_e = time.time()
    print('Calculating user emotion time:',emo_time_e-emo_time_s)  

    print('Calculating user social...')
    s_s_time = time.time()
    #cal_user_social(uid,weibo_data_dict)
    s_e_time = time.time()
    print('Calculating user social time:',s_e_time-s_s_time)

    print('Calculating user influence...')
    i_s_time = time.time()
    #cal_user_influence(uid,weibo_data_dict)
    i_e_time = time.time()
    print('Calculating user influence time:',i_e_time-i_s_time)
    
if __name__ == '__main__':
    user_portrait("1663765234","2016-11-13","2016-11-27")
    