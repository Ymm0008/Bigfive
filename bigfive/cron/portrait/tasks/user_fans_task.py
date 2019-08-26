import sys
sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')
sys.path.append('../user/')
from config import *
from time_utils import *
from global_utils import *

def user_fans_update(date):
    date = ts2date(int(date2ts(date)) - DAY)
    print(date)
    iter_result = get_user_generator("user_information", {"query":{"bool":{"must":[{"term":{"progress":2}}]}}}, 100000)
    num = 0
    find_num = 0
    have_num = 0
    while True:
        try:
            es_result = next(iter_result)
        except:
            break
        uid_list = []
        for k,v in enumerate(es_result):
            uid_list.append(es_result[k]["_source"]["uid"])

        for uid in uid_list:
            uid = str(uid)
            num += 1
            query_body = {
              "query": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "uid": uid
                      }
                    },
                    {
                      "range": {
                        "date": {
                          "gte": ts2date(int(date2ts(date)) - DAY*20),
                          "lte": date
                        }
                      }
                    }
                  ],
                  "must_not": [],
                  "should": [
                    {
                      "range": {
                        "comment": {
                          "gt": "0"
                        }
                      }
                    },
                    {
                      "range": {
                        "original": {
                          "gt": "0"
                        }
                      }
                    },
                    {
                      "range": {
                        "retweet": {
                          "gt": "0"
                        }
                      }
                    }
                  ],
                  "minimum_should_match": 1
                }
              },
              "size": 1,
              "sort": {
                "date":{
                  "order":"desc"
                }
              }
            }
            data = es.search(index='user_weibo_type',doc_type='text',body=query_body)['hits']['hits']
            if len(data):
                find_num += 1
                qb1 = {
                    'query':{
                        'term':{
                            'uid':uid
                        }
                    },
                    'size':1,
                    'sort':{
                        'timestamp':{
                            'order':'desc'
                        }
                    }
                }
                try:
                    item = es_weibo.search(index='flow_text_' + data[0]['_source']['date'], doc_type='text', body=qb1)['hits']['hits']
                except:
                    continue
                if len(item):
                    have_num += 1
                    user_fansnum = item[0]['_source']['user_fansnum']
                    user_friendsnum = item[0]['_source']['user_friendsnum']
                    dic = {'fans_num':user_fansnum, 'friends_num':user_friendsnum}
                    # print(num,find_num,have_num,dic)
                    es.update(index='user_information',doc_type='text',body={"doc":dic},id=uid)

if __name__ == '__main__':
    theday = today()
    print('Updating user fans...')
    user_fans_update(theday)
    # print(date2ts('2019-08-16'))