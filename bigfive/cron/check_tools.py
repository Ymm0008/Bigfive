import sys
sys.path.append('../')
from config import *
from time_utils import *
from global_utils import *
from elasticsearch.helpers import bulk

def update_position_date():
    while True:
        query_body = {
          "query": {
            "constant_score": {
              "filter": {
                "missing": {
                  "field": "date"
                }
              }
            }
          },
          "size":200000
        }
        data = es.search(index='user_activity',doc_type='text',body=query_body)['hits']['hits']
        num = len(data)
        if num == 0:
        	break
        package = []
        for index, item in enumerate(data):
            dic = {'date':ts2date(item['_source']['timestamp'])}
            package.append({
                '_index':'user_activity',
                '_op_type':'update',
                '_id':item['_id'],
                '_type':'text',
                'doc':dic
            })
            if index % 1000 == 0:
                bulk(es, package)
                package = []
                print(num - index)
        bulk(es, package)

def delete_all():
    query_body = {
        "query":{
            'term':{
                'timestamp':1566230400
            }
        },
        "size":200000
    }
    data = es.search(index='user_domain_topic',doc_type='text',body=query_body)['hits']['hits']
    num = len(data)
    package = []
    for index, item in enumerate(data):
        package.append({
            '_index':'user_domain_topic',
            '_op_type':'delete',
            '_id':item['_id'],
            '_type':'text',
        })
        if index % 1000 == 0:
            bulk(es, package)
            package = []
            print(num - index)
    bulk(es, package)

if __name__ == '__main__':
    # print(date2ts('2019-08-25'))
    print(ts2date(1566796374))
    # update_position_date()
    # delete_all()