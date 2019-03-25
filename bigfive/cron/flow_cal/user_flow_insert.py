import sys
import time
sys.path.append('../../')
from config import *
from time_utils import *

def user_flow_insert():
    res = es.mget(index=WEIBO_USER, doc_type='type1', body={'ids':['1978574705','2596620224']})['docs']
    for hit in res:
        source = hit['_source']
        dic = {
            "username": source['name'],
            "uid": str(source['u_id']),
            "gender": 0 if source['gender'] == 'f' else 1,
            "description": source['description'],
            "user_location": source['location'],
            "friends_num": source['friends_count'],
            "create_at": source['created_at'],
            "weibo_num": source['statuses_count'],
            "user_birth": '',
            "isreal": int(source['verified']),
            "photo_url": source['profile_image_url'],
            "fans_num": source['followers_count'],
            "political_bias": '',
            "domain": '',
            "insert_time": int(time.time()),
            "progress": 0
        }
        es.index(index=USER_INFORMATION,doc_type='text',body=dic,id=dic['uid'])

if __name__ == '__main__':
    user_flow_insert()