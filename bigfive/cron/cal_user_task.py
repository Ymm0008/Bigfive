import sys
sys.path.append('../')
from config import *
from time_utils import *
from cal_main import user_main

def main():
	query_body = {
		'query':{
			'term':{
				'progress':0
			}
		},
		'sort':{
			'insert_time':{
				'order':'asc'
			}
		},
		'size':USER_ITER_COUNT
	}
	res = es.search(index=USER_INFORMATION,doc_type='text',body=query_body)['hits']['hits']
	uid_list = []
	username_list = []
	for hit in res:
		uid_list.append(hit['_source']['uid'])
		username_list.append(hit['_source']['username'])
	start_date = '2016-11-13'
	end_date = '2016-11-27'
	if len(uid_list):
		for uid in uid_list:
			es.update(index=USER_INFORMATION,doc_type='text',body={'doc':{'progress':1}},id=uid)   #计算开始，计算状态变为计算中
		user_main(uid_list, username_list, start_date, end_date)
		for uid in uid_list:
			es.update(index=USER_INFORMATION,doc_type='text',body={'doc':{'progress':2}},id=uid)   #计算结束，计算状态变为计算完成

if __name__ == '__main__':
	main()
	# pass