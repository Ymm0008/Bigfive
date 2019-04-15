import sys
sys.path.append('../')
from config import *
from time_utils import *
from cal_main import user_main
from cron.portrait.user.normalizing import normalize_influence_index

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
	push_status_list = []
	for hit in res:
		uid_list.append(hit['_source']['uid'])
		push_status_list.append(hit['_source']['push_status'])
		try:
			username_list.append(hit['_source']['username'])
		except:
			username_list.append("")
	end_date = today()
	day = 15   #从start_date到end_date的天数
	start_date = ts2date(date2ts(end_date) - day*DAY)
	if len(uid_list):
		# for uid in uid_list:
		# 	es.update(index=USER_INFORMATION,doc_type='text',body={'doc':{'progress':1}},id=uid)   #计算开始，计算状态变为计算中
		
		user_main(uid_list, push_status_list, username_list, start_date, end_date)

		for uid in uid_list:
			es.update(index=USER_INFORMATION,doc_type='text',body={'doc':{'progress':2}},id=uid)   #计算结束，计算状态变为计算完成

if __name__ == '__main__':
	main()
	# pass