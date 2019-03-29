# -*- coding: UTF-8 -*-
import os
import operator
import sys

sys.path.append('../../')
from config import *
from time_utils import *
from global_utils import * 

from politics_user import get_politics_user

topic_num = 5
iteration_num = 1000
keywords_num = 20

ABS_PATH = os.path.dirname(os.path.abspath(__file__))


def get_weibo(mid_list,index_name):
    mid_final_list = []
    with open(os.path.join(ABS_PATH, 'LDA/weibo_data.txt' ),"w") as f:
        for mid in mid_list:
            text = es.search(index=index_name ,doc_type="text",\
                        body={"query":{"bool":{"must":[{"term":{"mid":mid}}]}}} )["hits"]["hits"][0]["_source"]["text"]            
            f.write(text)
            f.write("\n")
            mid_final_list.append(mid) 

    return "weibo_data.txt",mid_final_list

def get_topic_from_weibo(weibo_filename,topic_num,iteration_num,keywords_num):
    for f in ["LDA/weibo_data.txt.preprocessed","LDA/model.tdocs","LDA/model.twords"]:
        f = os.path.join(ABS_PATH, f )
        if os.path.isfile(f):
            os.remove(f)

    command = "cd "+os.path.join(ABS_PATH, 'LDA')+"; java -jar LDA.jar ./"+weibo_filename+" "+str(topic_num)+" "+str(iteration_num)+" "+str(keywords_num)

    p_object = os.popen(command)
    p_object.readlines()


def get_topic_result(mid_list,topic_num,keywords_num):
    topic_keyword_dict = {}
    topic_doc_dict = {}

    with open(os.path.join(ABS_PATH, 'LDA/model.twords' )) as f_words:    
        content = iter(f_words.readlines())
        while True:
            try:
                line = content.__next__().strip()
                topic_name = ""

                if line.startswith("Topic"):
                    topic_name = '_'.join(line.split(":")[0].split(" "))

                    keyword_dict = {}
                    for i in range(keywords_num):
                        key_value = content.__next__().strip()
                        keyword = key_value.split(" ")[0]
                        value = key_value.split(" ")[1]
                        keyword_dict[keyword] = float(value)

                    topic_keyword_dict[topic_name] = keyword_dict
            except:
                break

    with open(os.path.join(ABS_PATH, 'LDA/model.tdocs' )) as doc_f:
        content = iter(doc_f.readlines())
        while True:
            try:
                line = content.__next__().strip()
                topic_name = ""

                if line.startswith("Topic"):
                    topic_name = '_'.join(line.split(":")[0].split(" "))
                    print(topic_name)

                    doc_list = []
                    for i in range(5):
                        doc_dict = {}
                        text = content.__next__().strip()
                        doc = text.split(":")[1].strip()

                        doc_num = int(text.split(":")[0].strip())-1 #num
                        doc_list.append({"content":doc,"mid":mid_list[doc_num]})

                    topic_doc_dict[topic_name] = doc_list
            except:
                break
    return topic_keyword_dict,topic_doc_dict


def save_topic(topic_keyword_dict,topic_doc_dict,politics_id,user_type,sentiment):
    keyword_result = dict()
    if topic_keyword_dict == {}:
        keyword_result = {"Topic_0":[],"Topic_1":[],"Topic_2":[],"Topic_3":[],"Topic_4":[]}
    if topic_doc_dict == {}:
        topic_doc_dict = {"Topic_0":[],"Topic_1":[],"Topic_2":[],"Topic_3":[],"Topic_4":[]}
    
    if topic_keyword_dict != {}:
        for i in topic_keyword_dict:
            keyword_list = []
            for j in topic_keyword_dict[i]:
                keyword_list.append({ "keyword":j,"value":topic_keyword_dict[i][j]})
            keyword_result[i] = keyword_list

    query_body = {
        "politics_id":politics_id,
        "user_type":user_type,
        "sentiment":sentiment,
        "key_words_topic_1":keyword_result["Topic_0"],
        "key_words_topic_2":keyword_result["Topic_1"],
        "key_words_topic_3":keyword_result["Topic_2"],
        "key_words_topic_4":keyword_result["Topic_3"],
        "key_words_topic_5":keyword_result["Topic_4"],
        "weibo_topic_1":topic_doc_dict["Topic_0"],
        "weibo_topic_2":topic_doc_dict["Topic_1"],
        "weibo_topic_3":topic_doc_dict["Topic_2"],
        "weibo_topic_4":topic_doc_dict["Topic_3"],
        "weibo_topic_5":topic_doc_dict["Topic_4"]

    }
    es.index(index="politics_topic" ,doc_type="text" ,id=politics_id+"_"+sentiment+"_"+user_type,body=query_body )

def get_politics_topic(politics_id, politic_mapping_name, mid_dict):
    # mid_dict = get_politics_user(politics_id,politic_mapping_name)

    for i in mid_dict:
        sentiment = i
        for j in mid_dict[i]:
            if j == "bigv_user":
                user_type = "BigV"
            else:
                user_type = "ordinary"

            mid_list = mid_dict[i][j]

            if mid_list==[]:
                save_topic({},{},politics_id,user_type,sentiment)
            else:
                weibo_filename,mid_final_list = get_weibo(mid_list,politic_mapping_name)
                get_topic_from_weibo(weibo_filename,topic_num,iteration_num,keywords_num)
                topic_keyword_dict,topic_doc_dict = get_topic_result(mid_final_list,topic_num,keywords_num)
                
                save_topic(topic_keyword_dict,topic_doc_dict,politics_id,user_type,sentiment)



if __name__ == '__main__':

    get_politics_topic("politics_ceshizhengceer_1553060528","ceshizhengceer_1553060528")
