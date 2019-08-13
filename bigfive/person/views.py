# -*- coding: utf-8 -*-

import json
import time

from flask import Blueprint, request, jsonify, make_response, send_file
from datetime import datetime, timedelta
from bigfive.time_utils import *

from bigfive.person.utils import *

mod = Blueprint('person', __name__, url_prefix='/person')


@mod.route('/test')
def test():
    result = 'This is person!'
    return json.dumps(result, ensure_ascii=False)


# portrait表格
@mod.route('/portrait', methods=['POST'])
def return_portrait_table():
    parameters = request.form.to_dict()
    keyword = parameters.get('keyword', '')
    page = parameters.get('page', '1')
    size = parameters.get('size', '10')
    order_dict = parameters.get('order_dict', {})

    machiavellianism_index = parameters.get('machiavellianism_index', 0)
    narcissism_index = parameters.get('narcissism_index', 0)
    psychopathy_index = parameters.get('psychopathy_index', 0)
    extroversion_index = parameters.get('extroversion_index', 0)
    nervousness_index = parameters.get('nervousness_index', 0)
    openn_index = parameters.get('openn_index', 0)
    agreeableness_index = parameters.get('agreeableness_index', 0)
    conscientiousness_index = parameters.get('conscientiousness_index', 0)
    order_name = parameters.get('order_name', 'influence_index')
    order_type = parameters.get('order_type', 'desc')

    result = portrait_table(keyword, page, size, order_name, order_type, machiavellianism_index, narcissism_index, psychopathy_index, extroversion_index, nervousness_index, openn_index, agreeableness_index, conscientiousness_index, order_dict)

    return jsonify(result)
    # return jsonify(1)


# 根据uid删除一条记录
@mod.route('/delete_user', methods=['POST'])
def delete_user():
    uid = request.json.get('person_id')
    result = es.delete(index='user_ranking', doc_type='text', id=uid)
    # return json.dumps(result, ensure_ascii=False)
    return jsonify(1)


# 用户基本信息
@mod.route('/basic_info', methods=['GET', 'POST'])
def basic_info():
    uid = request.args.get('person_id')
    result = get_basic_info(uid)
    return json.dumps(result, ensure_ascii=False)


# 活动特征
@mod.route('/person_activity', methods=['POST', 'GET'])
def user_activity():
    uid = request.args.get('person_id')
    result = get_user_activity(uid)
    return json.dumps(result, ensure_ascii=False)


# 偏好特征
@mod.route('/preference_identity', methods=['POST', 'GET'])
def preference_identity():
    uid = request.args.get('person_id')
    result = get_preference_identity(uid)
    return jsonify(result)


# 影响力特征
@mod.route('/influence_feature', methods=['POST', 'GET'])
def influence_feature():
    uid = request.args.get('person_id')
    interval = request.args.get('type','day')
    result = get_influence_feature(uid,interval)
    return jsonify(result)

# 活动特征 --转发 评论 原创
@mod.route('/person_behavior', methods=['POST', 'GET'])
def person_behavior():
    uid = request.args.get('person_id')
    interval = request.args.get('type','day')
    result = get_user_behavior(uid,interval)
    return jsonify(result)


@mod.route('/person_personality', methods=['POST', 'GET'])
def user_personality():
    uid = request.args.get("person_id")

    query_body = {
        "query": {
            "bool": {
                "must": {
                    "term": {"uid": uid}
                }
            }
        }
    }
    user_index = es.search(index='user_ranking', doc_type='text', body=query_body)['hits']['hits'][0]["_source"]
    user_information = es.search(index="user_information", doc_type="text", body=query_body)["hits"]["hits"][0][
        "_source"]

    user_dict = dict()
    user_dict["user_index"] = user_index
    user_dict["user_information"] = user_information

    return json.dumps(user_dict, ensure_ascii=False)


# @mod.route('/person_activity', methods=['POST', 'GET'])  # 1098650354
# def user_activity():
#     uid = request.args.get("person_id")
#
#     day = datetime.today().date() - timedelta(days=6)
#     ts = int(time.mktime(time.strptime(str(day), '%Y-%m-%d')))
#
#     query_body = {
#         "query": {
#             "bool": {
#                 "must": [{
#                     "range": {
#                         "timestamp": {
#                             "gt": ts,
#                             "lt": int(time.time())
#                         }
#                     }
#                 },
#                     {
#                         "term": {"uid": uid}
#                     }
#                 ]
#             }
#         }
#     }
#     activity_table = es.search(index='user_activity', doc_type='text', body=query_body)['hits']['hits']
#     activity_lst = [i["_source"] for i in activity_table]
#
#     geo_lst = [i["_source"]["location"].split("&")[1] for i in activity_table]
#     geo_dict = dict(Counter(geo_lst))
#
#     query_body2 = {
#         "query": {
#             "bool": {
#                 "must": {
#                     "term": {"uid": uid}
#                 }
#             }
#         }
#     }
#     source_location = \
#         es.search(index='user_information', doc_type='text', body=query_body2)['hits']['hits'][0]["_source"][
#             "belong_home"].split(u"国")[1]
#
#     activity_dict = dict()
#     activity_dict["table"] = activity_lst
#     activity_dict["geo_dict"] = geo_dict
#     activity_dict["source_location"] = source_location
#
#     return json.dumps(activity_dict, ensure_ascii=False)


# @mod.route('/perference_identity', methods=['POST', 'GET'])
# def perference_identity():
#     uid = request.args.get('person_id')
#     user_inf = user_preference(uid)
#     node = []
#     link = []
#     user_pre_identity = {}
#     main_domain = user_inf["_source"]["main_domain"]
#     for key, value in user_inf["_source"]["domain"].items():
#         link_dict = {}
#         link_dict["source"] = main_domain
#         link_dict["target"] = value
#         link_dict["relation"] = key
#         node.append(value)
#         link.append(link_dict)
#     user_pre_identity["node"] = node
#     user_pre_identity["link"] = link
#     return json.dumps(user_pre_identity, ensure_ascii=False)


@mod.route('/perference_topic', methods=['POST', 'GET'])
def perference_topic():
    uid = request.args.get('person_id')
    user_inf = user_preference(uid)
    topic = user_inf["_source"]["topic"]
    return json.dumps(topic, ensure_ascii=False)


@mod.route('/perference_word', methods=['POST', 'GET'])
def perference_word():
    uid = request.args.get('person_id')
    user_inf = user_preference(uid)
    word = {}
    word['sensitive_words'] = user_inf["_source"]["sensitive_words"]
    word["key_words"] = user_inf["_source"]["key_words"]
    word["micro_words"] = user_inf["_source"]["micro_words"]
    return json.dumps(word, ensure_ascii=False)


@mod.route('/social_contact', methods=['POST', 'GET'])
def social_contact():
    # type 1 2 3 4 转发 被转发 评论 被评论
    uid = request.args.get('person_id')
    map_type = request.args.get("type")
    social_contact = user_social_contact(uid, map_type)
    return jsonify(social_contact)


# @mod.route('/influence_feature', methods=['POST', 'GET'])
# def influence_feature():
#     uid = request.args.get('person_id')
#     user_inf = user_influence(uid)
#     dict_inf = {}
#     time_list = []
#     activity = []
#     sensitivity = []
#     influence = []
#     warning = []
#     for i, _ in enumerate(user_inf):
#         time_list.append(_["_source"]["timestamp"])
#         activity.append(_["_source"]["activity"])
#         sensitivity.append(_["_source"]["sensitivity"])
#         influence.append(_["_source"]["influence"])
#         warning.append(_["_source"]["warning"])
#     dict_inf["time"] = time_list
#     dict_inf["activity_line"] = activity
#     dict_inf["sensitivity_line"] = sensitivity
#     dict_inf["influence_line"] = influence
#     dict_inf["warning_line"] = warning
#
#     return json.dumps(dict_inf, ensure_ascii=False)


@mod.route('/emotion_feature', methods=['POST', 'GET'])
def emotion_feature():
    uid = request.args.get('person_id')
    interval = request.args.get('type','day')
    result = user_emotion(uid,interval)
    return jsonify(result)

@mod.route('/user_add_one', methods=['POST', 'GET'])
def user_add_one():
    username = request.args.get('username')
    uid = request.args.get('uid')
    gender = request.args.get('gender')
    description = request.args.get('description')
    user_location = request.args.get('user_location')
    friends_num = request.args.get('friends_num')
    create_at = request.args.get('create_at')
    weibo_num = request.args.get('weibo_num')
    user_birth = request.args.get('user_birth')
    isreal = request.args.get('isreal')
    photo_url = request.args.get('photo_url')
    fans_num = request.args.get('fans_num')
    insert_time = int(time.time())
    progress = 0

    result = user_add_one_task(username, uid, gender, description, user_location, friends_num, create_at, weibo_num, user_birth, isreal, photo_url, fans_num, insert_time, progress)
    return jsonify(result)

@mod.route('/user_add_list', methods=['POST', 'GET'])
def user_add_list():
    task_list = json.loads(request.args.get('list'))
    result = user_add_list_task(task_list)
    return jsonify(result)

@mod.route('/user_task_show', methods=['POST', 'GET'])
def user_task_show():
    result = get_user_task_show()
    return jsonify(result)

@mod.route('/user_task_delete', methods=['POST', 'GET'])
def user_task_delete():
    uid = request.args.get('uid')
    result = delete_user_task(uid)
    return jsonify(result)

@mod.route('/user_info_download', methods=['POST', 'GET'])
def user_info_download():
    uids = request.args.get('person_id')
    timenow = ts2datetime(int(time.time()))
    filename = 'outfile/' + timenow + '.xlsx'
    uidlist = uids.split(',')
    get_user_excel_info(uidlist, filename)
    try:
        response = make_response(send_file(filename, as_attachment=True, attachment_filename="%s.xlsx" % timenow))
        ABS_PATH = os.path.abspath(os.path.dirname(__file__))
        os.remove(ABS_PATH + '/../' + filename)
    except Exception as e:
        print(e)
        return json.dumps({"status":0})
    return response