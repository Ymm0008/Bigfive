# -*- coding: utf-8 -*-
from flask import Blueprint ,request,jsonify,Response

import json
import time
from datetime import datetime,timedelta
from collections import Counter

from bigfive.group.utils import *
import os
mod = Blueprint('group',__name__,url_prefix='/group')


@mod.route('/test')
def test():
    result = 'This is group!'
    return json.dumps(result,ensure_ascii=False)


@mod.route('/create_group/',methods=['POST'])
def cgroup():
    """创建群体计算任务"""
    # data = request.form.to_dict()
    try:
        data = request.json
        result = create_group_task(data)
    except:
        return jsonify(0)
    return jsonify(1)


@mod.route('/delete_group/',methods=['POST'])
def dgroup():
    """删除群体任务/群体记录"""
    # gid = request.form.get('gid')
    gid = request.json.get('gid')
    index = request.json.get('index')
    try:
        delete_by_id(index,'text',gid)
    except:
        return jsonify(0)
    return jsonify(1)

@mod.route('/search_group/',methods=['GET'])
def sgroup():
    """搜索群体任务"""
    group_name = request.args.get('gname','')
    remark = request.args.get('remark','')
    create_time = request.args.get('ctime','')
    page = request.args.get('page','1')
    size = request.args.get('size','10')
    order_name = request.args.get('oname','create_time')
    order = request.args.get('order','desc')
    index = request.args.get('index')
    result = search_group_task(group_name,remark,create_time,page,size,order_name,order,index)
    return jsonify(result)


@mod.route('/group_ranking/',methods=['POST'])
def group_ranking():
    """群体排名"""
    parameters = request.form.to_dict()
    keyword = parameters.get('keyword', '')
    page = parameters.get('page', '1')
    size = parameters.get('size', '10')
    order_dict = parameters.get('order_dict', {})
    order_name = parameters.get('order_name', 'influence_index')
    order_type = parameters.get('order_type', 'desc')

    result = search_group_ranking(keyword, page, size, order_name, order_type, order_dict)
    return jsonify(result)


@mod.route('/delete_group_ranking',methods=['POST'])
def delete_ranking():
    """群体排名"""
    gid = request.json.get('gid')
    result = delete_by_id('group_ranking','text',gid)
    return jsonify(1)


@mod.route('/group_user_list/', methods=['POST'])
def group_user_list():
    parameters = request.form.to_dict()
    gid = parameters.get('group_id')
    page = parameters.get('page', '1')
    size = parameters.get('size', '10')
    # order_dict = parameters.get('order_dict', {})
    order_name = parameters.get('order_name', 'influence_index')
    order_type = parameters.get('order_type', 'desc')
    result = get_group_user_list(gid, page, size, order_name, order_type)
    return jsonify(result)


@mod.route('/basic_info', methods=['GET'])
def basic_info():
    gid = request.args.get('group_id')
    result = get_group_basic_info(gid)
    return jsonify(result)


@mod.route('/modify_remark/', methods=['POST'])
def modify_remark():
    parameters = request.form.to_dict()
    if not parameters:
        parameters = request.json
    group_id = parameters.get('group_id')
    remark = parameters.get('remark', '')
    try:
        modify_group_remark(group_id, remark)
        return jsonify(1)
    except:
        return jsonify(0)


################################ 宋慧慧负责 ###########################

@mod.route('/group_personality',methods=['POST','GET'])##group_id=mingxing_1548746836
def group_personality():
    group_id = request.args.get("group_id")
    query_body = {
        "query":{
            "bool":{
                "must":{
                    "term":{"group_id":group_id}
                }
            }
        }
    }
    group_index = es.search(index = 'group_ranking', doc_type = 'text', body = query_body)['hits']['hits'][0]["_source"]
    group_information = es.search(index = "group_information", doc_type = "text", body = query_body)["hits"]["hits"][0]["_source"]

    group_dict = dict()
    group_dict["group_index"] = group_index
    group_dict["group_information"] = group_information

    return json.dumps(group_dict,ensure_ascii=False)
'''
备注：
group_index代表的是基本信息+人格雷达图,各字段含义参照我上次发给你的字段备注

group_information代表的是群组名称、群体人数、关键词语等群组介绍信息
      返回的结果中：群组名称--group_name
                   群体人数--统计user_lst的长度
                   关键词语--暂时为空？？
                   创建人员--去掉此功能字段
                   群体备注--remark
'''
@mod.route('/group_activity', methods=['GET'])
def group_activity():
    group_id = request.args.get("group_id")
    result = get_group_activity(group_id)
    return jsonify(result)


################################ 李宛星负责 ###########################


@mod.route('/preference_identity', methods=['GET'])
def perference_identity():
    group_id=request.args.get('group_id')
    result = group_preference(group_id)

    return jsonify(result)


@mod.route('/preference_topic', methods=['GET'])
def perference_topic():
    group_id=request.args.get('group_id')
    group_inf = group_preference(group_id)

    topic = group_inf["_source"]["topic"]
    return json.dumps(topic,ensure_ascii=False)


@mod.route('/preference_word', methods=['GET'])
def perference_word():
    group_id=request.args.get('group_id')
    group_inf = group_preference(group_id)
    word = {}
    word['sensitive_words'] = group_inf["_source"]["sensitive_words"]
    word["key_words"] = group_inf["_source"]["key_words"]
    word["micro_words"] = group_inf["_source"]["micro_words"]
    return json.dumps(word,ensure_ascii=False)


@mod.route('/influence_feature', methods=['GET'])
def influence_feature():
    group_id=request.args.get('group_id')
    interval=request.args.get('type','day')
    result = group_influence(group_id,interval)
    return jsonify(result)


@mod.route('/emotion_feature', methods=['GET'])
def emotion_feature():
    group_id=request.args.get('group_id')
    interval = request.args.get('type','day')
    result = group_emotion(group_id,interval)
    return jsonify(result)


@mod.route('/social_contact', methods=['GET'])
def social_contact():
    group_id=request.args.get('group_id')
    map_type = request.args.get("type")
    try:
        social_contact = group_social_contact(group_id,map_type)
    except:
        return jsonify({})
    return jsonify(social_contact)
