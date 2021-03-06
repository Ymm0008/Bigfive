# -*- coding: utf-8 -*-

from flask import Blueprint ,request,jsonify
import json
from bigfive.politics.utils import *

mod = Blueprint('politics',__name__,url_prefix='/politics')

@mod.route('/test')
def test():
    result = 'This is politics!'
    return json.dumps(result,ensure_ascii=False)


@mod.route('/hot_politics_list/', methods=['POST'])
def hot_politics_list():
    parameters = request.form.to_dict()
    keyword = parameters.get('keyword', '')
    page = parameters.get('page', '1')
    size = parameters.get('size', '10')
    order_name = parameters.get('order_name', 'create_date')
    order_type = parameters.get('order_type', 'desc')
    result = get_hot_politics_list(keyword, page, size, order_name, order_type)
    return jsonify(result)


@mod.route('/create_hot_politics/', methods=['POST'])
def create_hot_politics():
    parameters = request.form.to_dict()
    if not parameters:
        parameters = request.json
    politics_name = parameters.get('politics_name', '')
    keywords = parameters.get('keywords', '')
    location = parameters.get('location', '')
    start_date = parameters.get('start_date', '')
    end_date = parameters.get('end_date', '')
    # try:
    post_create_hot_politics(politics_name, keywords, location,  start_date, end_date)
    return jsonify(1)

@mod.route('/delete_hot_politics/', methods=['POST'])
def delete_hot_politics():
    parameters = request.form.to_dict()
    if not parameters:
        parameters = request.json
    politics_id = parameters.get('pid', '')
    post_delete_hot_politics(politics_id)
    return jsonify(1)

@mod.route('/politics_personality/', methods=['GET'])
def politics_personality():
    politics_id = request.args.get('pid')
    sentiment = request.args.get('sentiment')
    result = get_politics_personality(politics_id,sentiment)
    return jsonify(result)

@mod.route('/politics_topic/', methods=['GET'])
def politics_topic():
    politics_id = request.args.get('pid')
    sentiment = request.args.get('sentiment')
    result = get_politics_topic(politics_id,sentiment)
    return jsonify(result)

@mod.route('/politics_statistics/', methods=['GET'])
def politics_statistics():
    politics_id = request.args.get('pid')
    result = get_politics_statistics(politics_id)
    return jsonify(result)