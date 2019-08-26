# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, flash, redirect, url_for, session, jsonify, abort
from flask_login import login_user, logout_user, login_required, current_user
import string
from sqlalchemy import or_, and_, desc
import json
from ..config import *
from .models import *
# from ..tools import get_md5_id
from ..extensions import db
from .decorators import *

mod = Blueprint('user_manage', __name__, url_prefix='/user_manage')


@mod.route("/insert_role", methods=['GET', 'POST'])
def insert_role():
    Role.insert_roles()
    return json.dumps({"status": 1}, ensure_ascii=False)


@mod.route("/insert_user", methods=['GET', 'POST'])
def insert_user():
    User.insert_users()
    return json.dumps({"status": 1}, ensure_ascii=False)


# 登录
@mod.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = {k: v for k, v in dict(request.form).items()}
        user = User.query.filter_by(username=data['username']).first()
        if user is not None:
            if user.verify_password(data['password']):
                session['username'] = user.username
                session['role'] = user.role_id
                session['uid'] = user.id
                # session['group'] = user.group_id
                # session['relname'] = user.relname if user.relname else ''
                # session['phone'] = user.phone if user.phone else ''
                # session['email'] = user.email if user.email else ''
                # session['company'] = user.company if user.company else ''
                # if not user.email or not user.phone or not user.relname or not user.company:
                #     # return "请完善个人信息"
                #     return render_template('backstage/manage/supplement.html', uid=user.id,
                #                            username=session['username'], relname=session['relname'],
                #                            phone=session['phone'], email=session['email'], company=session['company'])
                return json.dumps({"status": 1, "role": user.role_id}, ensure_ascii=False)
        #     else:
        #         error = u'密码错误'
        #         return render_template('backstage/manage/login.html',error=error)
        # else:
        #     error = u'用户不存在'
        #
        error = '用户名或密码错误,请检查或联系管理员!'
        return json.dumps({'status': 0, "error": error}, ensure_ascii=False)
    return json.dumps({"status": "GET"}, ensure_ascii=False)


# @mod.route('/userinfo', methods=['GET', 'POST'])
# def user_info():
#     user = {}
#     if session:
#         user['uid'] = session['uid']
#         user['username'] = session['username']
#         user['role'] = session['role']
#         # user['group'] = session['group']
#     return json.dumps(user, ensure_ascii=False)


# 登出
@mod.route('/logout')
def logout():
    if session:
        session.clear()
    return redirect('/pages/login.html')


# 删除
@permission_required(Permission.ADMINISTER)
@mod.route('/delete', methods=['GET', 'POST'])
def Delete():
    if not session or session['role'] != 1:
        return 0
    uid = int(request.args.get('uid', ''))
    user = User.query.filter_by(id=uid).first()
    db.session.delete(user)
    db.session.commit()
    return json.dumps({'status': 1}, ensure_ascii=False)


# 添加用户
@permission_required(Permission.ADMINISTER)
@mod.route('/addUser', methods=['GET', 'POST'])
def add_user():
    if not session or session['role'] != 1:
        return 0
    if request.method == 'POST':
        data = {k: v for k, v in dict(request.form).items()}
        # try:
        #     group_id = int(data['group_id'])
        # except:
        #     group_name = data['group_id']
        #     group_id = add_group(group_name)
        #     if group_id == False:
        #         group_id = 1
        # 判断用户是否存在
        user = User.query.filter_by(username=data['username']).first()
        if user is not None:
            error = u'用户 %s 已存在' % (data['username'])
            return json.dumps({'status': 0, "error": error}, ensure_ascii=False)

        # 存入数据库
        else:
            user = User(username=data['username'], password=data['password'], role_id=int(data['role_id']))
            db.session.add(user)
            db.session.commit()
            # return redirect('/user_manage/userList')
            return json.dumps({'status': 1}, ensure_ascii=False)

    return json.dumps({'status': "GET"}, ensure_ascii=False)


# 用户列表数据
@mod.route('/userList')
def user_list():
    if not session or session['role'] != 1:
        return redirect('/user_manage/login')
    result = []
    userlist = User.query.all()
    for user in userlist:
        dic = {}
        dic['username'] = user.username
        dic['role'] = user.role_id
        dic['uid'] = user.id
        # dic['group'] = user.group.relname
        # dic['group_id'] = user.group.id
        # dic['relname'] = user.relname
        # dic['email'] = user.email
        # dic['phone'] = user.phone
        # dic['company'] = user.company
        result.append(dic)
    return json.dumps(result, ensure_ascii=False)

# @mod.route('/checkUsername', methods=['GET', 'POST'])
# def check_username():
#     username = request.args.get('username', '')
#     user = User.query.filter(User.username == username).first()
#     if user is not None:
#         return json.dumps({'status': 0}, ensure_ascii=False)
#     else:
#         return json.dumps({'status': 1}, ensure_ascii=False)
#
#
# @mod.route('/checkUsername_notself', methods=['GET', 'POST'])
# def check_username_notself():
#     username = request.args.get('username', '')
#     uid = int(request.args.get('uid', ''))
#     user = User.query.filter(User.id == uid).first()
#     user_new = User.query.filter(User.username == username).first()
#     if user_new is not None:
#         if user_new.id != uid:  # 用户名存在
#             return json.dumps({'status': 0}, ensure_ascii=False)
#         else:
#             return json.dumps({'status': 1}, ensure_ascii=False)
#     else:
#         return json.dumps({'status': 1}, ensure_ascii=False)
#
#
# # 编辑用户名、密码、权限、用户组
# @mod.route('/EditUser', methods=['GET', 'POST'])
# def edit_user():
#     if not session or session['role'] != 4:
#         return redirect('index/maniPulate')
#     if request.method == 'POST':
#         data = {k: v[0] for k, v in dict(request.form).items()}
#         data['uid'] = int(data['uid'])
#         # 判断用户名是否存在
#         user = User.query.filter(User.id == data['uid']).first()
#         user_new = User.query.filter(User.username == data['username']).first()
#         if user_new is not None:
#             if user_new.id != data['uid']:  # 用户名存在
#                 error = u'用户 %s 已存在' % (data['username'])
#                 return render_template('backstage/manage/manage.html', error=error)
#             else:  # 用户名不变
#                 try:
#                     group_id = int(data['group_id'])
#                 except:
#                     group_name = data['group_id']
#                     group_id = add_group(group_name)
#                     if group_id == False:
#                         group_id = 1
#                 user.username = data['username']
#                 user.role_id = int(data['role_id'])
#                 user.group_id = group_id
#                 if data['password']:
#                     user.password = data['password']
#                 db.session.add(user)
#                 db.session.commit()
#                 return redirect('index/manage')
#         else:  # 用户名不重复
#             user.username = data['username']
#             user.role_id = data['role_id']
#             user.group_id = data['group_id']
#             if data['password']:
#                 user.password = data['password']
#             db.session.add(user)
#             db.session.commit()
#             return redirect('index/manage')
#     return redirect('index/manage')
#
#
# # 修改密码
# @mod.route('/changePassword', methods=['GET', 'POST'])
# def change_password():
#     if request.method == 'POST':
#         data = {k: v[0] for k, v in dict(request.form).items()}
#         data['uid'] = int(data['uid'])
#         user = User.query.filter(User.id == data['uid']).first()
#         user.password = data['password']
#         db.session.add(user)
#         db.session.commit()
#         session.clear()
#         send_mail(user.username, user.email, '密码修改成功!', '您的密码已被修改!如果不是本人操作请尽快联系管理员!!')
#         return redirect('manage/login')
#     return redirect('manage/login')
#
#
# # 用户注册
# @mod.route('/register', methods=['GET', 'POST'])
# def register():
#     if request.method == 'POST':
#         data = {}
#         data['username'] = request.form['username']
#         data['relname'] = request.form['relname']
#         data['email'] = request.form['email']
#         data['phone'] = request.form['phone']
#         data['company'] = request.form['company']
#
#         # 判断用户是否存在
#         user = User.query.filter(User.username == data['username']).first()
#         # 存入redis缓存
#         if not user:
#             data['screct'] = random.sample(string.ascii_letters + string.digits, 6)
#             check_id = get_md5_id(data)
#             del data['screct']
#             check_url = request.url_root + 'manage/register_check?id={}'.format(check_id)
#             send_mail(data['relname'], data['email'], sub='用户注册验证',
#                       msg='欢迎您注册北京智信度科技有限公司下属产品>证券市场异常个股洞察系统,请点击以下链接激活账号,链接有效时长30分钟哦,且不能重复使用!!\n<a href="{url}">{url}</a>'.format(
#                           url=check_url))
#             REDIS.set(check_id, json.dumps(data, ensure_ascii=False), ex=1800)
#             return jsonify({'status': 1})
#     elif request.method == 'GET':
#         username = request.args.get('username', '')
#         relname = request.args.get('relname', '')
#         email = request.args.get('email', '')
#         phone = request.args.get('phone', '')
#         user = ''
#         if username:
#             user = User.query.filter(User.username == username).first()
#         if relname:
#             user = User.query.filter(User.relname == relname).first()
#         if email:
#             user = User.query.filter(User.email == email).first()
#         if phone:
#             user = User.query.filter(User.phone == phone).first()
#         if not user:
#             return jsonify({'status': 1})
#     return jsonify({'status': 0})
#
#
# # 用户注册,邮箱验证
# @mod.route('/register_check', methods=['GET', 'POST'])
# def register_check():
#     check_id = request.args.get('id', '')
#     if check_id:
#         value = REDIS.get(check_id)
#         if value:
#             data = json.loads(value)
#             # 0是待审核状态
#             user = User(username=data['username'], relname=data['relname'], password='', email=data['email'],
#                         phone=data['phone'], company=data['company'], status=0)
#             db.session.add(user)
#             db.session.commit()
#             REDIS.delete(check_id)
#             msg = '您已通过身份确认,请耐心等待管理员审核!审核情况将以邮件方式通知您,请注意查收邮件!'
#             return msg
#     return '该链接已失效！！'
#
#
# @mod.route('/register_list', methods=['GET', 'POST'])
# def register_list():
#     if not session or session['role'] != 4:
#         return redirect('index/maniPulate')
#     result = []
#     userlist = User.query.filter(User.status == 0)
#     for user in userlist:
#         dic = {}
#         dic['uid'] = user.id
#         dic['username'] = user.username
#         dic['company'] = user.company
#         dic['email'] = user.email
#         dic['phone'] = user.phone
#         dic['relname'] = user.relname
#         dic['status'] = user.status
#         result.append(dic)
#     return jsonify(result)
#
#
# @mod.route('/examine', methods=['GET', 'POST'])
# def examine():
#     adopt = request.args.get('adopt', '0')
#     uid = int(request.args.get('uid', ''))
#     user = User.query.filter(User.id == uid).first()
#     if adopt == '1':
#         role_id = int(request.args.get('role_id'))
#         try:
#             group_id = int(request.args.get('group_id'))
#         except:
#             group_name = request.args.get('group_id')
#             group_id = add_group(group_name)
#             if group_id == False:
#                 group_id = 1
#         # 1表示审核通过
#         origin_password = ''.join(random.sample(string.ascii_letters + string.digits, 6))
#         user.status = 1
#         user.password = origin_password
#         user.group_id = group_id
#         user.role_id = role_id
#         db.session.add(user)
#         db.session.commit()
#         sub = '用户审核通过'
#         msg = '<p>欢迎您注册 北京智信度科技有限公司下属产品 >证券市场异常个股洞察系统</p></p>您的账户初始密码为: <b>{}</b></p><p>您可以在业务系统页面修改您的密码!</p>'.format(
#             origin_password)
#     else:
#         # 2表示审核未通过
#         # user.status = 2
#         db.session.delete(user)
#         # db.session.add(user)
#         db.session.commit()
#         sub = '用户审核未通过'
#         msg = '<p>欢迎您注册 北京智信度科技有限公司下属产品 >证券市场异常个股洞察系统</p></p>您的注册申请被管理员拒绝了,请检查您的注册信息或联系管理员!</p>'
#     send_mail(user.relname, user.email, sub=sub, msg=msg)
#     return jsonify(1)
#
#
# @mod.route('/group_list', methods=['GET', 'POST'])
# def group_list():
#     groups = Group.query.all()
#     result = []
#     for group in groups:
#         item = {
#             'group_name': group.relname,
#             'group_id': group.id
#         }
#         result.append(item)
#     return jsonify(result)
#
#
# def add_group(group_name):
#     group = Group.query.filter(Group.relname == group_name).first()
#     if not group:
#         p = Pinyin()
#         num = Group.query.order_by(desc(Group.id)).first().id
#         name = p.get_pinyin(group_name).capitalize()
#         num += 1
#         group = Group(id=num, name=name, daping_list=json.dumps([]), xiangqing_list=json.dumps([]),
#                       dama_list=json.dumps([]), relname=group_name)
#         db.session.add(group)
#         db.session.commit()
#         return num
#     return False
#
#
# @mod.route('/change_info', methods=['GET', 'POST'])
# def change_info():
#     if request.method == 'POST':
#         # print request.form,request.json
#         uid = request.json['uid']
#         company = request.json['company']
#         relname = request.json['relname']
#         email = request.json['email']
#         phone = request.json['phone']
#         user = User.query.filter(User.id == uid).first()
#         if user:
#             if company:
#                 user.company = company
#             if relname:
#                 user.relname = relname
#             if email:
#                 user.email = email
#             if phone:
#                 user.phone = phone
#             db.session.add(user)
#             db.session.commit()
#             return jsonify(1)
#     return jsonify(0)
