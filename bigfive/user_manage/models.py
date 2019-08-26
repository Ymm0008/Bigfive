# -*- coding: utf-8 -*-
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin, AnonymousUserMixin
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from flask import current_app
from datetime import datetime
import json

from ..extensions import db


# from . import login_manager

class Permission:
    READ = 0x01
    ADD = 0x02
    CHECK = 0x04
    REVISE = 0x08
    ADMINISTER = 0x80


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    default = db.Column(db.Boolean, default=False, index=True)
    permissions = db.Column(db.Integer)
    users = db.relationship('User', backref='role', lazy='dynamic')

    @staticmethod
    def insert_roles():
        roles = {'User': (Permission.READ, True),
                 'Operator': (Permission.READ | Permission.ADD, False),
                 'Moderator': (Permission.READ | Permission.ADD | Permission.CHECK | Permission.REVISE, False),
                 'Administrator': (0xff, False)}
        for r in sorted(roles.keys()):
            role = Role.query.filter_by(name=r).first()
            if role is None:
                role = Role(name=r)
            role.permissions = roles[r][0]
            role.default = roles[r][1]
            db.session.add(role)
        db.session.commit()

    def __repr__(self):
        return '<Role %r>' % self.name


# class Group(db.Model):
#     __tablename__ = 'groups'
#     id = db.Column(db.Integer, primary_key=True)
#     name = db.Column(db.String(64), unique=True)
#     default = db.Column(db.Boolean, default=False, index=True)
#     daping_status = db.Column(db.Integer, default=2)
#     daping_list = db.Column(db.Text())
#     xiangqing_status = db.Column(db.Integer, default=2)
#     xiangqing_list = db.Column(db.Text())
#     ifdama = db.Column(db.Boolean, default=False)
#     dama_list = db.Column(db.Text())
#     users = db.relationship('User', backref='group', lazy='dynamic')
#     relname = db.Column(db.String(255))
#
#     @staticmethod
#     def insert_groups():
#         groups = {'We': False,
#                   'Shanghai': False,
#                   'Others': True, }
#         num = 1
#         for r in ['Others', 'Shanghai', 'We']:
#             group = Group.query.filter_by(name=r).first()
#             if group is None:
#                 group = Group(id=num, name=r, daping_list=json.dumps([]), xiangqing_list=json.dumps([]),
#                               dama_list=json.dumps([]))
#             group.default = groups[r]
#             db.session.add(group)
#             num += 1
#         db.session.commit()
#
#     def __repr__(self):
#         return '<Group %r>' % self.name


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, index=True)
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    # group_id = db.Column(db.Integer, db.ForeignKey('groups.id'))
    password_hash = db.Column(db.String(128))

    # email = db.Column(db.String(128))
    # phone = db.Column(db.String(128))
    # company = db.Column(db.String(1024))
    # relname = db.Column(db.String(1024))
    # status = db.Column(db.Integer)

    @property  # 将password方法变成属性，装饰器只管一个函数
    def password(self):  # 当调用User.password时，返回password is not a readable attribute/ password 不是一个可读属性。
        raise AttributeError('you have no authority')

    @password.setter  # 使得可以通过User.password =进行赋值
    def password(self, password):  # def password(self, password):函数，对self.password_hash赋值。
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def can(self, permissions):
        return self.role is not None and (self.role.permissions & permissions) == permissions

    def is_administrator(self):
        return self.can(Permission.ADMINISTER)

    @staticmethod
    def insert_users():
        username = "Admin"
        user = User.query.filter_by(username=username).first()
        if user is None:
            user = User(username=username)
        user.role_id = 1
        user.password = "Admin123"
        db.session.add(user)
        db.session.commit()

    def __repr__(self):
        return '<User %r>' % self.username

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)
