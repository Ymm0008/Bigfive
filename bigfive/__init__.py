# -*- coding: utf-8 -*-
from bigfive.colony.views import mod as colony_mod
from bigfive.firstpage.views import mod as firstpage_mod
from bigfive.person.views import mod as person_mod
from bigfive.hotevent.views import mod as hotevent_mod
from bigfive.politics.views import mod as politics_mod
from bigfive.group.views import mod as group_mod
from bigfive.user_manage.views import mod as user_manage_mod

from bigfive.cache import cache
from flask_cors import CORS
from flask import Flask
import os

basedir = os.path.abspath(os.path.dirname(__file__))


def create_app():
    app = Flask(__name__)
    app.register_blueprint(colony_mod)
    app.register_blueprint(firstpage_mod)
    app.register_blueprint(person_mod)
    app.register_blueprint(hotevent_mod)
    app.register_blueprint(politics_mod)
    app.register_blueprint(group_mod)
    app.register_blueprint(user_manage_mod)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'data.sqlite')
    app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True

    cache.init_app(app)
    app.secret_key = "ruman"
    CORS(app, supports_credentials=True)
    return app
