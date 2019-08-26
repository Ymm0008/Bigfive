# -*- coding:utf-8 -*-
from functools import wraps
from flask import session, redirect, abort
from .models import User, Permission
# from ..config import IS_Admin


# def login_required(f):
#     @wraps(f)
#     def decorated_function(*args, **kwargs):
#         if not session:
#             if IS_Admin:
#                 user = User.query.filter(User.username == 'Admin').first()
#                 session['username'] = 'Admin'
#                 session['role'] = user.role_id
#                 session['uid'] = user.id
#                 # session['group'] = user.group_id
#                 return f(*args, **kwargs)
#             return redirect('/user_manage/login')
#         return f(*args, **kwargs)
#
#     return decorated_function


def permission_required(permission):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = User.query.filter_by(id=session['uid']).first()
            if not user.can(permission):
                # return redirect('/manage/login')
                abort(403)
            return f(*args, **kwargs)

        return decorated_function

    return decorator


def admin_required(f):
    return permission_required(Permission.ADMINISTER)(f)
