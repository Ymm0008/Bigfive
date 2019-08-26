# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, Blueprint, session, redirect, make_response
from optparse import OptionParser
from bigfive import create_app
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from bigfive.extensions import db

#optparser = OptionParser()
#optparser.add_option('-p', '--port', dest='port', help='Server Http Port Number', default=5000, type='int')
#(options, args) = optparser.parse_args()

app = create_app()

manager = Manager(app=app)
db.init_app(app=app)
migrate = Migrate(app=app, db=db)
manager.add_command("db", MigrateCommand)

if __name__ == '__main__':
    app.debug = True
    #app.run(host='0.0.0.0', port=options.port)
    manager.run()
