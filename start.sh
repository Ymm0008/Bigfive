#!/bin/bash
project_path=/var/www/Bigfive
cd project_path
uwsgi --ini uwsgi.ini
cp project_path/nginx.conf /etc/nginx/conf.d/nginx.conf
nginx -c /etc/nginx/nginx.conf
/bin/bash