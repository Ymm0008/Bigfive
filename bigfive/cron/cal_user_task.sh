#!/bin/bash

while ((1)); do
    date
	nohup python cal_user_task.py >> cal_user_task.log &
	sleep 300
done
