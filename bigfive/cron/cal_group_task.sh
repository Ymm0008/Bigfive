#!/bin/bash

while ((1)); do
    date
    nohup python cal_group_task.py >> cal_group_task.log &
    sleep 300
done