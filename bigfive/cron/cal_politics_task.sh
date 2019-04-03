#!/bin/bash

while ((1)); do
    date
    nohup python cal_politics_task.py >> cal_politics_task.log &
    sleep 300
done