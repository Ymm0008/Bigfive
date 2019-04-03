#!/bin/bash

while ((1)); do
    date
    nohup python cal_event_task.py >> cal_event_task.log &
    sleep 300
done
