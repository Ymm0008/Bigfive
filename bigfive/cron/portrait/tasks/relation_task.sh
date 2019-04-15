#!/bin/bash

while ((1)); do
    python relation_task.py >> log/relation_task.log 2>&1
    sleep 20
done