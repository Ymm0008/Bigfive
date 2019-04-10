#!/bin/bash

while ((1)); do
    python user_relation.py >> user_relation.log 2>&1
    sleep 20
done