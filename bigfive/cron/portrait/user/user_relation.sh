#!/bin/bash

while ((1)); do
    python3 user_relation.py >> user_relation.log 2>&1
    echo 'Finish excute python...______'
    sleep 20
done