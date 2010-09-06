#!/bin/bash
APP_FILENAME=$(readlink "$0" -m)
echo "App filename: $APP_FILENAME ($0)"
APP_PATH=$(dirname "$APP_FILENAME")
echo "App lives in: $APP_PATH"

aptitude install python python-pygresql python-mysqldb python-json

[[ $APP_PATH != "" ]] && (
unlink /usr/local/bin/pydba 2>/dev/null

ln -s $APP_PATH/pydba.py /usr/local/bin/pydba
)



