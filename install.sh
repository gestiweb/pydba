APP_PATH=$(dirname $(readlink $0))

aptitude install python python-pygresql python-mysqldb python-json

test -e /usr/local/pydba && unlink /usr/local/pydba

ln -s $APP_PATH/pydba.py /usr/local/pydba




