#!/bin/bash
TRIGGERFOLDER="/tmp/pydba_triggers"
MYPATH=$(dirname "$(readlink -f "$0")")
logfile=/var/log/pydba_triggers.log
if test \! -f /usr/bin/inotifywait; then
    echo "Falta por instalar el paquete inotify-tools"

fi
exec >> $logfile 2>&1

cat - <<__FILETEXTBODY  > /etc/cron.d/pydba_autorestart_watch
# /etc/cron.d/pydba_autorestart_watch: crontab entries for python autorestart_watch

SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

0 * * * * root  $MYPATH/autorestart_watch.sh &

__FILETEXTBODY

# Si cambias algo del fichero de arriba, necesitas:
# sudo ./autorestart_watch.sh
# sudo /etc/init.d/cron restart


function restart_services {
    unlink "$TRIGGERFOLDER"/restart_services 2>/dev/null
    date
    echo "Restart Services ... "
    test -f /etc/init.d/jrptguardian && /etc/init.d/jrptguardian restart
    test -f /etc/init.d/pgguardian && /etc/init.d/pgguardian restart
    test -f /etc/init.d/pdaservice && /etc/init.d/pdaservice restart
    echo "Restart Services ... Done. "
    date
    return 0
}

function restart_postgresql {
    unlink "$TRIGGERFOLDER"/restart_postgresql 2>/dev/null
    date
    echo "Restart PostgreSQL ... "
    # OJO, peligroso!!
    test -f /etc/init.d/postgresql && /etc/init.d/postgresql restart

    sleep 180
    echo "Restart PostgreSQL ... Done"
    date
    restart_services
    return 0
}

function execute {
    test -f "$TRIGGERFOLDER"/restart_services && restart_services
    test -f "$TRIGGERFOLDER"/restart_postgresql && restart_postgresql
}
echo " * autorestart starting * "
date

STARTTIME=$(date +%s)
mkdir -p "$TRIGGERFOLDER"/
chmod 0777 "$TRIGGERFOLDER" -R
touch "$TRIGGERFOLDER"/refresh
sleep 1
unlink "$TRIGGERFOLDER"/refresh
execute
for i in $(seq 6000); do
    if test -f /usr/bin/inotifywait; then
        /usr/bin/inotifywait -q -e CREATE -t 3600 "$TRIGGERFOLDER"/
    else
        sleep 2
    fi
    test -f "$TRIGGERFOLDER"/refresh && echo " * autorestart exiting (1) * " && date && exit 0
    CURTIME=$(date +%s)
    SECS=`expr $CURTIME - $STARTTIME`
    if [ $SECS -gt 3600 ]; then
        exit 0
    fi
    execute

done
execute
echo " * autorestart exiting * "
date







# COPY (SELECT '') TO '/tmp/pydba_triggers/restart_services';
