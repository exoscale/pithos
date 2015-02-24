#!/bin/sh
### BEGIN INIT INFO
# Provides:          pithos
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Pithos object store
# Description:       Pithos, a cassandra-backed object store
### END INIT INFO

# Source function library.
. /etc/rc.d/init.d/functions

# Pull in sysconfig settings
[ -f /etc/sysconfig/pithos ] && . /etc/sysconfig/pithos

# PATH should only include /usr/* if it runs after the mountnfs.sh script
PATH=/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/bin
DESC="Pithos"
NAME=pithos
DAEMON=/usr/bin/pithos
DAEMON_ARGS="-f /etc/pithos/pithos.yaml"
DAEMON_USER=pithos
PID_FILE=/var/run/$NAME.pid
SCRIPT_NAME=/etc/init.d/$NAME
LOCK_FILE=/var/lock/subsys/$NAME

start()
{
    echo -n $"Starting ${NAME}: "
    ulimit -n $NFILES
    daemonize -u $DAEMON_USER -p $PID_FILE -l $LOCK_FILE $DAEMON $DAEMON_ARGS
    RETVAL=$?
    echo
    [ $RETVAL -eq 0 ] && touch $LOCK_FILE
    return $RETVAL
}

stop()
{
    echo -n $"Stopping ${NAME}: "
    killproc -p ${PID_FILE} -d 10 $DAEMON
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && rm -f ${LOCK_FILE} ${PID_FILE}
    return $RETVAL
}

do_reload() {
    echo -n $"Reloading ${NAME}: "
    killproc -p ${PID_FILE} $DAEMON -1
    RETVAL=$?
    echo
    return $RETVAL
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status -p ${PID_FILE} $DAEMON
        RETVAL=$?
        ;;
    reload|force-reload)
        reload
        ;;
    restart)
        stop
        start
        ;;
    *)
        N=/etc/init.d/${NAME}
        echo "Usage: $N {start|stop|status|restart|force-reload}" >&2
        RETVAL=2
        ;;
esac

exit $RETVAL
