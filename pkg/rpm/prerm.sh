#!/bin/sh
set -e
if [ -x "/etc/init.d/pithos" ]; then
        service pithos stop || exit $?
fi
