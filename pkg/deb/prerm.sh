#!/bin/sh
set -e
if [ -x "/etc/init.d/pithos" ]; then
        invoke-rc.d pithos stop || exit $?
fi
