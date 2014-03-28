#!/bin/sh
set -e
if [ "$1" = "purge" ] ; then
	update-rc.d pithos remove >/dev/null
fi
