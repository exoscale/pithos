#!/bin/sh
# Fakeroot and lein don't get along, so we set ownership after the fact.
set -e

chown -R root:root /usr/lib/pithos
chown root:root /usr/bin/pithos
chown pithos:pithos /var/log/pithos
chown pithos:pithos /etc/pithos.yaml
chown root:root /etc/init.d/pithos

if [ -x "/etc/init.d/pithos" ]; then
	invoke-rc.d pithos start || exit $?
fi
