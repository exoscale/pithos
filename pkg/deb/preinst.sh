#!/bin/sh
# Create pithos user and group
set -e

USERNAME="pithos"
GROUPNAME="pithos"
getent group "$GROUPNAME" >/dev/null || groupadd -r "$GROUPNAME"
getent passwd "$USERNAME" >/dev/null || \
      useradd -r -g "$GROUPNAME" -d /usr/lib/pithos -s /bin/false \
      -c "Pithos object store" "$USERNAME"
exit 0
