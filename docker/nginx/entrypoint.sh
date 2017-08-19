#!/bin/bash

if [ -z ${DH_SIZE+x} ]
then
  >&2 echo ">> no \$DH_SIZE specified using default"
  DH_SIZE="2048"
fi

DH="/etc/nginx/external/dh.pem"

if [ ! -e "$DH" ]
then
  echo ">> generating $DH with size: $DH_SIZE"
  openssl dhparam -out "$DH" $DH_SIZE
fi

if [ ! -e "/etc/nginx/external/cert.pem" ] || [ ! -e "/etc/nginx/external/key.pem" ]
then
  echo ">> generating self signed cert"
  openssl req -x509 -newkey rsa:4086 \
  -subj "/C=XX/ST=XXXX/L=XXXX/O=XXXX/CN=localhost" \
  -keyout "/etc/nginx/external/key.pem" \
  -out "/etc/nginx/external/cert.pem" \
  -days 3650 -nodes -sha256
fi

echo "$@"
exec "$@"
