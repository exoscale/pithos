#!/bin/sh

set -eux

# wait for cassandra being ready
until nc -z -w 2 cassandra 9042; do sleep 1; done

java -jar /pithos-standalone.jar -a install-schema || true

exec java -jar /pithos-standalone.jar -a api-run
