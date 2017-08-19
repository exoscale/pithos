#!/bin/sh

set -eux

export PITHOS_CASSANDRA_HOST=${PITHOS_CASSANDRA_HOST:-cassandra}
export PITHOS_SERVICE_URI=${PITHOS_SERVICE_URI:-s3.example.com}

confd -onetime -backend env

# wait for cassandra being ready
until nc -z -w 2 $PITHOS_CASSANDRA_HOST 9042; do sleep 1; done

java -jar /pithos-standalone.jar -a install-schema || true

exec java -jar /pithos-standalone.jar -a api-run
