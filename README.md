:warning: Project not under active development :warning:
-------------------------------------------------------------------

We're working on open-sourcing another iteration of pithos which offers much better protocol support and better performances.

No release date known yet



pithos: cassandra object storage
--------------------------------

Pithos is an S3-compatible object store leveraging cassandra
to distribute contents horizontally.

Documentation site lives at http://pithos.io

[![Build Status](https://travis-ci.org/exoscale/pithos.svg)](https://travis-ci.org/exoscale/pithos)


# Quickstart

You can use [docker-compose](https://docs.docker.com/compose/) to easily
run the current branch in a Docker container for testing purposes. The
Clojure and Cassandra Docker images will use around 1GB of disk space.

    docker-compose up

Create a new bucket:

    s3cmd --config doc/s3cmd.cfg mb s3://my-bucket
    Bucket 's3://my-bucket/' created

    s3cmd --config doc/s3cmd.cfg ls s3://
    2016-05-27 09:04  s3://my-bucket

To build an run Pithos manually, continue reading.

# Prerequisites

In order to build and run Pithos you will need the following components:

* Working Java Runtime Environment version 7 or higher
* A [Cassandra](http://cassandra.apache.org/) cluster in version 2 or higher
* [Leiningen](https://github.com/technomancy/leiningen) Clojure package builder

# Build

To build pithos run:

    lein uberjar

you will get a standalone Java jar file in the `target/` directory

# Run

To run Pithos manually start it with

    java -jar target/pithos-0.7.5-standalone.jar

Pithos will expect finding a valid configuration file under `/etc/pithos/pithos.yaml`. You can specify a distinct config file using the `-f` switch.

The following startup switches are available:

     Switches                 Default  Desc
     --------                 -------  ----
     -h, --no-help, --help    false    Show Help
     -f, --path                        Configuration file path
     -q, --no-quiet, --quiet  false    Never output to stdout
     -a, --action             api-run  Specify an action (api-run, install-schema)

## Bootstrapping the environment

Pithos includes a schema definition file in order to bootstrap your Cassandra cluster.
To install the schema, run:

    java -jar target/pithos-0.7.5-standalone.jar -a install-schema


## Test using the s3cmd command line client

Have a look at the minimal configuration file provided in
`doc/s3cmd.cfg`. If not running locally, remove the last lines, as
explained in the configuration file.

Create a bucket:

    s3cmd -c doc/s3cmd.cfg mb S3://<bucket>

List your buckets:

    s3cmd -c doc/s3cmd.cfg la
