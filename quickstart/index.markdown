---
layout: default
title: Getting Started with Pithos
---

pithos: cassandra object storage
--------------------------------

Pithos is a fully S3 compatible object store developed and used at [exoscale](https://www.exoscale.ch), a public cloud provider.

# Prerequisites

In order to build and run Pithos you will need the following components:

* Working Java Runtime Environment version 7 or higher
* A [Cassandra](http://cassandra.apache.org/) cluster in version 2 or
  higher (2.1 is recommended)
* [Leiningen](https://github.com/technomancy/leiningen) Clojure package builder

# Build

To build pithos run:

    lein uberjar

you will get a standalone Java jar file in the `target/` directory

# Run

To run Pithos manually start it with

    java -jar target/pithos-0.6.0-standalone.jar

Pithos will expect finding a valid configuration file under `/etc/pithos/pithos.yaml`. You can specify a distinct config file using the `-f` switch.

The following startup switches are available:

     Switches                 Default  Desc
     --------                 -------  ----
     -h, --no-help, --help    false    Show Help
     -f, --path                        Configuration file path
     -q, --no-quiet, --quiet  false    Never output to stdout
     -a, --action             api-run  Specify an action (api-run,
     install-schema)

## Configuration

Please refer to the [documentation reference](/reference) for a
detailed walk-through of configuration options.

## Bootstrapping the environment

Pithos includes a schema definition file in order to bootstrap your Cassandra cluster.
To install the schema, run:

    java -jar target/pithos-0.6.0-standalone.jar -a install-schema


## Test using the s3cmd command line client

configure your client:

    s3cmd --configure

and provide the key and secret defined in the config file.

If running locally, also specify protocol, proxy and port options as follow (or adjust to your running config):

* Use HTTPS protocol: False
* HTTP Proxy server name: localhost
* HTTP Proxy server port: 8080

Create a bucket:

    s3cmd md S3://BUCKET

List your buckets:

    s3cmd la
