pithos: cassandra object storage
--------------------------------

Pithos is an S3-compatible object store leveraging cassandra
to distribute contents horizontally.

Documentation site lives at http://pithos.io

![Build Status](https://travis-ci.org/exoscale/pithos.svg)


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

    java -jar target/pithos-0.7.4-standalone.jar

To run pithos with an embedded cassandra daemon, first build the
target with:

    lein install
    lein sub uberjar

You can then run the fully standalone artifact:

    java -jar pithos-quickstart/target/pithos-quickstart-0.7.4-standalone.jar

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

    java -jar target/pithos-0.7.4-standalone.jar -a install-schema


## Test using the s3cmd command line client

Have a look at the minimal configuration file provided in
`doc/s3cmd.cfg`. If not running locally, remove the last lines, as
explained in the configuration file.

Create a bucket:

    s3cmd -c doc/s3cmd.cfg mb S3://<bucket>

List your buckets:

    s3cmd -c doc/s3cmd.cfg la
