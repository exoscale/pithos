pithos: cassandra object storage
--------------------------------

Pithos is a fully S3 compatible object store developed and used at [exoscale](https://www.exoscale.ch), a public cloud provider.

The full design documentation and schema layout is available [here](doc/design.org)


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

    java -jar target/pithos-0.1.10-standalone.jar

To run pithos with an embedded cassandra daemon, first build the
target with:

    lein install
    lein sub uberjar

You can then run the fully standalone artifact:

    java -jar pithos-quickstart/target/pithos-quickstart-0.1.10-standalone.jar

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

    java -jar target/pithos-0.1.10-standalone.jar -a install-schema


## Test using the s3cmd command line client

configure your client:

    s3cmd --configure

and provide the key and secret defined in the config file

If running locally, also specify protocol, proxy and port options as follow (or adjust to your running config):

* Use HTTPS protocol: False
* HTTP Proxy server name: localhost
* HTTP Proxy server port: 8080

Create a bucket:

    s3cmd mb S3://<bucket>

List your buckets:

    s3cmd la
