pithos: cassandra object storage
--------------------------------

Pithos is a fully S3 compatible object store developped and used at [exoscale](https://www.exoscale.ch), a public cloud provider.

The full design documentation and schema layout is available [here](doc/design.org)


# Prerequisites

In order to build and run Pithos you will need the following components:

* Working Java Runtime Environment version 6 or higher
* A Cassandra cluster in version 2 or higher
* [Leiningen](https://github.com/technomancy/leiningen) Clojure package buider

# Build

To build pithos run:

    lein uberjar

you will get a standalone Java jar file in the target/ directory

# Run

To run Pithos manually start it with

    java -jar target/pithos-0.1.0-standalone.jar

The following startup switches are available:

     Switches                 Default  Desc
     --------                 -------  ----
     -h, --no-help, --help    false    Show Help
     -f, --path                        Configuration file path
     -q, --no-quiet, --quiet  false    Never output to stdout
     -a, --action             api-run  Specify an action (api-run, install-schema)


