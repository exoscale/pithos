Quickstart Guide
================

Getting up and running with pithos involves two things which
we'll cover in this quick walk-through:

- Installing and running Apache Cassandra
- Installing and running pithos

Alternately, there is a version of pithos which embeds Apache Cassandra.

Obtaining pithos
----------------

Pithos is released in both source and binary. Binary distributions come in
two flavors: standard and standalone with embedded cassandra.

Binary releases
~~~~~~~~~~~~~~~

Binary release are the simplest way to get started and are hosted on github:
https://github.com/exoscale/pithos/releases.

Each release contains:

- A source code archive 
- A standard build (*pithos-VERSION-standalone.jar*)
- A quickstart build which embeds cassandra (*pithos-quickstart-VERSION-standalone.jar*)


Requirements
------------

Runtime requirements
~~~~~~~~~~~~~~~~~~~~

Runtime requirements for pithos are kept to a minimum

- Java 7 Runtime (Sun JDK recommended)
- Apache Cassandra 2.1 (for standard distribution)

Build requirements
~~~~~~~~~~~~~~~~~~

If you wish to build pithos you will additionally need the
`leiningen`_ build tool to produce working artifacts.

.. _leiningen: https://leiningen.org

Minimal configuration
---------------------

Pithos is configured with a single configuration file, formatted in YAML_.


.. _YAML: http://yaml.org

.. sourcecode:: yaml

  #
  ## pithos main configuration
  ## =========================
  #
  # This file contains the following sections
  #   - service
  #   - logging
  #   - options
  #   - keystore
  #   - bucketstore
  #   - regions
   
  
  ## service configuration
  ## ---------------------
  #
  # indicates
  service:
    host: '127.0.0.1'
    port: 8080
  
  
  ## logging configuration
  ## ---------------------
  logging:
    level: info
    console: true
    files:
      - "/tmp/pithos.log"
  # overrides:
  #   io.exo.pithos: debug
  
  
  ## global options
  ## --------------
  options:
    service-uri: 's3.example.com' 
    reporting: true
    server-side-encryption: true
    multipart-upload: true
    masterkey-provisioning: true
    masterkey-access: true
    default-region: 'CH-GV1'
  
  
  ## keystore configuration
  ## ----------------------
  #
  # Keystores associate an access key with
  # an organization and secret key.
  # 
  # They may offer provisioning capacities with the
  # masterkey. The default provider relies on keys
  # being defined inline.
  keystore:
    keys:
      AKIAIOSFODNN7EXAMPLE:
  # The master key allows provisinning operations
  # when the masterkey-provisioning feature is
  # set to true and will allow access to all
  # buckets when masterkey-access is set to true
        master: true
        tenant: 'pyr@spootnik.org'
        secret: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
      BKIAIOSFODNN7EXAMPLE:
        tenant: 'exoscale'
        secret: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
  
  
  ## bucketstore configuration
  ## -------------------------
  #
  # The bucketstore is ring global and contains information
  # on bucket location and global parameters.
  #
  # Its primary aim is to hold bucket location and ownership
  # information.
  #
  # The default provider relies on cassandra.
  bucketstore:
    default-region: 'CH-GV1'
    cluster: 'localhost'
    keyspace: 'storage'
  
  
  ## regions
  ## -------
  #
  # Regions are composed of a metastore and an arbitrary number
  # of named storage classes which depend on a blobstore.
  #
  # The metastore holds metadata for the full region, as well as
  # object storage-class placement information.
  #
  # The default implementation of both metastore and blobstore
  # rely on cassandra.
  #
  regions:
    CH-GV1:
      metastore:
        cluster: 'localhost'
        keyspace: 'storage'
      storage-classes:
        standard:
          cluster: 'localhost'
          keyspace: 'storage'
          max-chunk: '128k'
          max-block-chunks: 1024
  

Running pithos
--------------

Command line arguments
~~~~~~~~~~~~~~~~~~~~~~

Pithos accepts the following arguments::

     Switches                 Default  Desc                                        
     --------                 -------  ----                                        
     -h, --no-help, --help    false    Show Help                                   
     -f, --path                        Configuration file path                     
     -q, --no-quiet, --quiet  false    Never output to stdout                      
     -a, --action             api-run  Specify an action (api-run, install-schema) 
  
The only non-standard option is the `-a` option which allows either starting
the service normally or converging a cassandra schema.

Running the standalone version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The standalone version can just be run against a configuration file::

  java -jar pithos-quickstart-VERSION-standalone.jar -f pithos.yaml

Running against an existing cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first time you run a standard pithos distribution, you will need
to converge the necessary cassandra schema::

  java -jar pithos-VERSION-standalone.jar -f pithos.yaml -a install-schema

This will create the necessary keyspaces and column families in cassandra.
Once finished, pithos can be started normally::

  java -jar pithos-VERSION-standalone.jar -f pithos.yaml
  

