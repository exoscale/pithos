The Pithos Guide
================

.. image:: _static/pithos.svg
   :alt: pithos log
   :align: right

*pithos* is a daemon which provides an S3-compatible frontend for storing files
in a `Cassandra`_ cluster.

*pithos* provides the ability to build complex object storage topologies spanning
multiple regions and focuses on the following:

Scalability
  By relying on Apache Cassandra, pithos splits your files (objects) in small chunks
  which are replicated across a cluster of machines. This allows pithos to provide
  the following guarantees:

    - Fast writes
    - High Availability
    - Partition tolerance

Compatibility
   While there are no wide-spread official standard for object storage, the S3 protocol
   has become a de-facto standard and has thus been chosen as pithos' protocol.
   This means you can start using your favorite S3 tools to work with pithos, such as:

   - `s3cmd`_
   - `boto`_

Simplicity
  Pithos was built with ease of use and operability in mind. It should be easy to get started, require as few moving parts as possible and still be relatively easy to extend for larger installations. Pithos is distributed as a single executable JAR-file and relies on a YAML configuration file. As many of the JVM specifics are hidden from the administrator.

*pithos* is sponsored by exoscale_

.. _Cassandra: http://cassandra.apache.org/
.. _s3cmd: http://s3tools.org/
.. _boto: https://github.com/boto/boto
.. _exoscale: https://exoscale.ch

.. toctree::
   :maxdepth: 2

   quickstart
   concepts
   administrator
   api
   developer
   clients



