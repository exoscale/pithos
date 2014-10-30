Administrator Guide
===================

This section describes common operations and deploy strategies for
*pithos*.

Overview of configuration
-------------------------

Configuring *pithos* involves configuring the following components:

- At least one Apache Cassandra cluster
- Pithos itself
- An optional reverse proxy to handle SSL termination
- Client configuration

Choosing your topology
----------------------

Since *pithos* can target different clusters for its *bucketstore*, *metastore* and *blobstore* (cf :ref:`Pithos Architecture`). A simple deployment will most likely
use the same cluster and keyspace for all data while a multi-region large set-up would
span several clusters: a multi-DC one holding the *bucketstore* and DC-local ones
for the *metastores* and *blobstores*.

A good rule of thumb would be to separate at least the *blobstore* from other
stores early on since most of the data actually lies in it and the rest of 
the stores only hold metadata.

Additionally you will need to choose between using a pithos daemons colocated
with cassandra daemons or host them on separate machines (in which case you
will need to make sure inter-node latency is low).

Configuration file
------------------

The *pithos* configuration file is split in several sections:

- ``logging``
- ``options``
- ``service``
- ``keystore``
- ``bucketstore``
- ``metastore``
- ``blobstore``
- ``reporters``

Logging configuration
~~~~~~~~~~~~~~~~~~~~~

*pithos* relies on log4j_ for logging. To provide an easier way
to configure logging than the standard approach of supplying a
``log4j.properties`` file.

.. sourcecode:: yaml

  logging:
    level: info
    console: true
    files:
      - "/tmp/pithos.log"
  # overrides:
  #   io.exo.pithos: debug

The following keys are recognized:

- ``level``: logging level, may be one of: ``debug``, ``info``, ``warn``,
  ``error``, ``all``, ``fatal``, ``trace``, ``off``
- ``console``: whether to output to the console
- ``json``: if true, log in a way compatible with logstash on stdout
- ``pattern``: a pattern to be used when logging, formatting options are available at [#]_.
- ``files``: a list of files to log to. each entry may either be a string, treated as a path or a map containing a ``path`` key and a ``json`` key, which when true enables logging in a way compatible with logstash.

.. note::
  If you wish to configure logging in ways that pithos' configuration
  file does not support, ``logging: {external: true}`` in the config
  will prevent *pithos* from attempting to modify logging and let you
  supply your own ```log4j.properties`` file.

.. _log4j: http://logging.apache.org/log4j/1.2/


Global options
~~~~~~~~~~~~~~

Only two options are currently recognized in the global options map:

.. sourcecode:: yaml

  options:
    service-uri: 's3.example.com' 
    default-region: 'CH-GV1'

``service-uri``
  Determines on which hostname buckets will be hosted. This is necessary
  since when used without the proxy functionality present in some clients,
  buckets will be accessed using bucket-specific hostnames.

``default-region``
  Determines which region buckets will be created on.


Keystore configuration
~~~~~~~~~~~~~~~~~~~~~~

*pithos* does not concern itself with handling tenants.
The *keystore* associates API keys with 

By default the keystore implementation reads key information
from its own config, there are no other public implementations but
writing your own is straightforward (cf :ref:`Custom Stores` for details).

.. sourcecode:: yaml 

  keystore:
    keys:
      AKIAIOSFODNN7EXAMPLE:
        tenant: 'email@example.com'
        secret: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

This configuration, means that a single tenant ``email@example.com`` will exist,
identified by the ``AKIAIOSFODNN7EXAMPLE`` access key and the accompanying 
secret key.

Additionally, a key entry may have a ``memberof`` entry in its output which 
designates which groups a tenant is part of, this is useful when building
ACLs to allow groups of tenant to operate on an object or bucket.


Bucketstore configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

*pithos* currently only provides a Cassandra-backed bucketstore.

.. sourcecode:: yaml

  bucketstore:
    default-region: 'myregion'
    cluster: 'localhost'
    keyspace: 'storage'

The default region will be used as the destination to create buckets in
when unspecified in requests.


Metastore configuration
~~~~~~~~~~~~~~~~~~~~~~~

*pithos* needs one *metastore* per available region, to store
object metadata for that region.

.. sourcecode:: yaml

  regions:
    myregion:
      metastore:
        cluster: 'localhost'
        keyspace: 'storage'
      storage-classes:
        standard:
          cluster: 'localhost'
          keyspace: 'storage'
          max-chunk: '128k'
          max-block-chunks: 1024

The bulk of the config is simple and accepts the same option than
the *bucketstore* configuration. There may be as many regions as
necessary.

The ``storage-classes`` key is used to provide a list of *blobstore*
clusters for a region, each providing a specific storage class
(cf :ref:`S3 Concepts` for details).

Blobstore configuration
~~~~~~~~~~~~~~~~~~~~~~~

As shown above, *blobstore* configuration happens within the storage class
blocks in regions:

.. sourcecode:: yaml

      storage-classes:
        standard:
          cluster: 'localhost'
          keyspace: 'storage'
          max-chunk: '128k'
          max-block-chunks: 1024

*blobstore* are named (here, `standard` is used) and have a maximum
chunk size as well as maximum block chunks.

Reporter Configuration
----------------------

Reporters provide a way to ship events out of pithos. As it stands pithos
only ships with a log4j reporter, but adding more is trivial and covered
in :ref:`Alternative Reporter`

Two types of events may be shipped:

- ``put`` events, when either a standard put or a complete multipart upload operation succeeds.
- ``delete`` events, when objects are deleted

Additional events may be added in the future.

Reporters is expected to be a list of reporter configurations. The default ``log4j`` reporter
only takes a ``level`` key, to indicate at which level messages should be logged.

Here we showcase a sample configuration with the default reporter logging at info and
a third-party one:

.. sourcecode:: yaml

  reporters:
    - level: info
    - use: some.namespace/alternative-reporter
      config-key: config-val


Using non-default stores
------------------------

The *pithos* configuration file format supports a very lightweight
and unobtrusive dependency injection syntax. What this allows you
to do is provide your own implementation of stores.

One of the typical use-cases would be to swap the default *keystore*
implementation with one which is able to interface with your infrastructure's
credential store.

If you have an alternative keystore implementation you will need to fulfill
these two steps to be able to use it:

- Make sure the compiled code is available on the JVM's classpath_
- Instruct *pithos* to use it instead of the default through the ``use`` directive.

Pretending you have an alternative *keystore* implementation exposed as the
``com.example.http-keystore/http-keystore`` namespace, which connects to an
HTTP url to retrieve credentials, 
living in a **JAR** archive at ``/usr/lib/pithos-http-keystore.jar``
you would then need to update the configuration file:

.. sourcecode:: yaml

  keystore:
    use: "com.example.http-keystore/http-keystore"
    url: "http://my-endpoint"
    user: "foo"
    password: "bar"

And start pithos in the following manner::

  CLASSPATH=/usr/lib/pithos/pithos-http-keystore.jar:/usr/lib/pithos/pithos.jar
  java -cp  $CLASSPATH io.pithos -f pithos.yaml

While this example targets the keystore, alternative implementations can be provided
for *bucketstore*, *metatstore*, *blobstore* and even *service* or *logging*.

An introductory article on the mechanism used can be found at [#]_

.. _classpath: http://docs.oracle.com/javase/tutorial/essential/environment/paths.html

Configuring Apache Cassandra
----------------------------

This section is in no way a replacement for Apache Cassandra's documentation, but
a few pointers can be helpful when configuring your clusters for *pithos*.

Bucketstore and Metastore Clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The *bucketstore* and *metastore* clusters can share the same properties, they should:

- Provide fast access to metadata
- Be as consistent as possible

As such, the following recommendations can be made:

Strong replication
  Metadata corruption could have a strong impact on your cluster, apart from
  data leakage, it would make it much harder to get back to stored data, as
  such you'll need to 

Fast access
  It would make sense to store *pithos* metadata on SSD-backed storage, to
  provide fast access to metadata.

Decoupling from *blobstore*
  Since the amount of data in the *bucketstore* and *metastore* is much
  smaller, decoupling it from the *blobstore* makes a lot of sense

Levelled compaction strategy
  The type of workload a *bucketstore* and *metastore* will encounter
  makes it a good candidate for levelled compactions.

Conserve tombstores
  While keeping it at the default 10 days might be a lot, ``gc_grace_seconds``
  should not be dropped too low on the *bucketstore* and *metastore* column
  families.

Blobstore clusters
~~~~~~~~~~~~~~~~~~

The *blobstore* cluster is a different beast. You might not have the means to
provide a full SSD-backed object-store and thus will need to tweak your Cassandra
cluster to deal with the heavy workload on slower disks:

Low replication
  While data should most likely not be stored at replication factor one, it might
  not make sense to store above 3.

Size tiered compaction strategy
  To avoid huge proliferation of SStables, a size-tiered compaction strategy seems
  like the best approach

Disabling of Row cache and Key cache
  There is little gain in caching whole inodes, and it should most likely be 
  avoided. Our recommendation would be to add an external layer of caching
  for hot objects.


Enabling SSL
------------

*pithos* currently does not provide a way to serve SSL buckets by itself,
the best approach is to rely on a separate web server to handle this, such
as nginx_.

When using SSL, the certificate used should authenticate requests to the
chosen `service-uri` in the configuration, as well as sub-domains of the
`service-uri`. For instance, if you have chosen ``pithos.example.com`` as
your `service-uri`, you will need a certificate for ``pithos.example.com`` and
a wildcard certificate for ``*.pithos.example.com`` to be able to authenticate
requests for ``my-bucket.pithos.example.com``, which is the standard way of
accessing buckets.

Reverse proxying pithos
-----------------------

There are a few gotchas when placing a reverse proxy in front of *pithos*

Large request buffers
  Since objects sent might be large, it is important to make sure
  that your webserver accepts large input payloads. When using nginx_,
  this is done by setting ``client_max_body_size`` to something
  appropriate.

Input Buffering
  If your webserver (for instance nginx_) does not provide a way to
  proxy input chunks one at a time, objects sent will be buffered by
  the webserver before being handed over to *pithos* which might
  put a strain on the memory consumed by the webservers.

Disabling output buffering
  Some responses in *pithos* explicitly ask for buffering to be disabled.
  You can choose to disable buffering altogether in your reverse proxy
  configuration, or instruct your webserver to carry over headers.

  When using nginx_, this is done with ``proxy_pass_header X-Accel-Buffering``.


.. _nginx: http://nginx.org

.. [#] https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/EnhancedPatternLayout.html
.. [#] http://spootnik.org/entries/2014/01/25_poor-mans-dependency-injection-in-clojure.html

       
