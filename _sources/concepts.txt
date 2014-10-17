Design and Concepts
===================

This section describe the overall design and concepts within *pithos* and
its interaction with Apache Cassandra.

.. _S3 Concepts:

S3 Concepts
-----------

Since *pithos* exposes the AWS S3 API, some of its properties have direct impact on 
pithos's design.

Terminology
~~~~~~~~~~~

If you're not familiar with S3, the following terms need clarification:

Bucket
  A bucket is a named container for objects. A bucket belongs to a region
  and may contain an arbitrary number of objects, potentially in different
  storage classes.

Region
  A region hosts the metadata for objects. Regions may have several available
  storage classes.

Object
  An object is the S3 representation of a file. There is no filesystem hierarchy
  in S3 even though some mechanisms may help in emulating one.

Storage Class
  A storage class is a destination for objects with specific storage properties.
  A typical use case for storage properties is to provide cheap storage with
  low safety properties in a *reduced redundancy* class and standard safety
  properties in a *standard* class.

A global bucket namespace
~~~~~~~~~~~~~~~~~~~~~~~~~

The first thing of note is that an S3-compatible object store will expose a
global namespace for buckets across all tenants. Bucket names are first come,
first served and hold very little information. The most important properties stored
in a bucket are:

- The bucket name
- The bucket's tenant
- The bucket's ACL
- The bucket's CORS configuration
- The region the bucket's objects will be stored in


Cassandra concepts
------------------

*pithos* relies on `Apache Cassandra`_, which brings its own set of terminology and
concepts:

Cluster
  A Cassandra cluster is a collection of a number of nodes which share
  properties such as available schema and data.

Node
  A Cassandra node is a participant in a cluster. It can be seen as the
  equivalent of an SQL instance.

Keyspace
  A Cassandra keyspace holds a collection of column families which share
  similar properties, such as a replication factor. It can be seen as the
  equivalent of an SQL database

Column Family
  A Cassandra column family stores keyed rows of data sharing a specific
  schema. It can be seen as the equivalent of an SQL table.

.. _Apache Cassandra: http://cassandra.apache.org

.. _Pithos Architecture:

Pithos Architecture
-------------------

To isolate concerns and provide flexibility when building an object store service,
*pithos* is built around the notion of different stores which are all responsible
for a subset of the overall object store data. Each of the store can be independently
configured and may point to a separate location. 

*pithos* provides default implementations of each store targetting cassandra (except for
the keystore which is static by default) but the configuration file format allows for
providing different implementations if necessary.

The Keystore
~~~~~~~~~~~~

*pithos* does not concern itself with handling tenants, it relies on a
keystore to provide an association from API key to tenant information.

Out of the box, *pithos* only ships with a simple config-file based keystore,
but writing a separate one is trivial and covered in the developer documentation.

A key lookup in the keystore should yield a map of the following attributes::

  {
    "master": false,
    "tenant": "tenant name",
    "secret": "secret key",
    "memberof": ["group1", "group2"]
  }

This properties are then used by pithos to authenticate requests.

The Bucketstore
~~~~~~~~~~~~~~~

The bucketstore holds an association of bucket name to tenant and properties.


==========  ========  ===============================
Column      Type      Description
==========  ========  ===============================
bucket      text      bucket name
acl         text      serialized ACL definition
cors        text      serialized CORS definition
created     text      ISO8601 timestamp
policy      text      (unused)
region      text      region name
tenant      text      tenant name
versioned   boolean   (unused)
website     text      website configuration (unused)
==========  ========  ===============================


The Metastore
~~~~~~~~~~~~~

The metastore hold object metadata for a specific region. It also associates
objects with their storage class location and keeps track of ongoing 
multipart object uploads.

An object has the following properties:

============  ========  ==================================
Column        Type      Description
============  ========  ==================================
bucket        text      bucket name
object        text      full object path
acl           text      serialized ACL definition
atime         text      ISO8601 timestamp of access time
checksum      text      MD5 checksum of object
size          bigint    total file size
inode         uuid      object inode ID
version       uuid      object version ID
storageclass  text      storage class where data is stored
metadata      map       additional attributes
============  ========  ==================================

Multipart upload descriptions span two entities, the first
stores a list of ongoing uploads:

============  ========  ==================================
Column        Type      Description
============  ========  ==================================
bucket        text      bucket name
object        text      full object path
upload        uuid      object inode ID
metadata      map       additional attributes
============  ========  ==================================

The second stores information on each uploaded part

============  ========  ==================================
Column        Type      Description
============  ========  ==================================
bucket        text      bucket name
object        text      full object path
upload        uuid      object inode ID
partno        int       part number within this upload
cheksum       text      MD5 checksum of this part
inode         uuid      upload part inode ID
version       uuid      upload part version ID
modified      text      ISO 8601 timestamp of part upload
size          bigint    upload part total size
============  ========  ==================================


The Blobstore
~~~~~~~~~~~~~

The blobstore holds data for your files. Data is stored
based on inode ids. Data is stored across two entities
by default.

The first one is a list of blocks within an inode:

============  ========  ==================================
Column        Type      Description
============  ========  ==================================
inode         uuid      inode ID
version       uuid      version ID
block         bigint    offset of block start
size          bigint    block size
============  ========  ==================================

The second one holds data within a block:

============  ========  ==================================
Column        Type      Description
============  ========  ==================================
inode         uuid      inode ID
version       uuid      version ID
block         bigint    offset of block start
offset        bigint    offset of payload within object
chunksize     int       payload size
payload       blob      bytes for this payload
============  ========  ==================================

