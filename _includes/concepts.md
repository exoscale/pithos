## Introduction

Pithos aims to be a simple and effective solution for object storage
needs. It focuses on the following aspects:

-   Compatibility with the S3 REST API
-   Simple configuration
-   Ease of integration in existing environments

## Language

Pithos is written using Clojure for tight integration with Java based
Apache Cassandra (tm) and for a small code base.

## Layout 

Pithos splits object storage across regions, each having one or more
storage class. Metadata is stored globally, object data is isolated on a
region. Bucket objects may only be stored in a single region.

Most terminology in Pithos is inherited from Amazon S3. Data is spread
amongst several **regions** which only share metadata.

Pithos works by exposing the following entities:

-   *Ring*: The global storage service.
-   *Region*: A location denominator, mapping to a physical location where
    bucket object data is stored.
-   *Storage Class*: Denotes the redundancy at which data is stored.
-   *Bucket*: Logical aggregation of objects sharing common properties.
-   *Object*: Representation of a file as uploaded by a client.
-   *Tenant*: A client-organization.
-   *User*: A member of a tenant, identified by a unique ID or email
    address.

Additionally, pithos separates storage in different layers:

-  Keystore: ring global storage of credentials.
-  Metastore: ring global storage of bucket location information.
-  RegionMetastore: region local storage of object metadata.
-  Regionstore: region local and per-storage class object storage.

## Configuration

Pithos is configured by editing a single file:
    
    /etc/pithos/pithos.yaml


## Request Lifecycle

<img src="/img/request_lifecycle.png" alt="request lifecycle" style="max-width: 100%" />

## Data Layout

### Bucketstore Keyspace

Buckets are indexed by tenant and bucket name. They are a simple entity
that can aggregate common properties across all descendant paths and
inodes.

<table class="table">
<thead>
<tr class="header">
<th align="left">bucket</th>
<th align="left">key: bucket</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left">tenant</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">acl</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">cors</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">created</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">policy</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">region</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">tenant</td>
<td align="left">text <small>(indexed)</small></td>
</tr>
<tr class="even">
<td align="left">versioned</td>
<td align="left">boolean</td>
</tr>
</tbody>
</table>

### Metastore Keyspace

Objects allow the construction of an arbitrary tree of inodes. Objects are
semantically sorted and although the data model does not account for
hierarchy, the use of user-supplied delimiters can realize ad-hoc
hierarchies.

As a side-effect, for a specific path prefix, hierarchies are built by
retrieving all children objects.

<table class="table">
<thead>
<tr class="header">
<th align="left">object</th>
<th align="left">key: (bucket,object)</th>
</tr>
</thead>
<tbody>
<tr class="even">
<td align="left">bucket</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">object</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">inode</td>
<td align="left">uuid</td>
</tr>
<tr class="even">
<td align="left">version</td>
<td align="left">uuid</td>
</tr>
<tr class="odd">
<td align="left">acl</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">atime</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">checksum</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">size</td>
<td align="left">bigint</td>
</tr>
<tr class="odd">
<td align="left">storageclass</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">metadata</td>
<td align="left">map&lt;text,text&gt;</td>
</tr>
</tbody>
</table>

Inodes represent an object, independent of its actual location on the
hierarchy, to allow for efficient operations on the file system (links,
moves).

Inodes are versioned, each object update resulting in a new version.
Inodes might not be published, i.e: not yet ready to be seem in the path
hierarchy.

Inode versions do not store data, data is instead stored in a list of
blocks.

<table class="table">
<thead>
<tr class="header">
<th align="left">inode</th>
<th align="left">key ((inode, published)version)</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left">inode</td>
<td align="left">uuid</td>
</tr>
<tr class="even">
<td align="left">published</td>
<td align="left">boolean</td>
</tr>
<tr class="odd">
<td align="left">version</td>
<td align="left">timeuuid</td>
</tr>
<tr class="even">
<td align="left">atime</td>
<td align="left">timestamp</td>
</tr>
<tr class="odd">
<td align="left">attrs</td>
<td align="left">map<text,text></td>
</tr>
<tr class="even">
<td align="left">tags</td>
<td align="left">set<text></td>
</tr>
<tr class="odd">
<td align="left">checksum</td>
<td align="left">text</td>
</tr>
</tbody>
</table>

Inode blocks are a relation table holding a list of offsets at which
blocks start.

<table class="table">
<thead>
<tr class="header">
<th align="left">inode_blocks</th>
<th align="left">key ((inode,version), block)</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left">inode</td>
<td align="left">uuid</td>
</tr>
<tr class="even">
<td align="left">version</td>
<td align="left">timeuuid</td>
</tr>
<tr class="odd">
<td align="left">block</td>
<td align="left">bigint</td>
</tr>
</tbody>
</table>

### Data KeyspaceBlocks

store data in a list of chunks.

<table class="table">
<thead>
<tr class="header">
<th align="left">block</th>
<th align="left">key ((inode, version,block), offset)</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left">inode</td>
<td align="left">uuid</td>
</tr>
<tr class="even">
<td align="left">version</td>
<td align="left">timeuuid</td>
</tr>
<tr class="odd">
<td align="left">block</td>
<td align="left">bigint</td>
</tr>
<tr class="even">
<td align="left">offset</td>
<td align="left">bigint</td>
</tr>
<tr class="odd">
<td align="left">chunksize</td>
<td align="left">int</td>
</tr>
<tr class="even">
<td align="left">payload</td>
<td align="left">blob</td>
</tr>
</tbody>
</table>


