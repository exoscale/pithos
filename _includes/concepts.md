## Origin

**Pithos** is the Greek name of a large storage container.

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

Pithos splits object storage accross regions, each having one or more
storage class. Metadata is stored globally, object data is isolated on a
region. Bucket objects may only be stored in a single region.

Most terminology in Pithos is inherited from Amazon S3. Data is spread
amongst several **regions** which only share metadata.

Pithos works by exposing the following entities:

-   Ring: The global storage service.
-   Region: A location denominator, mapping to a physical location where
    bucket object data is stored.
-   Storage Class: Denotes the redundancy at which data is stored.
-   Bucket: Logical aggregation of objects sharing common properties.
-   Object: Representation of a file as uploaded by a client.
-   Tenant: A client-organization.
-   User: A member of a tenant, identified by a unique ID or email
    address.

Additionally, pithos internals stores information in different locations:

-   Keystore: ring global ID to details storage.
-   Metastore: ring global lightweight storage of bucket owner and
    location.

-   Keystore: Interface allowing ID to tenant, group and other details
    lookup
-   Metastore: Interface allowing interaction with the global ring
    metadata
-   RegionMetastore:
-   Regionstore: Interface allowing interfaction with

## Configuration

Pithos is configured by editing a single file:
    
    /etc/pithos/pithos.yaml


## Request Lifecycle


## Data Layout

### Metadata Keyspace

Buckets are indexed by tenant and bucket name. They are a simple entity
that can aggregate common properties across all descendant paths and
inodes.

<table>
<thead>
<tr class="header">
<th align="left">bucket</th>
<th align="left">key: (tenant,bucket)</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left">tenant</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">bucket</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">attrs</td>
<td align="left">map<text,text></td>
</tr>
<tr class="even">
<td align="left">tags</td>
<td align="left">set<text></td>
</tr>
</tbody>
</table>

Paths allow the construction of an arbitrary tree of inodes. Paths are
semantically sorted and although the data model does not account for
hierarchy, the use of user-supplied delimiters can realize ad-hoc
hierarchies.

As a side-effect, for a specific path prefix, hierarchies are built by
retrieving all children paths.

<table>
<thead>
<tr class="header">
<th align="left">path</th>
<th align="left">key: ((tenant,bucket),path)</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td align="left">tenant</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">bucket</td>
<td align="left">text</td>
</tr>
<tr class="odd">
<td align="left">path</td>
<td align="left">text</td>
</tr>
<tr class="even">
<td align="left">inode</td>
<td align="left">uuid</td>
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

<table>
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

<table>
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

<table>
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


