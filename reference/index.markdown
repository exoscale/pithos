---
layout: default
title: Reference
---

# Pithos configuration

The pithos configuration file contains the following sections

- service
- logging
- options
- keystore
- bucketstore
- regions
 

## service configuration

Provide details of where the HTTP S3 service should be exposed.

```yaml
service:
  host: '127.0.0.1'
  port: 8080
```


## logging configuration

```yaml
logging:
  level: info
  console: true
  files:
    - "/tmp/pithos.log"
# overrides:
#   io.exo.pithos: debug
```

This provides a small veneer on top of log4j. If you wish, you can
entirely bypass this section and provide your on **log4j.properties**
file through the `log4j.configuration` JVM property.

## global options

```yaml
options:
  service-uri: 's3.example.com' 
  reporting: true
  server-side-encryption: true
  multipart-upload: true
  masterkey-provisioning: true
  masterkey-access: true
  default-region: 'CH-GV1'
```


## keystore configuration


Keystores associate an access key with
an organization and secret key.
 
They may offer provisioning capacities with the
masterkey. The default provider relies on keys
being defined inline.

```
keystore:
  keys:
    AKIAIOSFODNN7EXAMPLE:
# The master key allows provisioning operations
# when the masterkey-provisioning feature is
# set to true and will allow access to all
# buckets when masterkey-access is set to true
      master: true
      tenant: 'pyr@spootnik.org'
      secret: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    BKIAIOSFODNN7EXAMPLE:
      tenant: 'exoscale'
      secret: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
```


## bucketstore configuration


The bucketstore is ring global and contains information
on bucket location and global parameters.

Its primary aim is to hold bucket location and ownership
information.

The default provider relies on cassandra.

```yaml
bucketstore:
  default-region: 'CH-GV1'
  cluster: 'localhost'
  keyspace: 'storage'
```


## regions

Regions are composed of a metastore and an arbitrary number
of named storage classes which depend on a blobstore.

The metastore holds metadata for the full region, as well as
object storage-class placement information.

The default implementation of both metastore and blobstore
rely on cassandra.

```yaml
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
```
