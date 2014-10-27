Client compatibility list
=========================

This section needs your help

s3cmd
-----

Fully tested with the current API coverage. Here is a minimal
configuration you can put in ``~/.s3cfg``::

    [default]
    host_base = s3.example.com
    host_bucket = %(bucket)s.s3.example.com
    access_key = YOUR_ACCESS_KEY
    secret_key = YOUR_SECRET_KEY
    use_https = True

Adapt with your credentials and replace ``s3.example.com`` with the
value you specified for ``service-uri``.  ``use_https`` is needed only
if Pithos is served over TLS.

When testing locally, the following configuration can be used::

    [default]
    host_base = s3.example.com
    host_bucket = %(bucket)s.s3.example.com
    access_key = YOUR_ACCESS_KEY
    secret_key = YOUR_SECRET_KEY
    use_https = False
    proxy_host = localhost
    proxy_port = 8080
    

boto
----

Fully tested with the current API coverage.

cyberduck
---------

On-going integration

owncloud
--------

Working support

s3fs - s3 fuse support
----------------------

Working support. If you specified ``s3.example.com`` as
``service-uri``, you can mount the bucket ``bucket`` with the
following command::

    s3fs bucket /mnt/bucket -o url=https://s3.example.com

The credentials have to be specified in ``~/.passwd-s3fs``::

    YOUR_ACCESS_KEY:YOUR_SECRET_KEY

WAL-E - continuous archiving for Postgres
-----------------------------------------

Support for S3-compatible object stores was added in version 0.8 of WAL-E.
Configure WAL-E with the following environment variables:

===================== ============================
AWS_ACCESS_KEY_ID     YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY YOUR_SECRET_KEY
WALE_S3_ENDPOINT      https+path://s3.example.com
WALE_S3_PREFIX        s3://your-bucket/your-prefix
===================== ============================

Archiving WAL files
```````````````````

Postgresql needs the following settings in ``postresql.conf``::

    wal_level = archive
    archive_mode = on
    archive_command = 'envdir /etc/wal-e.d/env /path/to/wal-e wal-push %p'
    archive_timeout = 60

Once postgres is setup to send WAL files, make a base backup with ``envdir
/etc/wal-e.d/env /path/to/wal-e backup-push /path/to/postgres/data``

Restoring from archived WAL files
`````````````````````````````````

Pull a base backup::

    envdir /etc/wal-e.d/env /path/to/wal-e backup-fetch /path/to/postgres/data LATEST

Create a ``recovery.conf`` file in the postgres data dir with the following
content::

    restore_command = 'envdir /etc/wal-e.d/env /path/to/wal-e wal-fetch "%f" "%p"'

Start postgresql and check the logs to see its restore status.
