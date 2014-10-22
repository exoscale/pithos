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

Working support
