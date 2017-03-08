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
    signature_v2 = True

Adapt with your credentials and replace ``s3.example.com`` with the
value you specified for ``service-uri``.  ``use_https`` is needed only
if Pithos is served over TLS. Currently pithos doesn't support v4
signatures so the ``signature_v2`` flag is necessary.

When testing locally, the following configuration can be used::

    [default]
    host_base = s3.example.com
    host_bucket = %(bucket)s.s3.example.com
    access_key = YOUR_ACCESS_KEY
    secret_key = YOUR_SECRET_KEY
    use_https = False
    signature_v2 = True
    proxy_host = localhost
    proxy_port = 8080
    

libcloud
--------

Working support with the S3 provider::

    from libcloud.storage.types import Provider
    from libcloud.storage.providers import get_driver
    cls = get_driver(Provider.S3)
    driver = cls('api key', 'api secret key', host='s3.example.com')
    driver.list_containers()

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

elasticsearch - index backup and restore
----------------------------------------

Snapshotting and restoring indices to Pithos is supported thanks to the `AWS
Cloud Plugin`_. To configure a snapshot repository that points to your pithos
installation, simply add to your ``/etc/elasticsearch/elasticsearch.yml``:

.. code-block:: yaml

    cloud:
      aws:
        access_key: <your key>
        secret_key: <your secret>
        s3:
          protocol: https
          endpoint: s3.example.com

Then create your repository::

    $ curl -XPUT 'http://localhost:9200/_snapshot/pithos' -d '{
        "type": "s3",
        "settings": {
            "bucket": "es-snapshots"
        }
    }'

Starting with version 2.4.2 of the plugin, all settings can be provided
per-repository::

    $ curl -XPUT 'http://localhost:9200/_snapshot/pithos' -d '{
        "type": "s3",
        "settings": {
            "bucket": "es-snapshots",
            "access_key": "your key",
            "secret_key": "your secret",
            "protocol": "http",
            "endpoint": "s3.example.com",
        }
    }'

.. _AWS Cloud Plugin: https://github.com/elasticsearch/elasticsearch-cloud-aws

AWS Languages SDKs
------------------

In general, AWS Language SDKs can work with Pithos with the following
configuration:

* In ``~/.aws/config``::

      [default]
      s3 =
          signature_version = s3

* In ``~/.aws/credentials``::

      [default]
      aws_access_key_id = <your key>
      aws_secret_access_key = <your secret>

You can have multiple profiles instead of altering the ``[default]``
configuration. Simply repeat configuration sections and name them ``[profile
<profile name>]``

Shell (awscli)
``````````````

Install `awscli`_, then::

    aws s3 ls --endpoint-url=https://your-endpoint

To use a non-default profile::

    aws s3 ls --endpoint-url=https://your-endpoint --profile=<profile-name>

Python (boto3)
``````````````

Install `boto3`_ and create a Pithos client like this:

.. code-block:: python

    import boto3.session

    session = boto3.session.Session()
    client = session.client('s3', endpoint_url='https://pithos-endpoint')
    client.list_buckets()

To use a non-default profile:

.. code-block:: python

    import boto3.session
    session = boto3.session.Session(profile_name='profile-name')
    client = session.client('s3', endpoint_url='https://pithos-endpoint')

Python (boto)
`````````````

`Boto`_ version 2 is boto3's ancestor but is still widely used. It doesn't
take ``~/.aws/*`` configuration files into account.

.. code-block:: python

    from boto.s3.connection import S3Connection, OrdinaryCallingFormat

    connection = S3Connection(key, secret, host='pithos-endpoint',
                              port=443, is_secure=True,
                              calling_format=OrdinaryCallingFormat())
    bucket = connection.get_bucket('your-bucket')

.NET
````

Install `AWSSDK.S3`_, then:

.. code-block:: csharp

    Amazon.AWSConfigsS3.UseSignatureVersion4 = false;
    var config = new Amazon.S3.AmazonS3Config()
    {
        ServiceURL = host,
        SignatureVersion = "s3",
    };
    var client = new Amazon.S3.AmazonS3Client(apikey, secretKey, config);

Java
````

Install `AWS SDK for Java`_, then:

.. code-block:: java

    import com.amazonaws.ClientConfiguration;
    import com.amazonaws.services.s3.AmazonS3Client;

    ClientConfiguration config = new ClientConfiguration();
    config.setSignerOverride("S3SignerType");
    AmazonS3Client s3 = new AmazonS3Client(config);
    s3.setEndpoint("https://your-endpoint");

Clojure
```````

Install `AWS SDK for Java`_, then:

.. code-block:: clojure

  (ns sos.core
  (:import com.amazonaws.ClientConfiguration
           com.amazonaws.services.s3.AmazonS3Client
           java.util.Date))

  (defn test-s3
    []
    (let [opts   (doto (ClientConfiguration.)
                   (.setSignerOverride "S3SignerType"))
          client (doto (AmazonS3Client. opts)
                   (.setEndpoint "https://your-endpoint"))]
      (.generatePresignedUrl client "batman" "foo.txt" (Date.))))


.. _awscli: https://aws.amazon.com/cli/
.. _boto3: https://boto3.readthedocs.io/en/latest/
.. _Boto: http://boto.cloudhackers.com/en/latest/
.. _AWSSDK.S3: https://www.nuget.org/packages/AWSSDK.S3/
.. _AWS SDK for Java: https://aws.amazon.com/sdk-for-java/

