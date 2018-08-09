S3 API
======

This section describe the subset of the S3 API which is
covered by pithos.

The original S3 API documentation can be found at: 
http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html

Soap vs. Rest
-------------

*pithos* as it stands only implements the S3 REST facade,
not the SOAP facade to the API. It should be noted that
there are no known S3 clients requiring SOAP functionality,
there are also no other alternative S3 compatible daemons
providing the SOAP API.

API Feature matrix
------------------

This simple matrix gives an overview of the API coverage
provided by pithos.

.. list-table:: Feature Matrix
   :header-rows: 1
 
   * - Feature
     - Status
     - Notes
   * - Authorization
     - OK
     - Identity management is pluggable.
   * - Storage classes
     - OK
     - Configurable per region.
   * - Bucket listing
     - OK
     - 
   * - Bucket Creation
     - OK
     - 
   * - Bucket Deletion
     - OK
     - 
   * - Bucket Info
     - OK
     - 
   * - Bucket ACLs
     - OK
     - 
   * - Bucket CORS
     - OK
     - 
   * - Bucket Lifecycle
     - OK
     - No actual action performed.
   * - Bucket Policy
     - OK
     - No actual action performed
   * - Object creation
     - OK
     - 
   * - Object deletion
     - OK
     - 
   * - Object multipart uploads
     - OK
     - 
   * - Object ACLs
     - OK
     - Canned ACLs, Header ACLs, Tenant and Group ACLs.
   * - Bucket Versioning
     - Unfinished
     - 
   * - Object Torrent
     - Not implemented
     - 
   * - Bucket Logging
     - Not implemented
     - 
   * - Bucket Website
     - Not implemented
     - 
   * - Bucket Requester Pays
     - Not implemented
     - 

Common Headers
--------------

``Authorization``
  When provided, signs the request. The expected format is::

    Authorization: AWS ACCESS_KEY:SIGNATURE

  See :ref:`Request Signature` for details on how to generate the signature.

  .. note::

    *pithos* currently supports version 2 of the request signature method.

  When the header isn't provided, the request will be treated as anonymous.

``Date`` and ``x-amz-date``
  Specifies the date at which the date was present, when ``x-amz-date`` is
  present it takes precedence over ``Date``, this allows to bypass the
  automatic setting of ``Date`` in some HTTP client libraries.

``Host``
  Used to specify the bucket

Operations Overview
-------------------

.. list-table:: Feature Matrix
   :header-rows: 1
   :widths: 5 50

   * - Operation
     - Action
   * - `GET Service`_
     - Lists bucket
   * - `PUT Bucket`_
     - Create bucket
   * - `DELETE Bucket`_
     - Destroy bucket
   * - `GET Bucket`_
     - Lists objects in bucket
   * - `HEAD Bucket`_
     - Bucket information
   * - `GET Bucket CORS`_
     - Bucket CORS configuration
   * - `PUT Bucket CORS`_
     - Update bucket CORS configuration
   * - `DELETE Bucket CORS`_
     - Destroy bucket CORS configuration
   * - `GET Bucket ACL`_
     - Bucket ACL configuration
   * - `PUT Bucket ACL`_
     - Update bucket ACL configuration
   * - `DELETE Bucket ACL`_
     - Destroy bucket ACL configuration
   * - `GET Bucket versioning`_
     - Bucket versioning configuration
   * - `PUT Bucket versioning`_
     - Update versioning configuration
   * - `GET Bucket lifecycle`_
     - Bucket lifecycle configuration
   * - `PUT Bucket lifecycle`_
     - Update bucket lifecycle configuration
   * - `DELETE Bucket lifecycle`_
     - Destroy bucket lifecycle configuration
   * - `GET Bucket location`_
     - Retrieve bucket location
   * - `GET Bucket policy`_
     - Retrieve bucket policy
   * - `GET Bucket uploads`_
     - List multipart uploads
   * - `GET Object`_
     - Retrieve object
   * - `HEAD Object`_
     - Retrieve object info
   * - `PUT Object`_
     - Upload object
   * - `DELETE Object`_
     - Destroy object
   * - `GET Object ACL`_
     - Retrieve object ACL
   * - `PUT Object ACL`_
     - Update object ACL
   * - `POST Object uploads`_
     - Initiate multipart upload
   * - `PUT Object upload`_
     - Upload multipart upload part
   * - `DELETE Object upload`_
     - Abort multipart upload
   * - `POST Object upload`_
     - Complete multipart upload


.. _GET Service:

GET Service
-----------

Returns a list of buckets for a specific tenant.
The request **must** be authenticated.
This request accepts **no** parameter.

Sample Request::

  GET / HTTP/1.1
  Host: service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response

.. sourcecode:: xml

  <?xml version="1.0" encoding="UTF-8"?>
  <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
      <ID>test@example.com</ID>
      <DisplayName>test@example.com</DisplayName>
    </Owner>
    <Buckets>
      <Bucket>
        <Name>my-bucket</Name>
        <CreationDate>2014-01-01T00:00:00.000Z</CreationDate>
      </Bucket>
    </Buckets>
  </ListAllMyBucketsResult>
  
.. _GET Bucket:

GET Bucket
----------

Lists objects in buckets. Internally, no hierarchy is maintained between objects.
Their metadata is only sorted lexicographically. The API provides a way to emulate
a hierachy through the specification of a `prefix`.

Request parameters:
  - ``prefix``: When present, will only return objects which are prefixed with the supplied string.
  - ``delimiter``: When present, will split entries according to the supplied string. Entries with no
    delimiter present will appear as `Contents` while entries containing the delimiter will be grouped
    and returned in the `CommonPrefixes` part of the reply.
  - ``max-keys``: The maximum number of keys to return.
  - ``marker``: When present, use the provided marker to access paged results.

Sample Request::

  GET /?delimiter=/ HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response:

.. sourcecode:: xml

  <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>batman</Name>
      <Prefix></Prefix>
      <MaxKeys>100</MaxKeys>
      <Delimiter>/</Delimiter>
      <IsTruncated>false</IsTruncated>
      <Contents>
        <Key>sample.txt</Key>
        <LastModified>2014-10-17T12:35:10.423Z</LastModified>
        <ETag>"a4b7923f7b2df9bc96fb263978c8bc40"</ETag>
        <Size>1603</Size>
        <Owner>
          <ID>test@example.com</ID>
          <DisplayName>test@example.com</DisplayName>
        </Owner>
        <StorageClass>Standard</StorageClass>
     </Contents>
  </ListBucketResult>

.. _HEAD Bucket:

HEAD Bucket
-----------

Determine whether a bucket exists and you have permission to access it.

Sample Request::

  HEAD / HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  HTTP/1.1 200 OK

.. _PUT Bucket:

PUT Bucket
----------

Create a new bucket. This request may use the common ACL headers (cf `ACL headers`_).
The request **must** be authenticated.
This request accepts **no** parameters.

Sample Request::

  PUT / HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

   HTTP/1.1 200 OK

.. _DELETE Bucket:

DELETE Bucket
-------------

Destroys a bucket.
This request accepts **no** parameters.

Sample Request::

  PUT / HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  HTTP/1.1 204 No Content

.. _GET Bucket CORS:

GET Bucket CORS
---------------

Retrieves the stored CORS profile for the bucket.
A bucket's CORS profile determines how pithos will treat
*OPTIONS* request made to both buckets and objects.

Sample Request::

  GET /?cors HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response:

.. sourcecode:: xml

  <CORSConfiguration>
     <CORSRule>
       <AllowedOrigin>http://client.example.com</AllowedOrigin>
       <AllowedMethod>GET</AllowedMethod>
       <MaxAgeSeconds>3000</MaxAgeSec>
     </CORSRule>
  </CORSConfiguration>  

The detailed format of the CORSConfiguration payload is described
in `PUT Bucket CORS`_.

.. _PUT Bucket CORS:

PUT Bucket CORS
---------------

Updates the stored CORS profile for the bucket. If a previous
CORS profile existed it will be replaced with the provided one.

Sample Request::

  PUT /?cors HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

  <CORSConfiguration>
     <CORSRule>
       <AllowedOrigin>http://client.example.com</AllowedOrigin>
       <AllowedMethod>GET</AllowedMethod>
       <MaxAgeSeconds>3000</MaxAgeSec>
     </CORSRule>
  </CORSConfiguration>  


Sample Response::

   HTTP/1.1 200 OK

.. _DELETE Bucket CORS:

DELETE Bucket CORS
------------------

Remove the stored CORS profile for the bucket.

Sample Request::

  DELETE /?cors HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  HTTP/1.1 204 No Content

.. _GET Bucket ACL:

GET Bucket ACL
--------------

Retrieve a bucket's ACL.

Sample Request::

  GET /?acl HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response

.. sourcecode:: xml

  <AccessControlPolicy>
    <Owner>
      <ID>test@example.com</ID>
      <DisplayName>test@example.com</DisplayName>
    </Owner>
    <AccessControlList>
      <Grant>
        <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			           xsi:type="CanonicalUser">
          <ID>test@example.com</ID>
          <DisplayName>test@example.com</DisplayName>
        </Grantee>
        <Permission>FULL_CONTROL</Permission>
      </Grant>
    </AccessControlList>
  </AccessControlPolicy> 

.. _PUT Bucket ACL:

PUT Bucket ACL
--------------

Update a bucket's ACL. ACLs may be provided in one of three ways.

- As a canned ACL in HTTP headers
- As a simple ACL in HTTP headers
- Using an XML body

ACLs are treated in this order of priority.
Please refer to `ACL Headers`_ for a complete description
of Canned and Simple Header ACLs, which may also be used
when initiating multipart uploads or uploading objects.

Sample Request::

  PUT /?acl HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

  <AccessControlPolicy>
    <Owner>
      <ID>test@example.com</ID>
      <DisplayName>test@example.com</DisplayName>
    </Owner>
    <AccessControlList>
      <Grant>
        <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			           xsi:type="CanonicalUser">
          <ID>test@example.com</ID>
          <DisplayName>test@example.com</DisplayName>
        </Grantee>
        <Permission>FULL_CONTROL</Permission>
      </Grant>
    </AccessControlList>
  </AccessControlPolicy> 


Sample Response::

  HTTP/1.1 200 OK

.. _DELETE Bucket ACL:

DELETE Bucket ACL
-----------------

Deletes the ACL for a bucket, will revert to an ACL yielding
full control to the bucket owner.

Sample Request::

  DELETE /?acl HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  HTTP/1.1 204 No Content

.. _GET Bucket lifecycle:

GET Bucket lifecycle
--------------------

Mock API call, provided for compatibility with some clients which 
always returns a 404 Response, with the **NoSuchLifecycleConfiguration**
message.

.. _PUT Bucket lifecycle:

PUT Bucket lifecycle
--------------------

Mock API call, provided for compatibility with some clients which
always yields a 200 response but does not take any action.

.. _DELETE Bucket lifecycle:

DELETE Bucket lifecycle
-----------------------

Mock API call, provided for compatibility with some clients which
always yields a 204 response but does not take any action.

.. _PUT Bucket versioning:

PUT Bucket versioning
---------------------

Mock API call, provided for compatibility with some clients which
always yields a 200 response but does not take any action.

.. _GET Bucket versioning:

GET Bucket versioning
---------------------

Mock API call, provided for compatibility with some clients which
always reports a bucket as unversioned.

.. _GET Bucket location:

GET Bucket location
-------------------

Retrieves the region a bucket is stored in.

Sample Request::

  GET /?location HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response:

.. sourcecode:: xml

  <LocationConstraint>myregion</LocationConstraint>  

.. _GET Bucket policy:

GET Bucket policy
-----------------

Mock API call, provided for compatibility with some clients which
always reports a bucket's policy as inexistent.

.. _GET Bucket uploads:

GET Bucket uploads
------------------

Lists multipart uploads for a bucket. This call accepts the same
arguments than `GET Bucket`_ does, to list uploads based on prefixes
if necessary.

Request parameters:
  - ``prefix``: When present, will only return objects which are prefixed with the supplied string.
  - ``delimiter``: When present, will split entries according to the supplied string. Entries with no
    delimiter present will appear as `Contents` while entries containing the delimiter will be grouped
    and returned in the `CommonPrefixes` part of the reply.
  - ``max-uploads``: The maximum number of uploades to return.
  - ``marker``: When present, use the provided marker to access paged results.

POST Bucket delete
------------------

Provide a list of objects to delete from a bucket.
The list is given as an XML payload.

Sample Request::

  POST /?delete HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Request Body:

.. sourcecode:: xml

<?xml version="1.0" encoding="UTF-8"?>
<Delete>
    <Object>
         <Key>Key1</Key>
    </Object>
    <Object>
         <Key>Key2</Key>
    </Object>
</Delete>	                


.. _GET Object:

GET Object
----------

Retrieves an object's content.

Sample Request::

  GET /myfile HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  Content-Type: text/plain
  Content-Length: 5

  foo\r\n

.. _HEAD Object:

HEAD Object
-----------

Asserts that an object exists and that permissions to retrieve it are met and returns
metadata.

Sample Request::

  HEAD /myfile HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  Content-Type: text/plain
  Content-Length: 5

.. _PUT Object:

PUT Object
----------

Uploads an object.

Sample Request::

  PUT /myfile HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>
  Content-MD5: <md5-checksum>
  Content-Length: 5
  Expect: 100-continue

  foo\r\n

Sample Response::

  HTTP/1.1 100 Continue

  HTTP/1.1 200 Ok

Objects may be uploaded with header ACLs, as described
in `ACL Headers`_

.. _DELETE Object:

DELETE Object
--------------

Destroys an object.

Sample Request::

  DELETE /myfile HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>
  
Sample Response::

  HTTP/1.1 204 No Content

.. _GET Object ACL:

GET Object ACL
--------------

Retrieves the ACL of an object.

Sample Request::

  GET /myfile?acl HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>


Sample Response

.. sourcecode:: xml

  <AccessControlPolicy>
    <Owner>
      <ID>test@example.com</ID>
      <DisplayName>test@example.com</DisplayName>
    </Owner>
    <AccessControlList>
      <Grant>
        <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			           xsi:type="CanonicalUser">
          <ID>test@example.com</ID>
          <DisplayName>test@example.com</DisplayName>
        </Grantee>
        <Permission>FULL_CONTROL</Permission>
      </Grant>
    </AccessControlList>
  </AccessControlPolicy> 

.. _PUT Object ACL:

PUT Object ACL
--------------

Update an object's ACL. ACLs may be provided in one of three ways.

- As a canned ACL in HTTP headers
- As a simple ACL in HTTP headers
- Using an XML body

ACLs are treated in this order of priority.
Please refer to `ACL Headers`_ for a complete description
of Canned and Simple Header ACLs, which may also be used
when initiating multipart uploads or uploading objects.

Sample Request::

  PUT /myfile?acl HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

  <AccessControlPolicy>
    <Owner>
      <ID>test@example.com</ID>
      <DisplayName>test@example.com</DisplayName>
    </Owner>
    <AccessControlList>
      <Grant>
        <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			           xsi:type="CanonicalUser">
          <ID>test@example.com</ID>
          <DisplayName>test@example.com</DisplayName>
        </Grantee>
        <Permission>FULL_CONTROL</Permission>
      </Grant>
    </AccessControlList>
  </AccessControlPolicy> 


Sample Response::

  HTTP/1.1 200 OK


.. _POST Object uploads:

POST Object uploads
-------------------

Most big payloads in pithos should be uploaded through the
multipart upload mechanism.

Multipart upload provide you with a way to upload (potentially
simultaneously) chunks of the object you wish to create and
then to promote these parts into a single object.

This request initiates a multipart upload, and yields a new
upload ID.

Sample Request::

  POST /myfile?uploads HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response:

.. sourcecode:: xml

  <?xml version="1.0" encoding="UTF-8"?>
  <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>mybucket</Bucket>
    <Key>myfile</Key>
    <UploadId>1d4bfa70-26f6-4819-97dd-275bf040f03f</UploadId>
  </InitiateMultipartUploadResult>

The ID may then be used in subsequent calls to `PUT Object upload`_, `DELETE Object upload`_ 
and `POST Object upload`_.

Additionally, header ACLs may be supplied to determine the access control for the
resulting object. Pleaser refer to `ACL Headers`_ for a full description of possible
headers.

.. _PUT Object upload:

PUT Object upload
-----------------

Add a part to an ongoing upload. This request works in a similar fashion than
the `PUT Object`_ call.

Sample Request::

  PUT /myfile?uploadId=1d4bfa70-26f6-4819-97dd-275bf040f03f&partNumber=1 HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>
  Content-MD5: <md5-checksum>
  Content-Length: 5
  Expect: 100-continue

  foo\r\n

Sample Response::

  HTTP/1.1 100 Continue

  HTTP/1.1 200 Ok


.. _DELETE Object upload:

DELETE Object upload
--------------------

Aborts an ongoing multipart upload.

Sample Request::

  DELETE /myfile?uploadId=1d4bfa70-26f6-4819-97dd-275bf040f03f HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response::

  HTTP/1.1 204 No Content



.. _POST Object upload:

POST Object upload
------------------

Completes a multipart upload. Assembling all parts in a single object.
At the moment, *pithos* will stream all parts, regardless of the provided
request input.

This process may take a while to finish and will thus emit whitespace at regular
intervals to make sure the connection stays up.

Sample Request::

  POST /myfile?uploadId=1d4bfa70-26f6-4819-97dd-275bf040f03f HTTP/1.1
  Host: mybucket.service.uri
  Date: <date>
  Authorization: AWS <key>:<signature>

Sample Response:

.. sourcecode:: xml

  <CompleteMultipartUpload>
    <Part>
      <PartNumber>1</PartNumber>
      <ETag>"acbd18db4cc2f85cedef654fccc4a4d8"</ETag>
    </Part>
    <Part>
      <PartNumber>2</PartNumber>
      <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
    </Part>
  </CompleteMultipartUpload>  

Specifying ACLs
---------------

.. _ACL Headers:

ACL Headers
~~~~~~~~~~~

.. _Request Signature:

Request Signature
-----------------


