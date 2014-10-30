Developer Guide
===============

*pithos* is an open source project, available on github_:
https://github.com/exoscale/pithos.

*pithos* is developed in clojure_, a functional lisp which
runs on the **JVM**.

Building Pithos from source
---------------------------

*pithos* is built with leiningen_, to build the
standard version of *pithos* just run::

  lein test
  lein compile :all
  lein uberjar

If you wish to quickly test versions as you develop,
you can run pithos directly from leiningen. You
should place your test configuration file in
the ``site/`` subdirectory::

  lein run -- -f site/pithos.yaml

Contributing to pithos
----------------------

Contributions to *pithos* are heavily encouraged.
The best way to contribute is to work on a separate
git branch, branching off of the master branch::

  git pull origin/master
  git checkout -B feature/new-feature

Once work is ready, use the github pull-request
mechanism for a code review to happen.

.. _Custom Stores:

Creating alternative store implementations
------------------------------------------

While pithos primarly targets Apache Cassandra,
nothing prevents alternative implementation to be
created for all parts of the service.

*pithos*, through a simple dependency injection
mechanism allows custom implementations of stores
to be plugged.

In clojure parlance, the only requirement an implementation
must fulfill is to realize the correct protocol.

Here is a summary of all current protocols:

Keystore
~~~~~~~~

The keystore protocol contains a single-method: ``fetch``,
which given a key ID should return a map containing the
keys:

``fetch``
  - ``tenant``: the tenant this key belongs to
  - ``secret``: the associated secret key
  - ``memberof``: (*optional*) groups this tenant belongs to

.. sourcecode:: clojure

  (defprotocol Keystore
    (fetch [this id]))

Bucketstore
~~~~~~~~~~~

The bucketstore exposes methods to handle buckets:

``converge!``
  Optional method to converge the schema during the ``install-schema`` phase.

``by-tenant``
  Retrieves a list of bucket by tenant

``by-name``
  Retrieves a bucket by name

``create!``
  Create a bucket

``update!``
  Update a bucket

``delete!``
  Destroys a bucket

.. sourcecode:: clojure

  (defprotocol Bucketstore
    "The bucketstore contains the schema migration function,
     two bucket lookup functions and CRUD signatures"
    (converge! [this])
    (by-tenant [this tenant])
    (by-name [this bucket])
    (create! [this tenant bucket columns])
    (update! [this bucket columns])
    (delete! [this bucket]))

Metastore
~~~~~~~~~

The metastore exposes methods to handle bucket metadata:


``converge!``
  Optional method to converge the schema during the ``install-schema`` phase.

``fetch``
  Retrieves an object's metdata

``prefixes``
  Lists objects

``update!``
  Updates an object's properties

``delete!``
  Destroys an object

``abort-multipart-upload!``
  Aborts a multipart upload

``update-part!``
  Updates a multipart upload's part metadata

``initiate-upload!``
  Stores metadata for a multipart upload

``get-upload-details``
  Retrieves metadata on an ongoing upload

``list-uploads``
  Lists all uploads for a bucket

``list-object-uploads``
  Lists all uploads for an object

``list-upload-parts``
  Lists registered upload parts for an upload.

.. sourcecode:: clojure

  (defprotocol Metastore
    "All necessary functions to manipulate bucket metadata"
    (converge! [this])
    (fetch [this bucket object] [this bucket object fail?])
    (prefixes [this bucket params])
    (update! [this bucket object columns])
    (delete! [this bucket object])
    (abort-multipart-upload! [this bucket object upload])
    (update-part! [this bucket object upload partno columns])
    (initiate-upload! [this bucket object upload metadata])
    (get-upload-details [this bucket object upload])
    (list-uploads [this bucket])
    (list-object-uploads [this bucket object])
    (list-upload-parts [this bucket object upload]))


Blobstore
~~~~~~~~~

The blobstore expores methods to store and retrieve data:

``converge!``
  Optional method to converge the schema during the ``install-schema`` phase.

``delete!``
  Destroys an inode

``blocks``
  Retrieves blocks from an object descriptor

``max-chunk``
  Maximum chunk-size for this blobstore

``chunks``
  Retrieve chunks from a starting offset

``start-block!``
  Mark the start of a block

``chunk!``
  Store a chunk

``boundary?``
  Check if a block boundary has been reached

.. sourcecode:: clojure

  (defprotocol Blobstore
    "The blobstore protocol, provides methods to read and write data
     to inodes, as well as a schema migration function.
     "
    (converge! [this])
    (delete! [this inode version])
    (blocks [this od])
    (max-chunk [this])
    (chunks [this od block offset])
    (start-block! [this od block offset])
    (chunk! [this od block offset chunk])
    (boundary? [this block offset]))


An alternative keystore
~~~~~~~~~~~~~~~~~~~~~~~

The simplest example would be to create an alternative keystore.
Let's pretend a simple, non-authenticated API is used to provide
credential results.

.. sourcecode:: clojure

  (ns com.example.http-keystore
    (:require [qbits.jet.client.http :as http]
              [io.pithos.keystore    :as ks]
              [clojure.core.async    :refer [<!!]]))

  (defn http-keystore
    [{:keys [base-url]}]
    (let [client (http/client)]
      (reify ks/Keystore
        (fetch [this key]
          (let [url  (str base-url "/" key)
                opts {:as :json}
                resp (<!! (http/get client url opts))]
            (when (= (:status resp) 200)
              (<!! (:body resp))))))))

.. _Alternative Reporter:

An alternative reporter
~~~~~~~~~~~~~~~~~~~~~~~

Likewise, creating alternative reporters is trivial, here
is a sample cassandra reporter:

.. sourcecode:: clojure

  (defn cassandra-reporter
    [config]
    (let [session (store/cassandra-store config)]
      (reify Reporter
        (report! [_ event]
          (execute session
                   (update :events
                           (colums (assoc event :id (UUID/randomUUID)))))))))                
  
.. _github: https://github.com
.. _clojure: http://clojure.org
.. _leiningen: http://leiningen.org
