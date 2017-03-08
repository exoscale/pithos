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

Convergeable
~~~~~~~~~~~~

The convergeable protocol is used to create the
initial schema for databases that need it. It
consists of a single method:

.. sourcecode:: clojure

  (defprotocol Convergeable
    (converge! [this]))

This method is called on blobstores, metastores and bucketstores
during the ``install-schema`` phase.

Crudable
~~~~~~~~

The metastore, blobstore and bucketstores share a few functions
which are gathered in this protocol:

``fetch``
  Retrieve metadata from buckets or objects (unused in blobstores)

``update!``
  Updates an object's or bucket's metadata (unused in blobstores)

``create!``
  Insert a bucket (unused in metastores and blobstores)

``delete!``
  Delete an object, bucket or blob

.. sourcecode:: clojure

  (defprotocol Crudable
    (fetch [this k] [this k1 k2] [this k1 k2 k3])
    (update! [this k v] [this k1 k2 v] [this k1 k2 k3 v])
    (delete! [this k] [this k1 k2] [this k1 k2 k3])
    (create! [this k v] [this k1 k2 v] [this k1 k2 k3 v]))
                  

clojure.lang.ILookup
~~~~~~~~~~~~~~~~~~~~
While not a *pithos* protocol per-se, this protocol
is used by keystores to behave like standard clojure
maps. The method used within ``ILookup`` is ``valAt``,
the expected output is a map containing the keys:

  - ``tenant``: the tenant this key belongs to
  - ``secret``: the associated secret key
  - ``memberof``: (*optional*) groups this tenant belongs to

Bucketstore
~~~~~~~~~~~

The bucketstore exposes methods to handle buckets:

``by-tenant``
  Retrieves a list of bucket by tenant

``by-name``
  Retrieves a bucket by name

.. sourcecode:: clojure

  (defprotocol Bucketstore
    "The bucketstore contains the schema migration function,
     two bucket lookup functions and CRUD signatures"
    (by-tenant [this tenant])
    (by-name [this bucket]))


Metastore
~~~~~~~~~

The metastore exposes methods to handle bucket metadata:

``prefixes``
  Lists objects

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
    (prefixes [this bucket params])
    (abort-multipart-upload! [this bucket object upload])
    (update-part! [this bucket object upload partno columns])
    (initiate-upload! [this bucket object upload metadata])
    (get-upload-details [this bucket object upload])
    (list-uploads [this bucket prefix])
    (list-object-uploads [this bucket object])
    (list-upload-parts [this bucket object upload]))


Blobstore
~~~~~~~~~

The blobstore expores methods to store and retrieve data:

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
    (blocks [this od])
    (max-chunk [this])
    (chunks [this od block offset])
    (start-block! [this od block offset])
    (chunk! [this od block offset chunk])
    (boundary? [this block offset]))

Reporter
~~~~~~~~

The reporter protocol exposes a single method used to register
an event.

``report!``
  This method hands off an event to the current reporter.


.. sourcecode:: clojure

  (defprotocol Reporter
    (report! [this event]))


An alternative keystore
~~~~~~~~~~~~~~~~~~~~~~~

The simplest example would be to create an alternative keystore.
Let's pretend a simple, non-authenticated API is used to provide
credential results.

.. sourcecode:: clojure

  (ns com.example.http-keystore
    (:require [qbits.jet.client.http :as http]
              [clojure.core.async    :refer [<!!]]))

  (defn http-keystore
    [{:keys [base-url]}]
    (let [client (http/client)]
      (reify clojure.lang.ILookup
        (valAt [this key]
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
