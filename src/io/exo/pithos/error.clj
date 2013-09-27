(ns io.exo.pithos.error
  (:require [clojure.string :refer [split join capitalize upper-case]]))

(defn format-code
  [code]
  (let [special     {::invalid-uri "InvalidURI"
                     ::malformed-post-request "MalformedPOSTRequest"
                     ::malformed-xml "MalformedXML"}]
    (or (special code)
        (->> (split (name code) #"-")
             (map capitalize)
             (join)))))

(def codes
  {::access-denied 
   {:message "Access Denied"
    :status 403}
   ::account-problem 
   {:message "There is a problem with your exoscale account."
    :status 403 ;; forbidden
    }
   ::ambiguous-grant-by-email-address
   {:message "The e-mail address you provided is associated with more than one account"
    :status 400 ;; bad-request
    }
   ::bad-digest
   {:message "The Content-MD5 you specified did not match what we received"
    :status 400}
   ::bucket-already-exists
   {:message "The requested bucket name is not available. The bucket namespace is shared by all users of the system"
    :status 409 ;; conflict
    }
   ::bucket-already-owned-by-you
   {:message "Your previous request to create the named bucket succeeded and you already own it."
    :status 409}
   ::bucket-not-empty
   {:message "The bucket you tried to delete is not empty."
    :status 409}
   ::credentials-not-supported
   {:message "This request does not support credentials."
    :status 400}
   ::cross-location-logging-prohibited
   {:message "Cross location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location."
    :status 403}
   ::entity-too-small
   {:message "Your proposed upload is smaller than the minimum allowed object size."
    :status 400}
   ::entity-too-large
   {:message "Your proposed upload exceeds the maximum allowed object size."
    :status 400}
   ::expired-token
   {:message "The provided token has expired"
    :status 400}
   ::illegal-versioning-configuration-exception
   {:message "Indicates that the Versioning configuration specified in the request is invalid."
    :status 400}
   ::incomplete-body
   {:message "You did not provide the number of bytes specified by the Content-Length HTTP header"
    :status 400}
   ::incorrect-number-of-files-in-post-request
   {:message "POST requires exactly one file upload per request."
    :status 400}
   ::inline-data-too-large
   {:message "Inline data exceeds the maximum allowed size."
    :status 400}
   ::internal-error
   {:message "We encountered an internal error. Please try again."
    :status 500}
   ::invalid-access-key-id
   {:message "The AWS Access Key Id you provided does not exist in our records."
    :status 403}
   ::invalid-argument
   {:message "Invalid Argument"
    :status 400}
   ::invalid-bucket-name
   {:message "The specified bucket is not valid"
    :status 400}
   ::invalid-bucket-state
   {:message "The request is not valid with the current state of the bucket"
    :status 409}
   ::invalid-digest
   {:message "The Content-MD5 you specified was invalid."
    :status 400}
   ::invalid-location-constraint
   {:message "The specified location constraint is not valid."
    :status 400}
   ::invalid-object-state
   {:message "The operation is not valid for the current state of the object"
    :status 400}
   ::invalid-part
   {:message "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag."
    :status 400}
   ::invalid-part-order
   {:message "The list of parts was not in ascending order.Parts list must specified in order by part number."
    :status 400}
   ::invalid-payer
   {:message "All access to this object has been disabled."
    :status 403}
   ::invalid-policy-document
   {:message "The content of the form does not meet the conditions specified in the policy document."
    :status 400}
   ::invalid-range
   {:message "The requested range cannot be satisfied."
    :status 416 ;; request range not satisfiable
    }
   ::invalid-storage-class
   {:message "The storage class you specified is not valid."
    :status 400}
   ::invalid-target-bucket-for-logging
   {:message "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group."
    :status 400}
   ::invalid-token
   {:message "The provided token is malformed or otherwise invalid."
    :status 400}
   ::invalid-uri
   {:message "Couldn't parse the specified URI."
    :status 400}
   ::key-too-long
   {:message "Your key is too long"
    :code 400}})
