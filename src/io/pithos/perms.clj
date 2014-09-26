(ns io.pithos.perms
  (:require [io.pithos.bucket      :as bucket]
            [io.pithos.system      :as system]
            [clojure.tools.logging :refer [debug]]))

(defmacro ensure!
  "Assert that a predicate is true, abort with access denied if not"
  [pred]
  `(when-not ~pred
     (debug "could not ensure: " (str (quote ~pred)))
     (throw (ex-info "access denied" {:status-code 403
                                      :type        :access-denied}))))

(defn granted?
  "Do current permissions allow for operation ?"
  [acl needs for]
  (= (get acl for) needs))

(defn bucket-satisfies?
  "Ensure sufficient rights for bucket access"
  [{:keys [tenant acl]} {:keys [for groups needs]}]
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn object-satisfies?
  "Ensure sufficient rights for object accessp"
  [{tenant :tenant} {acl :acl} {:keys [for groups needs]}]
  (or (= tenant for)
      (granted? acl needs for)
      (some identity (map (partial granted? acl needs) groups))))

(defn authorize
  "Check permission to service operation, each operation has a list
   of needed permissions, any failure results in an exception being raised
   which prevents any further action from being taken."
  [{:keys [authorization bucket object]} perms system]
  (let [{:keys [tenant memberof]} authorization
        memberof?                 (set memberof)
        bucketstore               (system/bucketstore system)]
    (doseq [[perm arg] (map (comp flatten vector) perms)]
      (case perm
        :authenticated (ensure! (not= tenant :anonymous))
        :memberof      (ensure! (memberof? arg))
        :bucket        (ensure! (bucket-satisfies?
                                 (bucket/by-name bucketstore bucket)
                                 {:for    tenant
                                  :groups memberof?
                                  :needs  arg}))
        :object        (ensure! (object-satisfies?
                                 (bucket/by-name bucketstore bucket)
                                 nil ;; XXX please fix me
                                 {:for    tenant
                                  :groups memberof?
                                  :needs  arg}))))
    true))
