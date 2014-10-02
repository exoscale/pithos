(ns io.pithos.perms
  (:require [io.pithos.bucket      :as bucket]
            [io.pithos.system      :as system]
            [io.pithos.desc        :as desc]
            [clojure.tools.logging :refer [debug]]))

(defmacro ensure!
  "Assert that a predicate is true, abort with access denied if not"
  [pred]
  `(when-not ~pred
     (debug "could not ensure: " (str (quote ~pred)))
     (throw (ex-info "access denied" {:status-code 403
                                      :type        :access-denied}))))

(defn granted-for?
  "Do current permission allow for operation on this particular perm ?"
  [acl for needs]
  (loop [[{:keys [URI DisplayName ID] :as id} & ids] (get acl needs)]
    (when id
      (or (= URI for) (= ID for) (recur ids)))))

(defn granted?
  "Do current permissions allow for operation ?"
  [acl needs for]
  (some identity (map (partial granted-for? acl for) needs)))

(defn bucket-satisfies?
  "Ensure sufficient rights for bucket access"
  [{:keys [tenant acl]} {:keys [for groups needs]}]
  (let [needs [:FULL_CONTROL needs]
        acl   (if acl (read-string acl))]
    (or (= tenant for)
        (granted? acl needs for)
        (some identity (map (partial granted? acl needs) groups)))))

(defn object-satisfies?
  "Ensure sufficient rights for object accessp"
  [{tenant :tenant} {acl :acl} {:keys [for groups needs]}]
  (let [needs [:FULL_CONTROL needs]
        acl   (if acl (read-string acl))]
    (or (= tenant for)
        (granted? acl needs for)
        (some identity (map (partial granted? acl needs) groups)))))

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
                                 (desc/object-descriptor system bucket object)
                                 {:for    tenant
                                  :groups memberof?
                                  :needs  arg}))))
    true))


(defn initialize-object
  [bd tenant headers]
  (let [canned-acl (get headers "x-amz-acl")]
    (cond
     canned-acl
     (let [init {:FULL_CONTROL [{:ID tenant}]}]
       (pr-str
        (case canned-acl
          "public-read-write"  (merge init {:READ  [{:URI "anonymous"}]
                                            :WRITE [{:URI "anonymous"}]})
          "public-read"        (merge init {:READ [{:URI "anonymous"}]})
          "authenticated-read" (merge init {:READ [{:URI "authenticated"}]})
          "log-delivery-write" init
          "private"            init
          nil                  init
          (throw (ex-info "Invalid Argument"
                          {:arg "x-amz-acl"
                           :val canned-acl
                           :status-code 400
                           :type :invalid-argument})))))))

)
