(ns io.pithos.perms
  (:require [io.pithos.bucket      :as bucket]
            [io.pithos.system      :as system]
            [io.pithos.desc        :as desc]
            [io.pithos.acl         :as acl]
            [clojure.string        :refer [split]]
            [clojure.tools.logging :refer [debug]]))

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
        :authenticated (when-not (not= tenant :anonymous)
                         (debug "unauthenticated request to private resource")
                         (throw (ex-info "access denied" {:status-code 403
                                                          :type        :access-denied})))
        :memberof      (when-not (memberof? arg)
                         (debug "not a member of: " arg "groups:" (pr-str memberof?))
                         (throw (ex-info "access denied" {:status-code 403
                                                          :type :access-denied})))
        :bucket        (let [bd (bucket/by-name bucketstore bucket)]
                         (when-not bd
                           (throw (ex-info "bucket not found"
                                           {:type        :no-such-bucket
                                            :status-code 404
                                            :bucket      bucket})))
                         (when-not (bucket-satisfies? bd {:for    tenant
                                                          :groups memberof?
                                                          :needs  arg})
                           (debug "unsatisfied ACL for bucket. candidate:" (pr-str tenant)
                                  "groups:" (pr-str memberof?)
                                  "needs:"  arg
                                  "acl:"    (:acl bd)
                                  "bucket-owner:" (:tenant bd))
                           (throw (ex-info "access denied" {:status-code 403
                                                            :type :access-denied}))))
        :object        (when-not (object-satisfies?
                                  (bucket/by-name bucketstore bucket)
                                  (desc/object-descriptor system bucket object)
                                  {:for    tenant
                                   :groups memberof?
                                   :needs  arg})
                         (debug "unsatisfied ACL for object. candidate:" (pr-str tenant)
                                "groups:" (pr-str memberof?)
                                "needs:"  arg)
                         (throw (ex-info "access denied" {:status-code 403
                                                          :type :access-denied})))))
    true))

(defn ->grantee
  [str]
  (debug "translating: " str)
  (let [[_ type dest] (or (re-find #"(emailAddress|id|uri)=\"(.*)\"" str)
                          (re-find #"(emailAddress|id|uri)=(.*)" str)
                          (throw (ex-info "Invalid Argument"
                                          {:type :invalid-argument
                                           :status-code 400
                                           :arg "x-amz-acl-*"
                                           :val str})))]
    (cond (#{"id" "emailAddress"} type) {:ID dest :DisplayName dest}
          :else                         {:URI (or (acl/known-uris dest)
                                                  dest)})))

(defn has-header-acl?
  [headers]
  (or (get headers "x-amz-acl")
      (get headers "x-amz-grant-read")
      (get headers "x-amz-grant-read-acp")
      (get headers "x-amz-grant-write")
      (get headers "x-amz-grant-write-acp")
      (get headers "x-amz-grant-full-control")))

(defn header-acl
  [owner tenant headers]
  (let [init          {:FULL_CONTROL [{:ID owner
                                       :DisplayName owner}]}
        canned-acl    (get headers "x-amz-acl")
        acl-read      (some-> (get headers "x-amz-grant-read")
                              (split #","))
        acl-write     (some-> (get headers "x-amz-grant-write")
                              (split #","))
        acl-read-acp  (some-> (get headers "x-amz-grant-read-acp")
                              (split #","))
        acl-write-acp (some-> (get headers "x-amz-grant-write-acp")
                              (split #","))
        acl-full-ctl  (some-> (get headers "x-amz-grant-full-control")
                              (split #","))
        explicit-acl  {:READ         (mapv ->grantee acl-read)
                       :READ_ACP     (mapv ->grantee acl-read-acp)
                       :WRITE        (mapv ->grantee acl-write)
                       :WRITE_ACP    (mapv ->grantee acl-write-acp)
                       :FULL_CONTROL (mapv ->grantee acl-full-ctl)}]
    (pr-str
     (cond

       canned-acl
       (case canned-acl
         "public-read-write"
         (merge init {:READ  [{:URI "anonymous"}]
                      :WRITE [{:URI "anonymous"}]})

         "public-read"
         (merge init {:READ [{:URI "anonymous"}]})

         "authenticated-read"
         (merge init {:READ [{:URI "authenticated"}]})

         "log-delivery-write" init

         "bucket-owner-read"
         (merge init {:READ [{:DisplayName owner
                              :ID          owner}]})

         "bucket-owner-full-control"
         init

         "private"
         (-> init
             (update-in [:FULL_CONTROL] conj {:ID tenant :DisplayName tenant})
             (update-in [:FULL_CONTROL] vec))

         nil
         init

         (throw (ex-info "Invalid Argument"
                         {:arg "x-amz-acl"
                          :val canned-acl
                          :status-code 400
                          :type :invalid-argument})))

       (some seq [acl-read acl-write
                  acl-read-acp acl-write-acp
                  acl-full-ctl])
       (-> explicit-acl
           (update-in [:FULL_CONTROL] conj {:ID tenant
                                            :DisplayName tenant})
           (update-in [:FULL_CONTROL] vec))


       :else
       init))))
