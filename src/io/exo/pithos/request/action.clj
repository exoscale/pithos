(ns io.exo.pithos.request.action
  (:require [clout.core     :refer [route-matches route-compile]]
            [clojure.string :refer [join]]))

(defn action-routes
  []
  (let [sroute (route-compile "/")
        broute1 (route-compile "/:bucket")
        broute2 (route-compile "/:bucket/")
        oroute (route-compile "/:bucket/*")]
    [[:service (partial route-matches sroute)]
     [:bucket  (partial route-matches broute1)]
     [:bucket  (partial route-matches broute2)]
     [:object  (partial route-matches oroute)]]))

(defn match-action-route
  [request [target matcher]]
  (when-let [{bucket :bucket object :*} (matcher request)]
    {:target target :bucket bucket :object object}))

(defn yield-assoc-target
  []
  (let [routes (action-routes)]
    (fn [request]
      (merge request
        (or (some (partial match-action-route request) routes)
            {:target :unknown})))))

(defn yield-assoc-operation
  [suffixes]
  (fn [{:keys [request-method action-params target] :as request}]
    (assoc request
      :operation
      (->> (if-let [suffix (some suffixes action-params)]
             [request-method target suffix]
             [request-method target])
           (map name)
           (join "-")
           keyword))))
