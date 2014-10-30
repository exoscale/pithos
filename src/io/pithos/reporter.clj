(ns io.pithos.reporter
  (:require [clojure.tools.logging :refer [log]]))

(defprotocol Reporter
  (report! [this event]))

(defn log4j-reporter
  [{:keys [level]}]
  (reify Reporter
    (report! [_ event]
      (log (keyword level) (pr-str event)))))

(defn report-all!
  [reporters event]
  (doseq [reporter reporters]
    (report! reporter event)))
