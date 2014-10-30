(ns io.pithos.reporter
  (:require [clojure.tools.logging :refer [info]]))

(defprotocol Reporter
  (report! [this event]))

(defn log4j-reporter
  []
  (reify Reporter
    (report! [_ event]
      (info (pr-str event)))))

(defn report-all!
  [reporters event]
  (doseq [reporter reporters]
    (report! reporter event)))
