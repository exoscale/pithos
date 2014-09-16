(ns io.pithos.reporter
  (:require [clojure.tools.logging    :refer [info]]
            [clj-logging-config.log4j :refer [with-logging-context]]))

(defprotocol Reporter
  (report! [this opcode attrs]))

(defn nil-reporter
  [_]
  (reify Reporter
    (report! [this _ _])))

(defn log4j-reporter
  [_]
  (reify Reporter
    (report! [this opcode attrs]
      (with-logging-context attrs
        (info "report:" (name opcode))))))
