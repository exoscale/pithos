(ns io.exo.pithos.inode
  (:import java.util.UUID)
  (:require [qbits.alia         :refer [execute]]
            [qbits.hayt         :refer [select where set-columns
                                        delete update limit]]))
