(ns event-sourcing.utils
  (:require [jackdaw.serdes.edn :as jse]))

(defn topic-config [name]
  {:topic-name name
   :partition-count 1
   :replication-factor 1
   :key-serde (jse/serde)
   :value-serde (jse/serde)})
