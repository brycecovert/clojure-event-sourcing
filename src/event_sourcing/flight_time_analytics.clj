(ns event-sourcing.flight-time-analytics
  (:import [org.apache.kafka.streams.kstream JoinWindows]
           [java.time Duration])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]
            [event-sourcing.utils :refer [topic-config]]))

(defn build-time-joining-topology [builder]
  (let [flight-events (j/kstream builder (topic-config "flight-events"))
        departures (-> flight-events
                       (j/filter (fn [[k v] ]
                                   (= (:event-type v) :departed))))
        arrivals (-> flight-events
                     (j/filter (fn [[k v]]
                                 (= (:event-type v) :arrived))))]
    (-> departures
        (j/join-windowed arrivals
                         (fn [departure arrival]
                           (let [duration (Duration/between
                                           (.toInstant (:time departure))
                                           (.toInstant (:time arrival)))]
                             {:duration (.getSeconds duration)}))
                         (JoinWindows/of 10000)
                         (topic-config "flight-events")
                         (topic-config "flight-events"))
        (j/to (topic-config "flight-times"))))
  builder)

;; if a kstream is
;; k1 -> k1-a
;; k2 -> k2-a
;; k1 -> k1-b
;; k2 -> k2-b
;; k1 -> k1-c

;; a kstream is a single value for the sequence in the stream
;; k1 -> f(k1-a, k1-b, k1-c)
;; k2 -> f(k2-a, k2-b)

(defn build-table-joining-topology [builder]
  (let [flight-events (j/kstream builder (topic-config "flight-events"))
        departures (-> flight-events
                       (j/filter (fn [[k v] ]
                                   (= (:event-type v) :departed)))
                       (j/group-by-key)
                       (j/reduce (fn [ v1 v2]
                                   v2)
                                 (topic-config "flight-departures")))
        arrivals (-> flight-events
                     (j/filter (fn [[k v] ]
                                 (= (:event-type v) :arrived))))]
    (-> arrivals
        (j/left-join departures
                         (fn [arrival departure]
                           (let [duration (java.time.Duration/between
                                           (.toInstant (:time departure))
                                           (.toInstant (:time arrival)))]
                             {:duration (.getSeconds duration)})))
        (j/to (topic-config "flight-times"))))
  builder)
