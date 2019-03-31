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

(defn build-table-joining-topology [builder]
  (let [flight-events (-> builder (j/kstream (topic-config "flight-events")) )
        departures (-> flight-events
                       (j/filter (fn [[k v] ]
                                   (= (:event-type v) :departed)))
                       (j/group-by-key)
                       (j/reduce (fn [ v1 v2]
                                   v2)
                                 (topic-config "flight-departures")))
        arrivals (-> flight-events (j/filter (fn [[k v] ]
                                 (= (:event-type v) :arrived)))
                     (j/group-by-key)
                     (j/reduce (fn [ v1 v2]
                                 v2)
                               (topic-config "flight-arrivals")))]
    (-> departures
        (j/join arrivals
                         (fn [v1 v2]
                           
                           (let [duration (java.time.Duration/between (.toInstant (:time v1)) (.toInstant (:time v2)))]
                             {:duration (.getSeconds duration)})))
        (j/to-kstream)
        (j/to (topic-config "flight-times"))))
  builder)
