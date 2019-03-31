(ns event-sourcing.delay-finder
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]
            [event-sourcing.utils :refer [topic-config]]))

(defn find-delays-topology [builder]
  (-> builder
      (j/kstream (topic-config "flight-events"))
      (j/filter (fn [[k v] ]
                  (and 
                   (= (:event-type v) :departed))))
      (j/map (fn [[k v]]
               (if (.after (:time v) (:scheduled-departure v))
                 [k {:status :left-late}]
                 [k {:status :left-on-time}])))
      (j/to (topic-config "flight-status")))
  builder)
