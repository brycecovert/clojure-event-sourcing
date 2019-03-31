(ns event-sourcing.transducer
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]
            [event-sourcing.utils :refer [topic-config]]))

(defn transformer-supplier [xform]
  #(let [processor-context (atom nil)]
     (reify  Transformer
       (init [_ pc]
         (reset! processor-context pc))
       (transform [_ k v]
         (transduce xform
                    (fn ([processor-context [k v]]
                       (.forward processor-context k v)
                       processor-context)
                      ([processor-context]
                       processor-context))
                    @processor-context
                    [[k v]])
         (.commit @processor-context)
         nil)
       (close [_]))))

(defn build-transducer-topology [builder]
  (let [xform (comp (filter (fn [[k v]]
                              (#{:passenger-boarded :passenger-departed} (:event-type v) )))
                    (map (fn [[k v]]
                           [k (assoc v :decorated? true)]))
                    (mapcat (fn [[k v]]
                              [[k v]
                               [k v]])))
        boarded-events (-> builder
                           (j/kstream (topic-config "flight-events"))
                           (j/transform (transformer-supplier xform))
                           (j/to (topic-config "transduced-events")))])
  builder)
