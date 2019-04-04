(ns event-sourcing.decisions
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [event-sourcing.passenger-counting :refer [flight->passenger-count-ktable transform-with-stores]]
            [event-sourcing.utils :refer [topic-config]]))

(defn build-clean-plane-topology [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))
        passengers-ktable (flight->passenger-count-ktable flight-events-stream)
        passenger-store-name (.queryableStoreName (j/ktable* passengers-ktable))]
    (-> flight-events-stream
        (transform-with-stores (fn [event [passenger-store]]
                                 (assoc event
                                        :passengers
                                        (-> passenger-store
                                            (.get {:flight (:flight event)})
                                            (count))))
                               [passenger-store-name])
        (j/filter (fn [[_ event]]
                    (and (= 0 (:passengers event))
                         (= :passenger-departed (:event-type event)))))
        (j/map (fn [[k last-passenger-event]]
                 [k (-> last-passenger-event
                        (dissoc :passengers :who :event-type :time)
                        (assoc :decision :clean-plane))]))
        
        (j/through (topic-config "flight-decisions"))
        (j/process! (fn [_ k v]
                      (println "SIDE EFFECT: Cleaning plane " k))
                    [])
        )
    builder))
