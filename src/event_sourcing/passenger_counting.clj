(ns event-sourcing.passenger-counting
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [event-sourcing.utils :refer [topic-config]]))

(defn flight->passenger-count-ktable [flight-events-stream]
  (-> flight-events-stream
      (j/filter (fn [[k v] ]
                  (#{:passenger-boarded :passenger-departed} (:event-type v))))
      (j/group-by-key )
      (j/aggregate (constantly 0)
                   (fn [current-count [_ event]]
                     (cond-> current-count
                       (= :passenger-boarded (:event-type event)) inc
                       (= :passenger-departed (:event-type event)) dec))
                   (topic-config "passengers"))))

(defn build-boarded-counting-topology [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))]
    (-> flight-events-stream
        (flight->passenger-count-ktable)
        (j/to-kstream)
        (j/map (fn [[k count]]
                 [k (assoc k :passenger-count count)]))
        (j/to (topic-config "passenger-counts"))))
  builder)

(defn build-boarded-decorating-topology [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))
        passengers-ktable (flight->passenger-count-ktable flight-events-stream)
        passenger-store-name (.queryableStoreName (j/ktable* passengers-ktable))]
    (-> flight-events-stream
        (j/transform-values #(let [passenger-store (atom nil)]
                               (reify  ValueTransformer
                                 (init [_ pc]
                                   (reset! passenger-store (.getStateStore pc passenger-store-name)))
                                 (transform [_ v]
                                   (assoc v :passengers (.get @passenger-store {:flight (:flight v)})))
                                 (close [_])))
                            [passenger-store-name])
        (j/to (topic-config "flight-events-with-passengers")))
    builder))

(defn transform-with-stores [stream f store-names]
  (j/transform-values stream #(let [stores (atom nil)]
                                (reify  ValueTransformer
                                  (init [_ pc]
                                    (reset! stores (mapv (fn [s] (.getStateStore pc s)) store-names)))
                                  (transform [_ v]
                                    (f v @stores))
                                  (close [_])))
                                store-names))

(defn build-boarded-decorating-topology-cleaner [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))
        passengers-ktable (flight->passenger-count-ktable flight-events-stream)
        passenger-store-name (.queryableStoreName (j/ktable* passengers-ktable))]
    (-> flight-events-stream
        (transform-with-stores (fn [event [passenger-store]]
                                 (assoc event :passengers (.get passenger-store {:flight (:flight event)})))
                               [passenger-store-name])
        (j/to (topic-config "flight-events-with-passengers")))
    builder))

(defn build-empty-flight-emitting-topology [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))
        passengers-ktable (flight->passenger-count-ktable flight-events-stream)
        passenger-store-name (.queryableStoreName (j/ktable* passengers-ktable))]
    (-> flight-events-stream
        (transform-with-stores (fn [event [passenger-store]]
                                 (assoc event :passengers (.get passenger-store {:flight (:flight event)})))
                               [passenger-store-name])
        (j/filter (fn [[_ event]]
                    (and (= 0 (:passengers event))
                         (= :passenger-departed (:event-type event)))))
        (j/map (fn [[k last-passenger-event]]
                 [k (-> last-passenger-event
                        (dissoc :passengers)
                        (assoc :event-type :plane-empty))]))
        (j/to (topic-config "flight-events")))
    builder))

(defn get-passengers [streams flight]
  (-> streams
      (.store "passengers" (QueryableStoreTypes/keyValueStore))
      (.get {:flight flight})))
