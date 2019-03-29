(ns event-sourcing.core
  #_(:gen-class)
  
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]))


(def app-config {"bootstrap.servers" "localhost:9093"
                 StreamsConfig/APPLICATION_ID_CONFIG "flight-app-4"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
                 ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
                 "acks"              "all"
                 "retries"           "0"
                 "cache.max.bytes.buffering" "0"})


(defn topic-config [name]
  {:topic-name name
   :partition-count 1
   :replication-factor 1
   :key-serde (jse/serde)
   :value-serde (jse/serde)})

(defn produce-one
  ([topic k v ]
   (with-open [producer (jc/producer app-config (topic-config topic))]
     @(jc/produce! producer (topic-config topic) k v))))

(defn build-time-joining-topology [builder]
  ;; let flight_events = builder.stream();
  ;; let departures = flight_events.filter();
  (let [flight-events (-> builder
                          (j/kstream (topic-config "flight-events")) )
        departures (-> flight-events (j/filter (fn [[k v] ]
                                                 (= (:event-type v) :departed))))
        arrivals (-> flight-events (j/filter (fn [[k v] ]
                                               (= (:event-type v) :arrived))))]
    (-> departures
        (j/join-windowed arrivals
                         (fn [v1 v2]
                           (let [duration (java.time.Duration/between (.toInstant (:time v1)) (.toInstant (:time v2)))]
                             {:duration (.getSeconds duration) :flight (:flight (:subject v1))}))
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
        arrivals (-> flight-events
                     (j/filter (fn [[k v] ]
                                 (= (:event-type v) :arrived)))
                     (j/group-by-key)
                     (j/reduce (fn [ v1 v2]
                                 v2)
                               (topic-config "flight-arrivals")))]
    (-> departures
        (j/join arrivals
                         (fn [v1 v2]
                           
                           (let [duration (java.time.Duration/between (.toInstant (:time v1)) (.toInstant (:time v2)))]
                             {:duration (.getSeconds duration) :flight (:flight (:subject v1))})))
        (j/to-kstream)
        (j/to (topic-config "flight-times"))))
  builder)


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

(defn transformer-supplier [xform]
  #(let [processor-context (atom nil)]
     (reify  Transformer
       (init [_ pc]
         (reset! processor-context pc))
       (transform [_ k v]
         (transduce xform
                    (fn [processor-context [k v]]
                      (.forward processor-context k v)
                      processor-context)
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

(defonce s (atom nil))
(defn main [topology]
  (let [topology (-> (j/streams-builder)
                    (topology))
        _ (println (-> topology j/streams-builder* .build .describe .toString))

        kafka-streams (j/kafka-streams topology app-config)]
    (reset! s kafka-streams)
    (j/start kafka-streams)))


(defn shutdown []
  (when @s
    (j/close @s)))

(defn get-passengers [flight]
  (-> @s 
      (.store "passengers" (QueryableStoreTypes/keyValueStore))
      (.get {:flight flight})))

#_(get-passengers "UA1496")

#_(do (shutdown) (main build-time-joining-topology))


#_(do (shutdown) (main build-boarded-counting-topology))

#_(do (shutdown) (main build-boarded-decorating-topology))

#_(do (shutdown) (main build-boarded-decorating-topology-cleaner))

#_(do (shutdown) (main build-empty-flight-emitting-topology))

#_(do (shutdown) (main build-transducer-topology))


#_(shutdown)



#_(produce-one "flight-events" {:flight "UA1496"} {:event-type :passenger-boarded
                                                   :time #inst "2019-03-16T00:00:00.000-00:00"
                                                   :flight "UA1496"})

#_(produce-one "flight-events" {:flight "UA1496"} {:event-type :departed
                                                   :time #inst "2019-03-16T00:00:00.000-00:00"
                                                   :flight "UA1496"
                                                   })

#_(produce-one "flight-events" {:flight "UA1496"} {:event-type :arrived
                                                   :time #inst "2019-03-17T04:00:00.000-00:00"
                                                   :flight "UA1496"
                                                   })

#_(produce-one "flight-events" {:flight "UA1496"} {:event-type :passenger-departed
                                                   :time #inst "2019-03-16T00:00:00.000-00:00"
                                                   :flight "UA1496"})


