(ns event-sourcing.core
  #_(:gen-class)
  
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]))


(def app-config {"bootstrap.servers" "localhost:9092"
                 StreamsConfig/APPLICATION_ID_CONFIG "flight-app-3"
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


(defn build-topology [builder]
  (-> builder
      (j/kstream (topic-config "flight-events"  ))
      (j/group-by-key)
      (j/reduce (fn [ v1 v2]
                  (println v1 v2)
                  (merge v1 v2))
                (topic-config "flight-events"))
      (j/to-kstream)
      #_(.mapValues (reify ValueMapper
                      (apply [_ v]
                        #_(println v)
                        (str v)))) 
      (j/to (topic-config "flight-results")))
  builder)


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


(defn build-boarded-counting-topology [builder]
  (let [boarded-events (-> builder (j/kstream (topic-config "flight-events"))
                           (j/filter (fn [[k v] ]
                                       (= (:event-type v) :passenger-boarded))))]
    (-> boarded-events
        (j/group-by-key )
        (j/count)
        (j/to-kstream)
        (j/map (fn [[k v]] [k (assoc k :passengers v)]))
        (j/to (topic-config "passenger-counts"))))
  builder)



(defn build-empty-flight-emitting-topology [builder]
  (let [boarded-events (-> builder (j/kstream (topic-config "flight-events"))
                           (j/filter (fn [[k v] ]
                                       (#{:passenger-boarded :passenger-departed} (:event-type v) ))))]
    (-> boarded-events
        (j/group-by-key )
        (j/aggregate (constantly {:count 0})
                     (fn [current-count [_ event]]
                       (cond-> current-count
                         true (assoc :time (:time event))
                         (= :passenger-boarded (:event-type event)) (update :count inc)
                         (= :passenger-departed (:event-type event)) (update :count dec)))
                     (topic-config "flight-count-store2"))
        
        (j/to-kstream)
        (j/filter (fn [[k v]] (= 0 (:count v))))
        (j/map (fn [[k v]] [k (assoc k :event-type :flight-ready-to-turnover
                                     :time (:time v))]))
        (j/to (topic-config "flight-events"))))
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

#_(do (shutdown) (main build-time-joining-topology))

#_(do (shutdown) (main build-empty-flight-emitting-topology))

#_(do (shutdown) (main build-boarded-counting-topology))

#_(do (shutdown) (main build-empty-flight-emitting-topology))


#_(shutdown)
