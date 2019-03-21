(ns event-sourcing.core
  #_(:gen-class)
  
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]))


(def app-config {"bootstrap.servers" "localhost:9093"
                 StreamsConfig/APPLICATION_ID_CONFIG "flight-timer"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
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
  ([k v]
   (produce-one "flights" k v)

   )
  ([topic k v ]
   (with-open [producer (jc/producer app-config (topic-config topic))]
     @(jc/produce! producer (topic-config topic) k v))))

#_(produce-one {:flight "UA1495"} {:event :departed
                                   :time #inst "2019-03-17T00:00:00.000-00:00"
                                   :subject {:flight "UA1495"
                                             :airline "UA"
                                             :departed #inst "2019-03-17T00:00:00.000-00:00"}})

#_(produce-one {:flight "UA1495"} {:event :arrived
                                   :time #inst "2019-03-17T01:00:00.000-00:00"
                                   :subject {:flight "UA1495"
                                             :airline "UA"
                                             :arrived #inst "2019-03-17T01:00:00.000-00:00"}})

#_(produce-one {:flight "SW9"} {:event :departed
                                :flight {:flight "SW9" :airline "SW" :took-off (java.util.Date.)}})

(defn build-topology [builder]
  (-> builder
      (j/kstream (topic-config "flights"  ))
      (j/group-by-key)
      (j/reduce (fn [ v1 v2]
                  (println v1 v2)
                  (merge v1 v2))
                (topic-config "flights"))
      (j/to-kstream)
      #_(.mapValues (reify ValueMapper
                      (apply [_ v]
                        #_(println v)
                        (str v)))) 
      (j/to (topic-config "flight-results")))
  builder)


(defn build-time-joining-topology [builder]
  (let [flight-events (-> builder (j/kstream (topic-config "flights")) )
        departures (-> flight-events (j/filter (fn [[k v] ] (= (:event v) :departed))))
        arrivals (-> flight-events (j/filter (fn [[k v] ] (= (:event v) :arrived))))]
    (-> departures
        (j/peek (fn [thing] (println thing)))
       
        (j/join-windowed arrivals
                         (fn [v1 v2]
                           (let [duration (java.time.Duration/between (.toInstant (:time v1)) (.toInstant (:time v2)))]
                             {:duration (.getSeconds duration) :flight (:flight (:subject v1))}))
                         (JoinWindows/of 10000)
                         (topic-config "flights")
                         (topic-config "flights"))
        (j/to (topic-config "flight-times"))))
  builder)


(defonce s (atom nil))
(defn main [topology]
  (let [topology (-> (j/streams-builder)
                    (topology))
        
        _ (println (-> topology j/streams-builder* .build .describe .toString))

        kafka-streams (j/kafka-streams topology app-config)
        
        ]
    
    (reset! s kafka-streams)
    

    (j/start kafka-streams)))


(defn shutdown []
  (when @s
    (j/close @s)))
  

(defn build-wine-example-topology [builder]
  (let [events (-> builder (j/kstream (topic-config "warehouse-events-avro")) )]
    (-> events
        (j/map (fn [[k v]] [k v]))
        (j/to (topic-config "wine-events-avro"))))
  builder)


#_(do (shutdown) (main build-time-joining-topology))

#_(shutdown)
