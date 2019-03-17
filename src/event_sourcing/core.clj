(ns event-sourcing.core
  #_(:gen-class)
  
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder]
           [org.apache.kafka.streams.kstream ValueMapper Reducer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :as jse]))


(def app-config {"bootstrap.servers" "localhost:9093"
                 StreamsConfig/APPLICATION_ID_CONFIG "flights-1"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
                 "acks"              "all"
                 "retries"           "0"
                 "cache.max.bytes.buffering" "0"})


(defn topic-config [name]
  {:topic-name name
   :partition-count 1
   :replication-factor 1
   :key-serde (jse/serde)
   "key.serializer" (jse/serde)
   :value-serde (jse/serde)})

(defn produce-one [k v]

  (with-open [producer (jc/producer app-config (topic-config "flights"))]
    (jc/produce! producer (topic-config "flights") {:flight k} v))
  
  #_(let [producer (KafkaProducer. app-config)]
    @(.send producer (ProducerRecord. "flight-events" k v))
    (.close producer)))

#_(produce-one "UA1492" {:flight "UA1492" :airline "UA" :departs (java.util.Date.)})

#_(produce-one "UA1492" {:flight "UA1492" :airline "UA" :arrives (java.util.Date.)})

#_(produce-one "SW9" {:flight "SW9" :airline "SW" :took-off (java.util.Date.)})

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

(defonce s (atom nil))
(defn main []
  (let [topology (-> (j/streams-builder)
                    (build-topology))

        kafka-streams (j/kafka-streams topology app-config)]
    (reset! s kafka-streams)
    

    (j/start kafka-streams)))


(defn shutdown []
  (when @s
    (j/close @s)))
  


#_(do (shutdown) (main))


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
