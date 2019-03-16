(ns event-sourcing.core
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder]
           [org.apache.kafka.streams.kstream ValueMapper Reducer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))


  #_(def xform (comp (filter (fn [[k v]] (string? v)))
                   (map (fn [[k v]] [v k]))
                   (filter (fn [[k v]] (= "foo" v)))))
  
  #_(def kstream (-> builder
                   (stream "tset")
                   (transduce-kstream xform)
                   (.to "test")))
(defn produce-one [k v]
  (let [producer (KafkaProducer. {"bootstrap.servers" "localhost:9093"
                                  "acks"              "all"
                                  "retries"           "0"
                                  "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer"
                                  "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"})]

    @(.send producer (ProducerRecord. "flight-events" k (pr-str v)))
    (.close producer)))

#_(produce-one "UA1492" {:flight "UA1492" :airline "UA" :departs (java.util.Date.)})

#_(produce-one "UA1492" {:flight "UA1492" :airline "UA" :arrives (java.util.Date.)})

#_(produce-one "SW9" {:flight "SW9" :airline "SW" :took-off (java.util.Date.)})

(defonce s (atom nil))
(defn main []
  (let [builder (-> (StreamsBuilder.))
        _  (-> builder
               (.stream "flight-events")
               
               
               (.groupByKey)
               
               (.reduce (reify Reducer
                          (apply [_ v1 v2]
                            (pr-str (merge (read-string v1) (read-string v2))))))
               (.toStream)
               #_(.mapValues (reify ValueMapper
                             (apply [_ v]
                               #_(println v)
                               (str v)))) 
               (.to "flight-counts"))

        kafka-streams (KafkaStreams. (.build builder)
                                     (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                                                      StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
                                                      StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9093"
                                                      StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   org.apache.kafka.common.serialization.Serdes$StringSerde
                                                      StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG org.apache.kafka.common.serialization.Serdes$StringSerde}))]
    (reset! s kafka-streams)
    

    (.start kafka-streams)))


(defn shutdown []
  (when @s
    (.close @s)))
  


#_(do (shutdown) (main))


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
