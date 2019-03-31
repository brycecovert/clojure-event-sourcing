(ns event-sourcing.core
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [event-sourcing.flight-time-analytics :as flight-time-analytics]
            [event-sourcing.passenger-counting :as passenger-counting]
            [event-sourcing.transducer :as transducer]
            [event-sourcing.utils :refer [topic-config]]
            [clojure.set :as set]))




(def app-config {"bootstrap.servers" "localhost:9092"
                 StreamsConfig/APPLICATION_ID_CONFIG "flight-app-4"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
                 ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
                 "acks"              "all"
                 "retries"           "0"
                 "cache.max.bytes.buffering" "0"})


(defn produce-one
  ([topic k v ]
   (with-open [producer (jc/producer app-config (topic-config topic))]
     @(jc/produce! producer (topic-config topic) k v))))

(defonce stream-app (atom nil))
(defonce continue-monitoring? (atom true))
(defn main [topology]
  (let [topology (-> (j/streams-builder)
                    (topology))
        _ (println (-> topology j/streams-builder* .build .describe .toString))

        kafka-streams (j/kafka-streams topology app-config)]
    (reset! stream-app kafka-streams)
    (j/start kafka-streams)))

(defn shutdown []
  (when @stream-app
    (j/close @stream-app))
  (when @continue-monitoring?
    (reset! continue-monitoring? false)))


(defn monitor-topics
  ([topics]
   (reset! continue-monitoring? true)
   (future 
     (with-open [subscription (jc/subscribed-consumer (assoc app-config "group.id" "monitor")
                                                      (map topic-config topics))]
       (loop [results (jc/poll subscription 200)] 
         (doseq [{:keys [topic-name key value]} results]
           (clojure.pprint/pprint [[:topic topic-name]
                                   [:key key]
                                   [:value value]]))
         (if @continue-monitoring?
           (recur (jc/poll subscription 200))
           nil))))))





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

#_(do (shutdown)
      (main flight-time-analytics/build-time-joining-topology)
      (monitor-topics ["flight-events" "flight-times"]))


#_(do (shutdown)
      (main flight-time-analytics/build-table-joining-topology)
      (monitor-topics ["flight-events" "flight-times"]))


#_(do (shutdown)
      (main passenger-counting/build-boarded-counting-topology)
      (monitor-topics ["flight-events" "passenger-counts"]))

#_(do (shutdown)
      (main passenger-counting/build-boarded-decorating-topology)
      (monitor-topics ["flight-events" "flight-events-with-passengers"]))

#_(do (shutdown)
      (main passenger-counting/build-boarded-decorating-topology-cleaner)
      (monitor-topics ["flight-events" "flight-events-with-passengers"]))

#_(do (shutdown)
      (main passenger-counting/build-empty-flight-emitting-topology)
      (monitor-topics ["flight-events" "flight-events"]))

#_(passenger-counting/get-passengers @stream-app "UA1496")

#_(do (shutdown)
      (main transducer/build-transducer-topology)
      (monitor-topics ["flight-events" "transduced-events"]))


#_(shutdown)


