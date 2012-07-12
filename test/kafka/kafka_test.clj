(ns 
  kafka.kafka-test
  (:use [kafka kafka types serializable buffer test util zookeeper]
        [clojure test])
  (:import [java.nio.channels.spi SelectorProvider]
           [java.nio.channels SocketChannel]
           [java.nio ByteBuffer]
           [org.apache.zookeeper KeeperException KeeperException$Code]
           [com.netflix.curator.framework CuratorFramework]
           [com.netflix.curator.framework.api GetChildrenBuilder GetDataBuilder]))

(def bytes-sent (atom 0))

(defn dummy-channel 
  [host port opt]
  (proxy [SocketChannel] [(SelectorProvider/provider)]
    (write [^ByteBuffer buffer] (swap! bytes-sent + (.limit buffer)) (println "dummy-write" (.limit buffer))  (.limit buffer))
    (implCloseSelectableChannel [] (println "dummy-close"))
  ))

(defn dummy-zk-data
  []
  (reify
    GetDataBuilder
    (forPath [this path] 
             (println "dummy-data" path) 
             (cond
               (= path "/brokers/ids/0") (.getBytes "host0-12345:host0:9092")
               (= path "/brokers/ids/1") (.getBytes "host1-12345:host1:9092")
               (= path "/brokers/ids/2") (.getBytes "host2-12345:host2:9092")
               (= path "/brokers/topics/topic/0") (.getBytes "1")
               (= path "/brokers/topics/topic/1") (.getBytes "2")
               :default (throw (KeeperException/create KeeperException$Code/NONODE))))
    )
)

(defn dummy-zk 
  [conn-str & {:keys [connection-timeout session-timeout watcher]}]
  (let [zk
    (reify
      CuratorFramework
      (getChildren [this] this)
      (getData [this] (dummy-zk-data))
      (close [this] (reset! ZK nil))
      GetChildrenBuilder
      (forPath [this path] 
               (println "dummy-get-children" path)
               (cond 
                 (= path "/brokers/ids") ["0" "1" "2" ] 
                 (= path "/brokers/topics") ["topic" ]
                 (= path "/brokers/topics/topic") ["0" "1" ] 
                 :default (throw (KeeperException/create KeeperException$Code/NONODE))))
      )]
  (reset! ZK zk)
  zk)
)


(deftest test-build-message
  (let [buffer  (buffer 128)]
    (build-message buffer "topic" 0 [["hello world"]])
    (is (= 36 (.position buffer)))
    (.clear buffer)
    
    (build-message buffer "topic" 0 [["hello world" "hello seoul"]]) ; + 10 (Message Overhead) + 11 (Message)
    (is (= 57 (.position buffer)))
    (.clear buffer)

    )
)



(deftest test-message
  (is (= 5 (count (.bytes (pack "hello"))))))

(deftest test-crt
  (is (= 907060870 (crc32-int (.getBytes "hello")))))

;(deftest test-build-request-data
;  (let [abs #(if (neg? %) (- %) %)
;        pool (atom {:broker-map {0 :channel} 
;                    :topic-partition {}
;                    :partitioner (fn [k n] (if k (rem (abs (hash k)) n) (rand-int n)))})] 
;    (is (= [0 "topic" -1 ["hello"]] (build-request-data pool "topic" nil "hello")))
;    (is (= [0 "topic2" -1 ["hello" "world"]] (build-request-data pool "topic2" nil ["hello" "world"])))
;    (swap! pool assoc :broker-map {0 :channel 1 :channel2 })
;    (is (= [[1 "topic3" -1 (seq ["world" "world2"])]] (build-request-data pool "topic3" nil [["helloo" "world" "world2"]])))
;    (is (= [1 "topic3" -1 (seq ["world" "world2"])] (build-request-data pool "topic3" "hellow" ["world" "world2"])))
;    ))

(def pv-topic-flatten (ns-resolve 'kafka.kafka 'topic-flatten))
(def pv-add-topic-partition (ns-resolve 'kafka.kafka 'add-topic-partition))
(def pv-get-zk-brokers (ns-resolve 'kafka.kafka 'get-brokers))

(deftest test-topic-flatten
  (is (= (seq [(seq ["topic" -1 ["hello"]])]) (doall (pv-topic-flatten {"topic" {-1 ["hello"]}}))))
  (is (= (seq [(seq ["topic" 1 ["world"]]) (seq ["topic" 0 ["hello"]])]) (doall (pv-topic-flatten {"topic" {0 ["hello"] 1 ["world"]}}))))
  )

(deftest test-add-topic-partition
  (let [brokers {0 :c1 1 :c2 2 :c3}]
    (is (= (seq [[0 -1] [1 -1] [2 -1]]) (pv-add-topic-partition brokers "topic")))
      ))

(deftest test-producer
  (with-captures [new-channel dummy-channel]
     (let [conf {:broker.list "0:host0:9092, 1:host1:9092"
                 :producer.type "sync"}]
       (reset! bytes-sent 0)
       (with-open [p (producer conf)] 
           (is (= 2 (times new-channel)))
           (.produce p "topic" "hello")
           (is (= 36 @bytes-sent))
         )))
  )


(deftest test-zk-brokers
  (with-captures [connect-zk! dummy-zk] 
    (let [conf {:zk.connect "host1:2181,host2:2181,host3:2181"}
          brokers (pv-get-zk-brokers conf)]
      (is (= 3 (count brokers)))
      )))

(deftest test-zk-producer
  (with-captures [new-channel dummy-channel
                  connect-zk! dummy-zk]
     (let [conf {:zk.connect "host1:2181,host2:2181,host3:2181"
                 :producer.type "sync"}]
       (with-open [p (producer conf)]
         (is (= [[0 0] [1 0] [1 1]] (@TOPIC-PARTITIONS "topic")))
         (reset! bytes-sent 0)
         (is (= 3 (times new-channel)))
         (.produce p "topic" "hello")
         (is (= 36 @bytes-sent)))
  )))
