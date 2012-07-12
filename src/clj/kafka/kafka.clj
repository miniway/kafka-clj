(ns 
  kafka.kafka
  (:use [kafka types buffer serializable disruptor util zookeeper])
  (:use [clojure.tools.logging])
  (:require [clojure [string :as str] [stacktrace :as trace]])
  (:import (kafka.types Message)
           (java.nio ByteBuffer)
           (java.nio.channels SocketChannel)
           (java.net Socket InetSocketAddress))
  (:gen-class 
    :methods [#^{:static true} [newProducer [java.util.Properties] kafka.types.Producer]])
  )


; Constants

(def ^:const REQUEST-SENT 0)
(def ^:const REQUEST-FETCH (int 1))
(def ^:const REQUEST-MULTIFETCH (int 2))
(def ^:const REQUEST-MULTIPRODUCE (int 3))
(def ^:const REQUEST-OFFSET (int 4)) 

(def ^:const MAGIC-NO-COMPRESSION (int 0))
(def ^:const MAGIC-COMPRESSION (int 1))

(def ^:const COMPRESSION-NO-COMPRESSION (int 0))
(def ^:const COMPRESSION-GZIP (int 1))
(def ^:const COMPRESSION-SNAPPY (int 2))


(def ^:const OFFSET_LATEST (int -1))
(def ^:const OFFSET_EARLIEST (int -2))

; NoError = 0;
; OffsetOutOfRangeCode = 1;
; InvalidMessageCode = 2;
; WrongPartitionCode = 3;
; InvalidRetchSizeCode = 4;

(def TOPIC-PARTITIONS (atom {}))

(defn new-channel
  "Create and setup a new channel for a host name, port and options.
  Supported options:
  :receive-buffer-size - receive socket buffer size, default 65536.
  :send-buffer-size    - send socket buffer size, default 65536.
  :socket-timeout      - socket timeout."
;  [^String host ^long port opts]
  [host port opts]
  (let [receive-buf-size (or (:socket.buffersize opts) 65536)
        send-buf-size    (or (:buffer.size opts) 102400)
        so-timeout       (or (:socket.timeout.ms opts) 30000)
        co-timeout       (or (:connect.timeout.ms opts) 5000)
        async            (or (= "async" (:producer.type opts)) false)
        ch (SocketChannel/open)]
    (doto (.socket ch)
      (.setReceiveBufferSize receive-buf-size)
      (.setSendBufferSize send-buf-size)
      (.setSoTimeout so-timeout)
      (.setKeepAlive true))
    (doto ch
      (.configureBlocking (not async))
      (.connect (InetSocketAddress. host port)))))

(defn- close-channel
  "Close the channel."
  [^SocketChannel channel]
  (.close channel)
  )

;   Response Header
;   0                   1                   2                   3
;   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |                        RESPONSE_LENGTH                        |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |         ERROR_CODE            |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  
(defn- response-size
  "Read first four bytes from channel as an integer."
  [channel]
  (with-buffer (buffer 4)
    (read-completely-from channel)
    (flip)
    (get-int)))

(defmacro with-error-code
  "Convenience response error code check."
  [request & body]
  `(let [error-code# (get-short)] ; error code
     (if (not= error-code# 0)
       (error (str "Request " ~request " returned error code: " error-code# "."))
       ~@body)))

; 
; Producer
;

;   Topic - Partition - Messages
;   0                   1                   2                   3
;   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |       TOPIC_LENGTH          |   TOPIC (variable length)       /
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |                           PARTITION                           |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |                      TOTAL MESSAGES_LENGTH                    |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;                           == [Message ] ==
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |                      MESSAGE LENGTH  + 6                      |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |     MAGIC       |  COMPRESSION  |           CHECKSUM          |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |      CHECKSUM (cont.)           |           PAYLOAD           /
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                             /
;  /                         PAYLOAD (cont.)                       /
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  
(defn build-message
  "Build message"
  ^ByteBuffer [^ByteBuffer buffer ^String topic ^Integer partition messages & [opts]]
  (let [magic       (byte (or (:magic opts) MAGIC-COMPRESSION))
        compression (byte (or (:compression.codec opts) COMPRESSION-NO-COMPRESSION)) ;  #FIXME: compressed.topics
        ^bytes topic-bytes (.getBytes topic)]  
    (doto buffer
      (.putShort (alength topic-bytes))  ; topic size
      (.put topic-bytes)                 ; topic
      (.putInt partition)                ; partition
      (.mark)
      (.putInt 0)                 ; dummy total message size
      )
        
    (let [pos (.position buffer)
          ^ByteBuffer child-buffer (.slice buffer)]
       (doseq [m messages]
          (let [^bytes bytes (.bytes (pack m))
                crc   (crc32-int bytes)
                size  (+ (alength bytes) 6)] ; message-length + 6 (crc + magic + compression)
            (doto child-buffer
              (.putInt size)
              (.put magic)
              (.put compression)
              (.putInt crc)
              (.put bytes)))
       )
       (set-length buffer (- pos 4) (.position child-buffer)))
    buffer)
  )

;   Single Request Header
;   0                   1                   2                   3
;   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |                       REQUEST_LENGTH                          |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;  |         REQUEST_TYPE          |                               |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               |
;  |          == Topic - Prtition - Message ==                     |
;  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+


(defn build-request
  "Build request"
  [^ByteBuffer buffer ^String topic ^Integer partition messages & [opts]]
  (doto buffer
    (.clear)
    (.putInt 0)); dummy total length
  (-> (.slice buffer)
      (.putShort REQUEST-SENT)
      (build-message topic partition messages opts)
      (.position)
      (set-length-to buffer 0))
  (.flip buffer)
  )

; Multi Request Header
; 0                   1                   2                   3
; 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
; +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
; |                       REQUEST_LENGTH                          |
; +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
; |         REQUEST_TYPE          |    TOPICPARTITION_COUNT       |
; +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
;               == [Topic - Prtition - Message] ==                |
; +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

(defn build-multi-request
  "Build request"
  [^ByteBuffer buffer topic-partition-messages & [opts]]
  (doto buffer 
    (.clear)
    (.putInt buffer 0))               ; dummy total length
  (let [^ByteBuffer child-buffer 
         (-> (.slice buffer)
             (.putShort REQUEST-MULTIPRODUCE)    ; request type
             (.putShort (count topic-partition-messages)))]    
      (doseq [[[topic partition] messages] topic-partition-messages] 
        (build-message child-buffer topic partition messages opts))
      (set-length buffer 0 (.position child-buffer)))
  (.flip buffer)
  )

(defn- build-packet
  [^ByteBuffer buffer topic-partition-messages opts]
  (if (= (count topic-partition-messages) 1) 
     (let [[[topic partition] message] (first topic-partition-messages)]
        (build-request buffer topic partition message opts))
     (build-multi-request buffer topic-partition-messages opts))
  )

(defn- broker-handler
  "Send messages."
  [[^long broker-id ^SocketChannel channel] opts evt sequence end]
  (when (= (.getKey evt) broker-id)
      (.write channel (.getValue evt)))   ; TODO exception handling, reconnect or stop etc
  )


(defn- add-topic-partition
  [brokers topic]
  (let [tp (map #(vector % -1) (keys brokers))]
    (swap! TOPIC-PARTITIONS merge {topic tp})
    tp))

; returns [ broker-id , partition ]
(defn- get-broker-partition
  [brokers partitioner topic & [key]]
   (let [broker-partition-list (get @TOPIC-PARTITIONS topic)
         broker-partition-list (or broker-partition-list (add-topic-partition brokers topic))]
      (nth broker-partition-list (partitioner key (count broker-partition-list)))
    )
   )


(defn- add-brokers
  [broker-map conf]
  (into {} (for [[k v] broker-map] 
             {k (new-channel (first v) (Integer/parseInt (last v)) conf)})))

(defn- get-brokers
  [conf]
  (or (get-zk-brokers (:zk.connect conf) 
                        :connection-timeout (or (:zk.connectiontimeout.ms conf) 6000)
                        :session-timeout (or (:zk.sessiontimeout.ms conf) 6000))
                    (let [broker-list (map 
                                        (comp #(str/split % #":") str/trim) 
                                        (str/split (:broker.list conf) #","))]
                      (zipmap 
                        (map (comp #(Integer/parseInt %) first) broker-list) 
                        (map rest broker-list))
                      )))

(defn- get-set-topic-partitions
  []
  (swap! TOPIC-PARTITIONS merge (get-zk-topic-partitions)))

(defn- topic-flatten [d]
    (lazy-seq
        (when-let [[k v] (first d)]
            (concat (map #(cons k %) (reduce #(cons %2 %1) [] v)) (topic-flatten (rest d)))
        )))

(defn- abs [v]
  (if (neg? v) (- v) v)
  )

(defn- partition-messages 
  [brokers partitioner topic-key-messages]
  (loop [acc {} 
         [topic key message] (first topic-key-messages)]
    (if topic
      (let [[bid partition] (get-broker-partition brokers partitioner topic key)
            value (get acc bid {})
            mvalue (get value [topic partition] [])
            value (assoc value [topic partition] 
                         (concat mvalue (if 
                                 (instance? Iterable message) message
                                 [message])))
            ]
        (recur (assoc acc bid value) (rest topic-key-messages)))
        acc))
  )

(defn producer
  "Producer factory. See new-channel for list of supported options."
  [conf]
  (let [tick  (let [t (atom -1)] #(swap! t inc))
        partitioner (fn [key num-partition] (cond (= 1 num-partition) 0
                                                   (nil? key) (rem (tick) num-partition)
                                                   :else (rem (abs (hash key)) num-partition)))
        
        ;partitioner (fn [key num-partition] (cond (= 1 num-partition) 0
        ;                                           (nil? key) (rand-int num-partition)
        ;                                           :else (rem (abs (hash key)) num-partition)))

        brokers (-> (get-brokers conf)
                     (add-brokers conf))
        _ (get-set-topic-partitions)
        pt (:producer.type conf)
        async (or (= "async" pt) false)
        batch (or (= "batch" pt) (not (= "sync" pt)) (nil? pt))
        dyn-zk (or (:listen.zookeepr conf) false)
        multithread (or (:multithread conf) false)
        disruptor (new-disruptor (map #(partial broker-handler % conf) brokers) 
                                 :mt multithread 
                                 :factory-options [(or (:max.message.size conf) 131072)])
        _ (.start disruptor)
        _ (info "async" async batch multithread @TOPIC-PARTITIONS)
        build-packet-fn (fn [buffer value] (build-packet buffer value conf))
        _ (when-not dyn-zk (disconnect-zk!))
        ]
    (reify Producer
      
      (produce [this topic-key-messages]
               (let [requests (partition-messages brokers partitioner topic-key-messages)
                     cursor (.publishBatch disruptor build-packet-fn requests)
                     ]
                 (when-not batch
                   (let [counter (atom 200)]
                     (while (> cursor (.getMinimumSequence disruptor))
                       (swap! counter dec)
                       (when (< @counter 100) (Thread/yield)))))
               ))
      
      (produce [this topic message]
               (produce this [[topic nil message]]))
      
      (produce [this topic key message]
               (produce this [[topic key message]]))


      (close [this]
             (info "Closing Producer")
             (disconnect-zk!)
             (.stop disruptor)
             (doseq [channel (vals brokers)] 
               (close-channel channel) 
             )
             (info "Closed Producer")
           )
      )))

(defn -newProducer
  [conf]
  (producer (into {} (map (fn [[k v]] [(keyword k) v]) conf))))
;
; Consumer
;

; Offset

(defn- offset-fetch-request
  "Fetch offsets request."
  [channel topic partition time max-offsets]
  (let [size     (+ 4 2 2 (count topic) 4 8 4)]
    (with-buffer (buffer size)
      (with-length          ; request size
        (put (short REQUEST-OFFSET))           ; request type
        (length-encoded short     ; topic size
          (put topic))            ; topic
        (put-int partition)     ; partition
        (put (long time))         ; time in millisecs before (-1: from latest available)
        (put-int max-offsets))  ; max-offsets
        (flip)
        (write-to channel))))

(defn- fetch-offsets
  "Fetch offsets as an integer sequence."
  [channel topic partition time max-offsets]
  (offset-fetch-request channel topic partition time max-offsets)
  (let [rsp-size (response-size channel)]
    (with-buffer (buffer rsp-size)
      (read-completely-from channel)
      (flip)
      (with-error-code "Fetch-Offsets"
        (loop [c (get-int) res []]
          (if (> c 0)
            (recur (dec c) (conj res (get-long)))
            (doall res)))))))
 
; Messages

(defn- message-fetch-request
  "Fetch messages request."
  [channel topic partition offset max-size]
  (let [size (+ 4 2 2 (count topic) 4 8 4)]
    (with-buffer (buffer size)
      (with-length     ; request size
        (put (short 1))       ; request type
        (length-encoded short ; topic size
          (put topic))        ; topic
        (put-int partition) ; partition
        (put (long offset))   ; offset
        (put-int max-size)) ; max size
        (flip)
        (write-to channel))))

(defn- read-response
  "Read response from buffer. Returns a pair [new offset, messages sequence]."
  [offset]
  (with-error-code "Fetch-Messages"
    (loop [off offset msg []]
        (if-let [size (remaining-size)]
        (let [magic   (get-byte) ; magic
              algo   (get-byte) ; algo
              crc     (get-int)  ; crc
              message (Message. (get-array (- size 6)))
              ]
          (recur (+ off size 4) (conj msg message)))
          [off (doall msg)]))))

(defn- fetch-messages
  "Message fetch, returns a pair [new offset, messages sequence]."
  [channel topic partition offset max-size]
  (message-fetch-request channel topic partition offset max-size)
  (let [rsp-size (response-size channel)
        _ (debug "Response size" rsp-size topic partition offset)]
    (with-buffer (buffer rsp-size)
      (read-completely-from channel)
      (flip)
      (read-response offset))))

; Consumer sequence

(defn- seq-fetch
  "Non-blocking fetch function used by consumer sequence."
  [channel topic partition opts]
  (let [max-size (or (:fetch.size opts) 307200)]
    (fn [offset]
      (fetch-messages channel topic partition offset max-size))))

(defn- blocking-seq-fetch
  "Blocking fetch function used by consumer sequence."
  [channel topic partition opts]
  (let [repeat-count   (or (:repeat-count opts) 10)
        repeat-timeout (or (:backoff.increment.ms opts) 1000)
        max-size       (or (:fetch.size opts) 307200)]
    (fn [offset]
      (loop [c repeat-count]
        (if (> c 0)
          (let [rs (fetch-messages channel topic partition offset max-size)]
            (if (or (nil? rs) (= offset (first rs)))
              (do
                (Thread/sleep repeat-timeout)
                (recur (dec c)))
              (doall rs)))
          (debug "Stopping blocking seq fetch."))))))

(defn- fetch-queue
  [offset queue fetch-fn]
  (if (empty? @queue)
    (let [[new-offset msg] (fetch-fn @offset)]
      (when new-offset
        (debug (str "Fetched " (count msg) " messages:"))
        (debug (str "New offset " new-offset "."))
        (swap! queue #(reduce conj % (reverse msg)))
        (reset! offset new-offset)))))

(defn- consumer-seq
  "Sequence constructor."
  [offset fetch-fn]
  (let [offset (atom offset)
        queue  (atom (seq []))]
    (reify
      clojure.lang.IPersistentCollection
        (seq [this]    this)
        (cons [this _] (throw (Exception. "cons not supported for consumer sequence.")))
        (empty [this]  nil)
        (equiv [this o]
          (fatal "Implement equiv for consumer seq!")
          false)
      clojure.lang.ISeq
        (first [this] 
          (fetch-queue offset queue fetch-fn)
          (first @queue))
        (next [this]
          (swap! queue rest)
          (fetch-queue offset queue fetch-fn)
          (if (not (empty? @queue)) this))
        (more [this]
          (swap! queue rest)
          (fetch-queue offset queue fetch-fn)
          (if (empty? @queue) (empty) this))
      Object
      (toString [this]
        (str "ConsumerQueue")))))

; Consumer factory 

(defn consumer
  "Consumer factory. See new-channel for list of supported options."
  ([conf]
  (let [broker-map (get-brokers conf)
        brokers (add-brokers broker-map conf)
        groupid (:groupid conf)]
    ))
  ([host port & [opts]]
  (let [channel (new-channel host port opts)]
    (reify Consumer
      (consume [this topic partition offset max-size]
        (fetch-messages channel topic partition offset max-size))
      
      (offsets [this topic partition time max-offsets]
        (fetch-offsets channel topic partition time max-offsets))

      (consume-seq [this topic partition]
        (let [[offset] (fetch-offsets channel topic partition OFFSET_LATEST 1)]
          (info (str "Initializing last offset to " offset "."))
          (consumer-seq (or offset 0) (seq-fetch channel topic partition opts))))

      (consume-seq [this topic partition opts]
        (let [[offset] (or (:offset opts)
                           (fetch-offsets channel topic partition OFFSET_LATEST 1))
              fetch-fn (if (:blocking opts)
                         (blocking-seq-fetch channel topic partition opts)
                         (seq-fetch channel topic partition opts))]
          (debug (str "Initializing last offset to " offset "."))
          (consumer-seq (or offset 0) fetch-fn)))

      java.io.Closeable
      (close [this]
        (close-channel channel))))))

