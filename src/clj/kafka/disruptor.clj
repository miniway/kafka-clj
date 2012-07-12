(ns kafka.disruptor
  (:use [clojure.tools.logging])
  (:import [com.lmax.disruptor RingBuffer 
            EventFactory EventHandler
            MultiThreadedClaimStrategy MultiThreadedLowContentionClaimStrategy
            SingleThreadedClaimStrategy
            BatchEventProcessor EventProcessor
            Sequence Sequencer BatchDescriptor SequenceBarrier
            SleepingWaitStrategy])
  (:import [com.lmax.disruptor.util Util])
  (:import [java.nio ByteBuffer])
  (:import [java.util.concurrent ExecutorService Executors TimeUnit]))

(definterface KVGetterSetter
  (setKeyValue [^long key value])
  (setKey [^long key])
  (getkeyValue [])
  (^long getKey [])
  (getValue [])
  )

(deftype kv-event [^{:unsynchronized-mutable true} ^long k
                   ^{:unsynchronized-mutable true} v] 
  KVGetterSetter
  (setKeyValue [this key value] (set! k key) (set! v value) this)
  (setKey [this key] (set! k key) this)
  (getkeyValue [this] [(.k this) (.v this)])
  (getKey [this] (.k this))
  (getValue [this] (.v this))
  )

(deftype buffer-event [^{:unsynchronized-mutable true} ^long k  ^ByteBuffer v] 
  KVGetterSetter
  (setKeyValue [this key value] (set! k key) this)
  (setKey [this key] (set! k key) this)
  (getkeyValue [this] [(.k this) (.v this)])
  (getKey [this] (.k this))
  (getValue [this] (.v this))
  )

(def default-event-factory
  (proxy [EventFactory] []
    (newInstance [] (buffer-event. -1 nil))))

(defn buffer-event-factory-fn [buffer-size]
  (proxy [EventFactory] []
    (newInstance [] (buffer-event. -1 (ByteBuffer/allocateDirect buffer-size)))))


(defn new-event-handler
  [f]
  (proxy [EventHandler] []
    (onEvent [event sequence end]
             (f event sequence end))))

(defn new-event-processor
  [ring-buffer barrier handler ]
  (BatchEventProcessor. ring-buffer barrier (new-event-handler handler))
 )


(defprotocol disruptor
  (start [this] "Start disruptor")
  (stop [this] "Stop disruptor")
  (get-ring-buffer [this] "Get the Ring-Buffer")
  (publish [this k v] "Publish kev-value Event")
  (publishBatch 
    [this kvs]
    [this f kvs] "Publish multiple kev-value Event")
  (getCursor [this] "Get current cursor value")
  (getMinimumSequence [this] "Get the mininum consumer sequence")
  (getMaximumSequence [this] "Get the mininum consumer sequence")
  )

(defn waitForFinished 
  [barrier processor & [timeout]]
  (let [sequence (.getSequence processor)
        cursor (.getCursor barrier)
        timeout (+ (System/currentTimeMillis) (or timeout 10000))
       ]
    (while (and (< (.get sequence) cursor) (< (System/currentTimeMillis) timeout)) 
      (info "waitTermination" processor (- timeout (System/currentTimeMillis)) "ms" (.get sequence) cursor)
      (Thread/sleep 1000))
    ))

(defn new-disruptor 
  [event-handler & {:keys [size mt event-factory factory-options] 
                    :or {size 256 
                         mt false
                         factory-options [8192]
                         event-factory (apply buffer-event-factory-fn factory-options)  
                         }}] 
  (let [^RingBuffer ring-buffer (RingBuffer. event-factory
                                 (if mt (MultiThreadedClaimStrategy. size)
                                   (SingleThreadedClaimStrategy. size)) 
                                 (SleepingWaitStrategy.))
        ^SequenceBarrier barrier (.newBarrier ring-buffer (make-array Sequence 0))
        processors (map (fn [eh] (new-event-processor ring-buffer barrier eh )) 
                     (if (sequential? event-handler) event-handler [event-handler]))
        ^ExecutorService executor (Executors/newFixedThreadPool (count processors)) 
        consumer-sequences (into-array Sequence (map #(.getSequence %) processors))
        _ (.setGatingSequences ring-buffer consumer-sequences)
        update-fn (fn [e k v] (assoc e :k k :v v))
        ]
    (reify disruptor
      (start [this]
             (doseq [p processors] (.submit executor p)))
      (stop [this]
            (info "Stoping Disruptor")
            (doseq [p processors]
              (waitForFinished barrier p))
            (.alert barrier)
            (doseq [p processors]
              (.halt p))
            (.shutdownNow executor)
            (info "Stopped Disruptor")
            )
      (publish [this k v]
            (let [sequence (.next ring-buffer)]
              (.setKeyValue (.get ring-buffer sequence) k v)
              (.publish ring-buffer sequence)
              sequence))
      (publishBatch [this kvs]
            (let [^BatchDescriptor batch (.newBatchDescriptor ring-buffer (count kvs))
                  batch (.next ring-buffer batch)
                  start (.getStart batch)
                  ]
              (doseq [[i [k v]] (map-indexed vector kvs)] 
                (.setKeyValue (.get ring-buffer (+ start i)) k v))
              (.publish ring-buffer batch)
              (.getEnd batch)))
      (publishBatch [this f kvs]
            (let [batch (->> (.newBatchDescriptor ring-buffer (count kvs))
                             (.next ring-buffer))
                  start (.getStart batch)
                  err (atom nil)
                  ]
              (try
                (doseq [[i [k v]] (map-indexed vector kvs)] 
                  (-> (.get ring-buffer (+ start i))
                    (.setKey k)
                    (.getValue)
                    (f v)))
              (catch Exception e
                (error e)
                (reset! err e)
                ))
              (when @err
                (doseq [i (range (count kvs))] 
                    (-> (.get ring-buffer (+ start i))
                      (.setKey -1)
                    )))
              (.publish ring-buffer batch)
              (when @err (throw @err))
              (.getEnd batch)))

      (getCursor [this] (.getCursor ring-buffer))
      (getMinimumSequence [this] (Util/getMinimumSequence consumer-sequences))
      (getMaximumSequence [this] (apply max (map #(.get %) consumer-sequences)))
      (get-ring-buffer [this] ring-buffer)
    )))
