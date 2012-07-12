(ns kafka.disruptor-test
  (:use [kafka disruptor])
  (:use [clojure test]))

(defn handler 
  [evt sequence end]
  (println "new-event" (.getName (Thread/currentThread)) evt sequence end))

(defn handler-odd 
  [evt sequence end]
  (when (odd? (.getKey evt)) (println "odd-event" (.getName (Thread/currentThread)) evt sequence end)))

(defn handler-even 
  [evt sequence end]
  (when (even? (.getKey evt)) (println "even-event" (.getName (Thread/currentThread)) evt sequence end)))

(deftest disruptor-processor
  (let [disruptor (new-disruptor handler)
        _ (start disruptor)
        ring-buffer (get-ring-buffer disruptor)
        retry 100
        ]
    (is (= -1 (.getCursor ring-buffer)))
    (doseq [i (range retry)] 
      (.setKeyValue (.get ring-buffer i) i (* i i))
      (.publish ring-buffer (.next ring-buffer)))
    (Thread/sleep 2000)
    (is (= retry (.next ring-buffer)))
    (stop disruptor)
    ))

(deftest disruptor-multi-consume
  (let [disruptor (new-disruptor [handler-odd handler-even])
        _ (start disruptor)
        ring-buffer (get-ring-buffer disruptor)
        retry 100
        ]
    (is (= -1 (.getCursor ring-buffer)))
    (doseq [i (range retry)] 
      (publish disruptor i (* i i)))
    (Thread/sleep 1000)
    (is (= retry (.next ring-buffer)))
    (stop disruptor)))

(deftest disruptor-batch-publish
  (let [disruptor (new-disruptor [handler-odd handler-even])
        _ (start disruptor)
        ring-buffer (get-ring-buffer disruptor)
        retry 100
        ]
    (is (= -1 (.getCursor ring-buffer)))
    (println "=================" (type (count (map (fn [i] [i (* i i)]) (range retry)))))
    (publishBatch disruptor (map (fn [i] [i (* i i)]) (range retry))) 
    (Thread/sleep 1000)
    (is (= retry (.next ring-buffer)))
    (stop disruptor)))

;(deftest disruptor-single
;  (let [disruptor (new-disruptor [handler-odd handler-even] :batch false)
;        ring-buffer (get-ring-buffer disruptor)
;        retry 100
;        ]
;    (is (= -1 (.getCursor ring-buffer)))
;      (publish-and-wait disruptor (map (fn [i] [i (* i i)]) (range retry)))
;    (Thread/sleep 1000)
;    (is (= retry (.next ring-buffer)))
;    (stop disruptor)))