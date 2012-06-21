(ns kafka.util
  (:use [clojure.tools.logging])
  (:import [clojure.lang IDeref])
  (:import [java.util.zip CRC32]))

; from the Strom project
(defn exception-cause? [klass ^Throwable t]
  (->> (iterate #(.getCause ^Throwable %) t)
       (take-while identity)
       (some (partial instance? klass))
       boolean))

(defn halt-process! [val & msg]
  (info "Halting process: " msg)
  (.halt (Runtime/getRuntime) val)
  )

(defn ^{:dont-test "Used in impl of thread-local"}
  thread-local*
  "Non-macro version of thread-local - see documentation for same."
  [init]
  (let [generator (proxy [ThreadLocal] []
                    (initialValue [] (init)))]
    (reify IDeref
      (deref [this]
        (.get generator)))))

(defmacro thread-local
  [& body]
  `(thread-local* (fn [] ~@body)))

(defn- gen-cycle [k] 
  (let [t (atom -1) kk (dec k)] 
    #(swap! t (fn [x] (if (= kk x) 0 (inc x))))))

(defn crc32-int
  "CRC for byte array."
  ^long [^bytes ba]
  (let [crc (doto (CRC32.) (.update ba))
        lv  (.getValue crc)]
    (.intValue (bit-and lv 0xffffffff))))

