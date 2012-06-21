(ns kafka.print-test
  (:use (kafka types)
        clojure.test)
  (:import (kafka.types Message)
           (java.io Serializable)))



(extend-type Serializable
  Pack
    (pack [this]
      (let [^String st (with-out-str (print-dup this *out*)) ]
        (Message. (.getBytes st "UTF-8")))))


(extend-type Message
  Unpack
    (unpack [this] 
      (let [^bytes ba  (.bytes this)
                   msg (String. ba "UTF-8")]
        (if (not (empty? msg))
          (try
            (read-string msg)
            (catch Exception e
              (println "Invalid expression " msg)))))))

(defn test-pack-unpack []
  ((is (= "test" (unpack (pack "test"))))
  (is (= 123 (unpack (pack 123))))
  (is (= true (unpack (pack true))))
  (is (= [1 2 3] (unpack (pack [1 2 3]))))
  (is (= {:a 1} (unpack (pack {:a 1}))))
  (is (= '(+ 1 2 3) (unpack (pack '(+ 1 2 3)))))
  ))

