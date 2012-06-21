(ns kafka.serializable-test
  (:use :reload-all (kafka types serializable)
        clojure.test))


;(deftest test-pack-unpack
;  (is (= "test" (String. (.bytes (pack "test")) "UTF8")))
;  (is (= 123 (Long. (unpack (pack 123)))))
;  (is (= true (unpack (pack true))))
;  (is (= [1 2 3] (unpack (pack [1 2 3]))))
;  (is (= {:a 1} (unpack (pack {:a 1}))))
;  (is (= '(+ 1 2 3) (unpack (pack '(+ 1 2 3)))))
;  (let [now (java.util.Date.)]
;    (is (= now (unpack (pack now)))))
;)

