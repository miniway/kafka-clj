(defproject kafka.client/kafka-clj "0.1.0-SNAPSHOT"
  :description "Kafka client for Clojure."
  :source-paths ["src/clj"]
  :url          "http://sna-projects.com/kafka/"
  :dependencies [[org.clojure/clojure	"1.4.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [com.googlecode.disruptor/disruptor "2.10.1"]
                 [com.netflix.curator/curator-framework "1.1.12"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :license {:name "The Apache Software License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.txt"
            :distribution :repo }
  :disable-deps-clean false
  :warn-on-reflection false
  :aot :all
  :test-path "test")
