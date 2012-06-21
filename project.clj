(defproject kafka-clj "0.1.0-SNAPSHOT"
  :description "Kafka client for Clojure."
  :url          "http://sna-projects.com/kafka/"
  :dependencies [[org.clojure/clojure	"1.4.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [com.googlecode.disruptor/disruptor "2.10"]
                 [com.netflix.curator/curator-framework "1.1.12"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :disable-deps-clean false
  :warn-on-reflection false
  :source-path "src"
  :aot :all
  :test-path "test")
