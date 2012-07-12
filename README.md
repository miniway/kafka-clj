# Announcement
Copied and modifed from the legacy kafka-clj(https://github.com/kafka-dev/kafka/tree/master/clients/clojure)

# kafka-clj
This kafka-clj provides a producer and consumer that supports a basic fetch API as well as a managed sequence interface. 
* Better concurrency than Java API

* Multifetch is not supported yet.
* Consumer should be refactored

## Quick Start

Download and start [Kafka](http://sna-projects.com/kafka/quickstart.php). 

Pull dependencies with [Leiningen](https://github.com/technomancy/leiningen):

    $ lein deps
    $ lein jar 

## Usage

### Sending messages

    (with-open [p (producer {:broker.list "0:localhost:9092"})]
      (.produce p "topic1" "Message 1")
      (.produce p "topic1" ["Message 2" "Message 3"])
      (.produce p "topic1" "partition-key" ["Message 2" "Message 3"])
      )

### Sending Multi messages

    (with-open [p (producer {:zk.connect "localhost:2082"})]
      (.produce p [ ["topic1" nil "Messages 1"] 
                    ["topic2" "partition-key" "Message 2"]]))

### Message Partitioning
    (with-open [p (producer {:zk.connect "localhost:2082"}]
      (.produce p "test" "Partition-key1" "Message 1")
      (.produce p "test" "Partition-key2" "Message 2")

Following options are supported:
* :broker.list _string__ Comma seperated broker connection string. <broker-id>:<broker-host>:<broker-port>
* :zk.connect _string_ ZooKeeper Connection String
* :zk.connectiontimeout.ms _int_ ZooKeepr connection timeout Millis
* :zk.sessiontimeout.ms _int_ ZooKeepr session timeout Millis
* :broker.type _string_ [sync|async|batch] default : sync
* :partitioner _function_ Partitioner function whchi accepts two arguments (partition-key num-partition). Default is random partition.

### Simple consumer (will be deprecated)

    (with-open [c (consumer "localhost" 9092)]
      (let [offs (offsets c "test" 0 -1 10)]
        (.consume c "test" 0 (last offs) 1000000)))

### Consumer sequence (will be deprecated)

    (with-open [c (consumer {:broker.list "localhost:9092"})]
      (doseq [m (.consume-seq c "test" 0 {:blocking true})]
        (println m)))

Following options are supported:

* :blocking _boolean_ default false, sequence returns nil the first time fetch does not return new messages. If set to true, the sequence tries to fetch new messages :repeat-count times every :repeat-timeout milliseconds. 
* :repeat-count _int_ number of attempts to fetch new messages before terminating, default 10.
* :repeat-timeout _int_ wait time in milliseconds between fetch attempts, default 1000.
* :offset   _long_ initialized to highest offset if not provided.
* :max-size _int_  max result message size, default 1000000.

### Java Interop

   Properties props = new Properties();
   props.put("zk.connect", "localhost:2181");
   props.put("multithread", "true");
   kafka.types.Producer producer = kafka.kafka.newProducer(props);

   List<String> messages = new java.util.ArrayList<String>();
   messages.add("hello")
   messages.add("world")
   producer.produce("topic", messages);
           
   producer.close();
           
