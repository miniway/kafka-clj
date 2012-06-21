(ns kafka.zookeeper
  (:use [kafka util])
  (:use [clojure.tools.logging])
  (:require [clojure 
             [string :as str] 
             [stacktrace :as trace]])
  (:import (com.netflix.curator.retry RetryOneTime)
           (org.apache.zookeeper  KeeperException$NoNodeException)
           (com.netflix.curator.framework CuratorFramework CuratorFrameworkFactory)
           (com.netflix.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener)))

(def ZK (atom nil))


(defn get-children [& path]
  (.. @ZK (getChildren) (forPath (apply str path))))

(defn get-data [& path]
  (String. (.. @ZK (getData) (forPath (apply str path)))))

; zk-paths
; consumer path : /consumers
; broker id path : /brokers/ids
; broker topic path : /brokers/topics
; broker topic partition path : /brokers/topics/{topic}/{broker-id}

(defn connect-zk!
  [conn-str & {:keys [connection-timeout session-timeout]
               :or {connection-timeout 6000
                   session-timeout 6000}}]
  (when conn-str 
    (let [zk (CuratorFrameworkFactory/newClient
                                      conn-str
                                      session-timeout
                                      connection-timeout (RetryOneTime. 10000))]
      (.. zk
        (getUnhandledErrorListenable)
        (addListener
         (reify UnhandledErrorListener
           (unhandledError [this msg error]
             (if (or (exception-cause? InterruptedException error)
                     (exception-cause? java.nio.channels.ClosedByInterruptException error))
               (do (warn error "Zookeeper exception " msg)
                   (let [to-throw (InterruptedException.)]
                     (.initCause to-throw error)
                     (throw to-throw)
                     ))
               (do (error error "Unrecoverable Zookeeper error " msg)
                   (halt-process! 1 "Unrecoverable Zookeeper error")))
             ))))
      (.start zk) 
      (reset! ZK zk)
    ))
 )

(defn get-zk-brokers
  [conn-str & {:keys [connection-timeout session-timeout] 
               :or {connection-timeout 6000
                   session-timeout 6000}
               :as opts}]
  (let [zk (connect-zk! conn-str 
                        :connection-timeout connection-timeout
                        :session-timeout session-timeout)]
    (when zk (into {} 
          (for [broker-id (get-children "/brokers/ids")] 
            {(Integer/parseInt broker-id) 
             (rest (str/split (get-data "/brokers/ids/" broker-id) #":"))} 
        ))))
  )

; { topic : [ [ broker-id partition ] ... ]
(defn get-zk-topic-partitions  
  []
  (when @ZK
    (apply merge-with concat
     (for [topic (get-children "/brokers/topics")
           broker-id (get-children "/brokers/topics/" topic)
           partition (range (Integer/parseInt (get-data "/brokers/topics/" topic "/" broker-id)))]
       { topic [[(Integer/parseInt broker-id) partition]] })
  )))


(defn disconnect-zk!
  []
  (when @ZK 
     (.close @ZK))
  (reset! ZK nil))
