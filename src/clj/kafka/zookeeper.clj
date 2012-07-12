(ns kafka.zookeeper
  (:use [kafka util])
  (:use [clojure.tools.logging])
  (:require [clojure 
             [string :as str] 
             [stacktrace :as trace]])
  (:import [java.util UUID])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import (com.netflix.curator.retry RetryOneTime)
           (org.apache.zookeeper  KeeperException$NoNodeException)
           (com.netflix.curator.framework CuratorFramework CuratorFrameworkFactory)
           (com.netflix.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener))
  (:gen-class 
    :methods [#^{:static true} [getFramework [] com.netflix.curator.framework.CuratorFramework]])
  )

(def ZK (atom nil))
(def CALLBACKS (atom {}))

(defn uuid []
  (str (UUID/randomUUID)))

(defn register-callback [callback]
  (let [id (uuid)]
    (swap! CALLBACKS assoc id callback)
   id))

(defn unregister-callback [id]
   (swap! CALLBACKS dissoc id))
     
(defn get-children [& path]
  (.. @ZK (getChildren) (forPath (apply str path))))

(defn get-watched-children [& path]
  (.. @ZK (getChildren) (watched) (forPath (apply str path))))

(defn get-data [& path]
  (String. (.. @ZK (getData) (forPath (apply str path)))))

(defn get-watched-data [& path]
  (String. (.. @ZK (getData) (watched) (forPath (apply str path)))))

(def zk-keeper-states
  {Watcher$Event$KeeperState/Disconnected :disconnected
   Watcher$Event$KeeperState/SyncConnected :connected
   Watcher$Event$KeeperState/AuthFailed :auth-failed
   Watcher$Event$KeeperState/Expired :expired
  })   
       
(def zk-event-types            
  {Watcher$Event$EventType/None :none 
   Watcher$Event$EventType/NodeCreated :node-created
   Watcher$Event$EventType/NodeDeleted :node-deleted
   Watcher$Event$EventType/NodeDataChanged :node-data-changed
   Watcher$Event$EventType/NodeChildrenChanged :node-children-changed
  })

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT
   :sequential CreateMode/PERSISTENT_SEQUENTIAL})

(defn create-node
  ([^String path ^bytes data mode]
    (.. @ZK (create) (withMode (zk-create-modes mode)) (withACL ZooDefs$Ids/OPEN_ACL_UNSAFE) (forPath path data)))
  ([^String path ^bytes data]
    (create-node path data :persistent)))

; zk-paths
; consumer path : /consumers
; broker id path : /brokers/ids
; broker topic path : /brokers/topics
; broker topic partition path : /brokers/topics/{topic}/{broker-id}

(defn- default-watcher 
  [state type path]
  (when-not (= :connected state)
    (warn "Received event " state ":" type ":" path " with disconnected Zookeeper."))
  (when-not (= :none type)
    (doseq [callback (vals @CALLBACKS)]
      (callback type path)))
  )
                                       

(defn connect-zk!
  [conn-str & {:keys [connection-timeout session-timeout watcher]
               :or {connection-timeout 6000
                   session-timeout 6000
                   watcher default-watcher}}]
  (when conn-str 
    (let [zk (CuratorFrameworkFactory/newClient
                                      conn-str
                                      session-timeout
                                      connection-timeout (RetryOneTime. 10000))]
      (.. zk
        (getCuratorListenable)
        (addListener
         (reify CuratorListener
           (^void eventReceived [this ^CuratorFramework _fk ^CuratorEvent e]
             (when (= (.getType e) CuratorEventType/WATCHED)
               (let [^WatchedEvent event (.getWatchedEvent e)]
                 (watcher (zk-keeper-states (.getState event))
                          (zk-event-types (.getType event))
                          (.getPath event))))))))      
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
  [conn-str & {:keys [connection-timeout session-timeout watcher] 
               :or {connection-timeout 6000
                   session-timeout 6000
                   watcher default-watcher}
               :as opts}]
  (let [zk (connect-zk! conn-str 
                        :connection-timeout connection-timeout
                        :session-timeout session-timeout
                        :watcher watcher)]
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

; java interop
(defn -getFramework []
  @ZK)
