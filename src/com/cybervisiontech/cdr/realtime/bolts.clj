(ns com.cybervisiontech.cdr.realtime.bolts
  (:use [clojure.java.io])
  (:require [clojure.string])
  (:require [backtype.storm [clojure
                             :refer [emit-bolt! defbolt ack! bolt]]]))


(def wrtr (writer "/tmp/test.txt"))
(def counter (atom 0N))

(defbolt dummy-hdfs-passer ["calling-party" "caller-party" "starting-time" "duration" "biller-phone" "route-enter" "route-left"] {:prepare true}
  [conf context collector]
  (bolt
   (execute [tuple]
        (.write wrtr (str @counter (clojure.string/join tuple) "\n"))
        (swap! counter inc)
          ;; For now we do nothing simply submit whole tuple
        (emit-bolt! collector tuple)
        (ack! collector tuple)
    )
   )
  )



; http://stackoverflow.com/questions/15540486/how-to-use-hadoop-fs-api-inside-storm-bolt-in-java

(defbolt hdfs-passer ["calling-party" "caller-party" "starting-time" "duration" "biller-phone" "route-enter" "route-left"] {:prepare true}
  [conf context collector]
  (bolt
   (execute [tuple]
        ; (.write wrtr (str @counter (clojure.string/join tuple) "\n"))
        (swap! counter inc)
          ;; For now we do nothing simply submit whole tuple
        (emit-bolt! collector tuple)
        (ack! collector tuple)
    )
   )
  )
