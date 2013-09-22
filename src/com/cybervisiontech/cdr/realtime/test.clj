(ns com.cybervisiontech.cdr.realtime.test
  (:use [clojure.java.io])
  (:require [clojure.string])
  (:import (java.io FileOutputStream OutputStreamWriter BufferedWriter))
)

(import org.apache.hadoop.conf.Configuration)
(import org.apache.hadoop.fs.FileSystem)
(import org.apache.hadoop.fs.Path)
(import org.apache.hadoop.io.SequenceFile)
(import org.apache.hadoop.io.Text)
(import org.apache.hadoop.io.IOUtils)

(defn -main
  []
   (do ((println "Started") 

     (let [hadoop_configuration ((fn []
                                (let [conf (new Configuration)]
                                  (. conf set "fs.default.name" "hdfs://localhost:8020")
                                  conf)))
           hadoop_fs (FileSystem/get hadoop_configuration)
           stream (.create hadoop_fs (new Path "/user/hue/test2.data") true 128 1 1048576)] ; replication 1 (development)
        (
            (with-open [writer (-> stream
                         OutputStreamWriter.
                         BufferedWriter.)]
                (.write writer (str "TEST DATA TEST DATA TEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATATEST DATA"))
            )
        ))
     (println "Finished"))))
