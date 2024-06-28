(ns spark.conf
  (:import
   [org.apache.spark SparkConf]
   [org.apache.spark.api.java JavaSparkContext]))

(defn master
  ([conf]
   (master conf "local[*]"))
  ([conf master]
   (.setMaster conf master)))

(defn app-name
  [conf name]
  (.setAppName conf name))

(defn spark-conf
  []
  (-> (SparkConf.)
      (.set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")))

(defn set
  ([conf key val]
   (.set conf key val))
  ([conf amap]
   (loop [c conf
          aseq (seq amap)]
     (if aseq
       (let [[k v] (first aseq)
             c (set c k v)]
         (recur c (next aseq)))
       c))))

(defn set-if-missing
  [conf key val]
  (.setIfMissing conf key val))

(defn spark-home
  [conf home]
  (.setSparkHome conf home))

(defn to-string
  [conf]
  (.toDebugString conf))
