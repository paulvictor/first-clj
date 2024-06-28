(ns hello.core
  (:require
   [clojure.string :as string]
   [sparkling.conf :as conf]
   [sparkling.core :as spark]
   [taoensso.timbre :as log]
;;    [sparkling.core as spark]
;;    [org.apache.spark :as spark]
;;    [org.apache.spark.api.java :as jspark]
;;    [scala.Tuple2 :as Tuple2]
   )
  (:gen-class))

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "sparkling-example")))

(def sc (spark/spark-context c))

(def data (spark/parallelize sc ["a" "b" "c" "d" "e"]))


(defn -main
  [& args]
  (log/debug (str (clojure.string/upper-case "First")))
  (log/debug (str "Hello from " (string/upper-case "clojure!!!")))
  (spark/first data))
