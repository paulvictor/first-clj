(ns hello.core
  (:require
   [spark.core :as core]
   [spark.conf :as conf]
   [clojure.string :as string]
   [taoensso.timbre :as log])
  (:gen-class))

(def local-context
  (-> (conf/spark-conf)
      (conf/master)
      (conf/app-name "first")
      ))


(defn -main
  [& args]
  (println local-context))
