(ns hello.core
  (:require
   [spark.core :as core]
   [spark.conf :as conf]
   [clojure.string :as string]
   [taoensso.timbre :as log])
  (:gen-class))

(def local-conf
  (-> (conf/spark-conf)
      (conf/master)
      (conf/app-name "first")))

(defn -main
  [& args]
  (core/with-context ctx local-conf
    (let [data (core/parallelize ctx (range 100))]
      (->> data
           (core/map inc)
           (core/reduce +)
           (clojure.pprint/pprint))))


;;   (let [local-context (core/spark-context local-conf)
;;         data (core/parallelize local-context (range 100))]
;;     (->> data
;;          (core/map inc)
;;          ;; (core/reduce +)
;;          (core/first)
;;          (clojure.pprint/pprint)))
  )
