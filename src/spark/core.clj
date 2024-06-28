(ns spark.core
  (:require
   [spark.conf :as conf]
   [spark.function :refer [flat-map-function
                           flat-map-function2
                           function
                           function2
                           function3
                           pair-function
                           pair-flat-map-function
                           void-function]]
   [spark.utils :as u])
  (:import
   [scala Tuple2]
   [java.util Comparator ArrayList]
   [org.apache.spark.api.java JavaSparkContext StorageLevels
    JavaRDD JavaPairRDD JavaDoubleRDD]
   [org.apache.spark HashPartitioner Partitioner]
   [org.apache.spark.rdd PartitionwiseSampledRDD PartitionerAwareUnionRDD]
   [scala.collection JavaConverters]
   [scala.reflect ClassTag$]))

(def stop
  (memfn #^JavaSparkContext stop))

(defn spark-context
  "Creates a spark context that loads settings from given configuration object
   or system properties"
  [conf]
  (log/debug "JavaSparkContext" (to-string conf))
  (JavaSparkContext. conf))

(defmacro with-context
  [context-sym conf & body]
  `(let [~context-sym (spark-context ~conf)]
     (try
       ~@body
       (finally (stop ~context-sym)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; RDD construction
;;
;; Functions for constructing new RDDs
;;
(defn text-file
  "Reads a text file from HDFS, a local file system (available on all nodes),
  or any Hadoop-supported file system URI, and returns it as an `JavaRDD` of Strings."
  ([spark-context filename]
  (.textFile spark-context filename))
  ([spark-context filename min-partitions]
   (.textFile spark-context filename min-partitions)))

(defn whole-text-files
  "Read a directory of text files from HDFS, a local file system (available on all nodes),
  or any Hadoop-supported file system URI. Each file is read as a single record and returned
  in a key-value pair, where the key is the path of each file, the value is the content of each file."
  ([spark-context filename min-partitions]
   (.wholeTextFiles spark-context filename min-partitions))
  ([spark-context filename]
   (.wholeTextFiles spark-context filename)))

(defn into-rdd
  "Distributes a local collection to form/return an RDD"
  ([spark-context lst]
   (.parallelize spark-context lst))
  ([spark-context num-slices lst]
   (.parallelize spark-context lst num-slices)
   num-slices))

(defn into-pair-rdd
  "Distributes a local collection to form/return an RDD"
  ([spark-context lst]  (.parallelizePairs spark-context lst))
  ([spark-context num-slices lst]  (.parallelizePairs spark-context lst num-slices) num-slices))

(def parallelize into-rdd)
(def parallelize-pairs into-pair-rdd)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Transformations
;; Functions for transforming RDDs
;;

;; functions on one rdd
(defn map
  "Returns a new RDD formed by passing each element of the source through the function `f`."
  [f rdd]
  (-> (.map rdd (function f))
      (u/set-auto-name (u/unmangle-fn f))))

(defn map-to-pair
  "Returns a new `JavaPairRDD` of (K, V) pairs by applying `f` to all elements of `rdd`."
  [f rdd]
  (-> (.mapToPair rdd (pair-function f))
      (u/set-auto-name (u/unmangle-fn f))))

(defn map-values [f rdd]
  "Returns a new `JavaPairRDD` of (K,V) pairs by applying `f` to all values in the
  pair-rdd `rdd`"
  (-> (.mapValues rdd (function f))
      (u/set-auto-name (u/unmangle-fn f))))

(defn reduce
  "Aggregates the elements of `rdd` using the function `f` (which takes two arguments
  and returns one). The function should be commutative and associative so that it can be
  computed correctly in parallel."
  [f rdd]
  (u/set-auto-name (.reduce rdd (function2 f)) (u/unmangle-fn f)))

(defn values
  "Returns the values of a JavaPairRDD"
  [rdd]
  (u/set-auto-name (.values rdd)))

(defn flat-map
  "Similar to `map`, but each input item can be mapped to 0 or more output items (so the
   function `f` should return a collection rather than a single item)"
  [f rdd]
  (u/set-auto-name
    (.flatMap rdd (flat-map-function f))
    (u/unmangle-fn f)))

(defn flat-map-to-pair
  "Returns a new `JavaPairRDD` by first applying `f` to all elements of `rdd`, and then flattening
  the results."
  [f rdd]
  (u/set-auto-name
    (.flatMapToPair rdd (pair-flat-map-function f))
    (u/unmangle-fn f)))

(defn flat-map-values
  "Returns a `JavaPairRDD` by applying `f` to all values of `rdd`, and then
  flattening the results"
  [f rdd]
  (u/set-auto-name
    (.flatMapValues rdd (flat-map-function f))
    (u/unmangle-fn f)))

(defn map-partition
  "Similar to `map`, but runs separately on each partition (block) of the `rdd`, so function `f`
  must be of type Iterator<T> => Iterable<U>.
  https://issues.apache.org/jira/browse/SPARK-3369"
  [f rdd]
  (u/set-auto-name
    (.mapPartitions rdd (flat-map-function f))
    (u/unmangle-fn f)))


(defn map-partitions-to-pair
  "Similar to `map`, but runs separately on each partition (block) of the `rdd`, so function `f`
  must be of type Iterator<T> => Iterable<U>.
  https://issues.apache.org/jira/browse/SPARK-3369"
  [f preserve-partitioning? rdd]
  (u/set-auto-name
    (.mapPartitionsToPair rdd (pair-flat-map-function f) (u/truthy? preserve-partitioning?))
    (u/unmangle-fn f)
    preserve-partitioning?))

(defn map-partition-with-index
  "Similar to `map-partition` but function `f` is of type (Int, Iterator<T>) => Iterator<U> where
  `i` represents the index of partition."
  [f rdd]
  (u/set-auto-name
    (.mapPartitionsWithIndex rdd (function2 f) true)
    (u/unmangle-fn f)))



(defn filter
  "Returns a new RDD containing only the elements of `rdd` that satisfy a predicate `f`."
  [f rdd]
  (u/set-auto-name
    (.filter rdd (function (ftruthy? f)))
    (u/unmangle-fn f)))





(defn foreach
  "Applies the function `f` to all elements of `rdd`."
  [f rdd]
  (.foreach rdd (void-function f)))

(defn aggregate
  "Aggregates the elements of each partition, and then the results for all the partitions,
   using a given combine function and a neutral 'zero value'."
  [item-to-seq-fn combine-seqs-fn zero-value rdd]
  (.aggregate rdd zero-value (function2 item-to-seq-fn) (function2 combine-seqs-fn)))

(defn fold
  "Aggregates the elements of each partition, and then the results for all the partitions,
  using a given associative function and a neutral 'zero value'"
  [f zero-value rdd]
  (.fold rdd zero-value (function2 f)))

(defn tuple-by [f]
  (fn tuple-by-fn [x]
    (tuple (f x) x)))

(defn key-by
  "Creates tuples of the elements in this RDD by applying `f`."
  [f ^JavaRDD rdd]
  (u/set-auto-name
    (map-to-pair (tuple-by f) rdd)
    (u/unmangle-fn f)))

(defn keys
  "Return an RDD with the keys of each tuple."
  [^JavaPairRDD rdd]
  (u/set-auto-name
    (.keys rdd)))


(defn reduce-by-key
  "When called on an `rdd` of (K, V) pairs, returns an RDD of (K, V) pairs
  where the values for each key are aggregated using the given reduce function `f`."
  [f rdd]
  (u/set-auto-name
    (.reduceByKey rdd (function2 f))
    (u/unmangle-fn f)))

(defn group-by
  "Returns an RDD of items grouped by the return value of function `f`."
  ([f rdd]
   (u/set-auto-name
     (.groupBy rdd (function f))
     (u/unmangle-fn f)))
  ([f n rdd]
   (u/set-auto-name
     (.groupBy rdd (function f) n)
     (u/unmangle-fn f)
     n)))

(defn group-by-key
  "Groups the values for each key in `rdd` into a single sequence."
  ([rdd]
   (u/set-auto-name
     (.groupByKey rdd)))
  ([n rdd]
   (u/set-auto-name
     (.groupByKey rdd n)
     n)))

(defn combine-by-key
  "Combines the elements for each key using a custom set of aggregation functions.
  Turns an RDD of (K, V) pairs into a result of type (K, C), for a 'combined type' C.
  Note that V and C can be different -- for example, one might group an RDD of type
  (Int, Int) into an RDD of type (Int, List[Int]).
  Users must provide three functions:
  -- seq-fn, which turns a V into a C (e.g., creates a one-element list)
  -- conj-fn, to merge a V into a C (e.g., adds it to the end of a list)
  -- merge-fn, to combine two C's into a single one."
  ([seq-fn conj-fn merge-fn rdd]
   (u/set-auto-name
     (.combineByKey rdd
                    (function seq-fn)
                    (function2 conj-fn)
                    (function2 merge-fn))
     (u/unmangle-fn seq-fn)
     (u/unmangle-fn conj-fn)
     (u/unmangle-fn merge-fn)))
  ([seq-fn conj-fn merge-fn n rdd]
   (u/set-auto-name
     (.combineByKey rdd
                    (function seq-fn)
                    (function2 conj-fn)
                    (function2 merge-fn)
                    n)
     (u/unmangle-fn seq-fn)
     (u/unmangle-fn conj-fn)
     (u/unmangle-fn merge-fn)
     n
     )))

(defn- wrap-comparator [f]
  "Turn a plain function into a Comparator instance, or pass a Comparator unchanged"
  (if (instance? Comparator f) f (comparator f)))

(defn sort-by-key
  "When called on `rdd` of (K, V) pairs where K implements ordered, returns a dataset of
   (K, V) pairs sorted by keys in ascending or descending order, as specified by the boolean
   ascending argument."
  ([rdd]
   (u/set-auto-name
     (sort-by-key compare true rdd)))
  ([x rdd]
    ;; RDD has a .sortByKey signature with just a Boolean arg, but it doesn't
    ;; seem to work when I try it, bool is ignored.
   (if (instance? Boolean x)
     (sort-by-key compare x rdd)
     (sort-by-key x true rdd)))
  ([compare-fn asc? rdd]
   (u/set-auto-name
     (.sortByKey rdd
                 (wrap-comparator compare-fn)
                 (u/truthy? asc?))
     (u/unmangle-fn compare-fn)
     asc?
     )))

(defn max
  "Return the maximum value in `rdd` in the ordering defined by `compare-fn`"
  [compare-fn rdd]
  (u/set-auto-name
   (.max rdd (wrap-comparator compare-fn))
   (u/unmangle-fn compare-fn)))

(defn min
  "Return the minimum value in `rdd` in the ordering defined by `compare-fn`"
  [compare-fn rdd]
  (u/set-auto-name
   (.min rdd (wrap-comparator compare-fn))
   (u/unmangle-fn compare-fn)))

(defn sample
  "Returns a `fraction` sample of `rdd`, with or without replacement,
  using a given random number generator `seed`."
  [with-replacement? fraction seed rdd]
  (u/set-auto-name
    (.sample rdd with-replacement? fraction seed)
    with-replacement? fraction seed))

(defn coalesce
  "Decrease the number of partitions in `rdd` to `n`.
  Useful for running operations more efficiently after filtering down a large dataset."
  ([n rdd]
   (u/set-auto-name
     (.coalesce rdd n)
     n))
  ([n shuffle? rdd]
   (u/set-auto-name
     (.coalesce rdd n shuffle?)
     n shuffle?)))


(defn coalesce-max
  "Decrease the number of partitions in `rdd` to `n`.
  Useful for running operations more efficiently after filtering down a large dataset."
  ([n rdd]
   (u/set-auto-name
     (.coalesce rdd (clojure.core/min n (count-partitions rdd))) n))
  ([n shuffle? rdd]
   (u/set-auto-name
     (.coalesce rdd (clojure.core/min n (count-partitions rdd)) shuffle?) n shuffle?)))


(defn zip-with-index
  "Zips this RDD with its element indices, creating an RDD of tuples of (item, index)"
  [rdd]
  (u/set-auto-name (.zipWithIndex rdd)))


(defn zip-with-unique-id
  "Zips this RDD with generated unique Long ids, creating an RDD of tuples of (item, uniqueId)"
  [rdd]
  (u/set-auto-name (.zipWithUniqueId rdd)))


;; functions on multiple rdds

(defn cogroup
  ([^JavaPairRDD rdd ^JavaPairRDD other]
   (u/set-auto-name
     (.cogroup rdd other)))
  ([^JavaPairRDD rdd ^JavaPairRDD other1 ^JavaPairRDD other2]
   (u/set-auto-name
     (.cogroup rdd
               other1
               other2)))
  ([^JavaPairRDD rdd ^JavaPairRDD other1 ^JavaPairRDD other2 ^JavaPairRDD other3]
   (u/set-auto-name
     (.cogroup rdd
               other1
               other2
               other3))))

(defn union
  "Build the union of two or more RDDs"
  ([rdd1 rdd2]
   (u/set-auto-name (.union rdd1 rdd2)))
  ([rdd1 rdd2 & rdds]
   (u/set-auto-name (.union (JavaSparkContext/fromSparkContext (.context rdd1))
                            (into-array JavaRDD (concat [rdd1 rdd2] rdds))))))

(defn join
  "When called on `rdd` of type (K, V) and (K, W), returns a dataset of
  (K, (V, W)) pairs with all pairs of elements for each key."
  [rdd other]
  (u/set-auto-name
    (.join rdd other)))

(defn left-outer-join
  "Performs a left outer join of `rdd` and `other`. For each element (K, V)
   in the RDD, the resulting RDD will either contain all pairs (K, (V, W)) for W in other,
   or the pair (K, (V, nil)) if no elements in other have key K."
  [rdd other]
  (u/set-auto-name
    (.leftOuterJoin rdd other)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; RDD management


(def cache
  "Persists `rdd` with the default storage level (`MEMORY_ONLY`)."
  (memfn cache))

(defn uncache
  "Marks `rdd` as non-persistent (removes all blocks for it from memory and disk).  If `blocking?` is true, block until the operation is complete."
  ([blocking? rdd] (.unpersist rdd blocking?))
  ([rdd] (uncache false rdd)))

(defn storage-level!
  "Sets the storage level of `rdd` to persist its values across operations
  after the first time it is computed. storage levels are available in the `STORAGE-LEVELS' map.
  This can only be used to assign a new storage level if the RDD does not have a storage level set already."
  [storage-level rdd]
  (.persist rdd storage-level))

;; Add documentation, make sure you have checkpoint-directory set. How?
(defn checkpoint [rdd]
  (.checkpoint rdd))

(defn rdd-name
  ([name rdd]
   (.setName rdd name))
  ([rdd]
   (.name rdd)))
