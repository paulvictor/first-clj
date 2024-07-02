(ns build
  (:require [clojure.tools.build.api :as b]))

(def build-folder "target")
(def jar-content (str build-folder "/classes"))

(def lib-name 'com.juspay/first-clj)
(def version "0.0.1")
(def app-name "first-clj-spark")
(def jar-file-name (format "%s/%s-%s.jar" build-folder (name lib-name) version))
(def uber-file-name (format "%s/%s-%s-standalone.jar" build-folder app-name version))

(def basis (b/create-basis {:project "deps.edn"}))

(defn clean [_]
  (b/delete {:path build-folder})
  (println (format "Build folder \"%s\" removed" build-folder)))

(defn jar [_]
                                        ;clean leftovers
  (clean nil)
                                        ;copy sources and resources
  (b/copy-dir {:src-dirs ["src" "resources"]
                :target-dir jar-content})

                                        ;create a pom.xml
  (b/write-pom
   {:class-dir jar-content
    :lib lib-name
    :version version
    :basis basis
    :src-dirs ["src"]})

                                        ;pack a jar
  (b/jar {:class-dir jar-content
          :jar-file jar-file-name
          :manifest {"mykey" "myvalue"}})
  (println (format "Jar created \"%s\"" jar-file-name))
  )

(defn uber [_]
                                        ;clean leftovers
  (clean nil)
                                        ;copy sources and resources
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir jar-content})

  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir jar-content})

                                        ;pack a jar
  (b/uber {:class-dir jar-content
           :uber-file uber-file-name
           :basis basis
           :main 'hello.core})
  (println (format "Jar created \"%s\"" jar-file-name))
  )
