(ns schemata.core-test
  (:refer-clojure :exclude [list])
  (:require [clojure.test :refer :all]
            [schemata.core :refer :all]
            [schemata.core :as s]
            [clojure.java.io :as io])
  (:import (java.io File)))

(deftest utc-test
  (let [format "yyyy-MM-dd'T'HH:mm:ss.SSSS"
        ts 1555865080691
        formatted "2019-04-21T16:44:40.0691"

        {:keys [in out]} (utc :ts format)]
    (testing "Renders to a given UTC date format"
      (is (= (out {:ts ts}) formatted)))
    (testing "Parses a format string into a ts attr"
      (is (= (in formatted) {:ts ts})))))

(deftest split-by-test
  (let [{:keys [in out]} (split-by "_" :foo :bar (utc :ts "MMM"))]
    (testing "Renders a path part split by the given delimiter"
      (is (= (out {:foo "ABC" :bar "XYZ" :ts 1555865080691})
             "ABC_XYZ_Apr")))
    (testing "Parses a path part split by the given delimiter"
      (is (= (in "ABC_XYZ_Apr")
             {:foo "ABC" :bar "XYZ" :ts 7776000000})))))

(deftest file-convention-test
  (let [{:keys [in out]} (file-convention :foo :ext)]
    (testing "Renders a base name"
      (is (= (out {:foo "error" :ext "log.gz"}) "error.log.gz")))
    (testing "Parses a base name"
      (is (= (in "error.log.gz") {:foo "error" :ext "log.gz"})))))

(deftest path-convention-test
  (let [naming-convention (path-convention
                            (s/utc :ts "yyyy-MM-dd")
                            (file-convention :name :ext))]
    (testing "Renders a whole path"
      (is (= (spec->path
               naming-convention
               {:ts 1555865080691 :name "error" :ext "log.gz"})
             ["2019-04-21" "error.log.gz"])))
    (testing "Parses a whole path"
      (is (= (path->spec naming-convention ["2019-04-21" "error.log.gz"])
             {:ts 1555804800000 :name "error" :ext "log.gz"})))))

(def context-tests
  "Uses each :context to perform io on / between each of :file-specs. If
  :in-contexts is provided, checks that the :file-specs point to the paths
  specified by :in-contexts."
  {"default naming convention"
   {:context    (local-context)
    :file-specs [(-> (File/createTempFile "schemata-test" ".txt" (io/file "."))
                     .getCanonicalPath)
                 (-> (File/createTempFile "schemata-test" ".txt" (io/file "."))
                     .getCanonicalPath)]}
   "custom naming convention"
   ;; E.g. ticker/Bitstamp_2019-04-21.log
   (let [naming (path-convention
                  :type
                  (file-convention
                    (split-by "_" :name (utc :ts "yyyy-MM-dd"))
                    "log"))]

     {:context        (local-context "schemata-test" naming)
      :file-specs     [{:ts 1555804800000 :type "ticker" :name "Bitstamp"}
                       {:ts 1555891200000 :type "ticker" :name "Bitstamp"}]
      :in-contexts    [(.getCanonicalPath
                         (io/file "schemata-test/ticker/Bitstamp_2019-04-21.log"))
                       (.getCanonicalPath
                         (io/file "schemata-test/ticker/Bitstamp_2019-04-22.log"))]
      :test-discover? true
      :clean-up       (fn []
                        (.delete (io/file "schemata-test/ticker"))
                        (.delete (io/file "schemata-test")))})})

(deftest local-context-test
  (doseq [[test-desc {:keys [context file-specs in-contexts test-discover? clean-up]}]
          context-tests]
    (let [[tmp-1 tmp-2] file-specs]
      (testing (format "Local context IO works with %s." test-desc)
        (println (format "\ntesting local context IO with %s" test-desc))
        (testing "(read/write)"
          (println "writing to" (in-context context tmp-1))
          (spit (io context tmp-1) "foo\n")
          (spit (io context tmp-1) "bar\n" :append true)
          (println "reading from" (in-context context tmp-1))
          (is (= (slurp (io context tmp-1)) "foo\nbar\n")))
        (testing "(copy)"
          (println "copying" (in-context context tmp-1) "to" (in-context context tmp-2))
          (copy tmp-1 tmp-2 context context)
          (is (= (slurp (io context tmp-1))
                 (slurp (io context tmp-2))
                 "foo\nbar\n")))
        (when (not-empty in-contexts)
          (testing "(path rendering)"
            (is (= (in-context context tmp-1) (first in-contexts)))
            (is (= (in-context context tmp-2) (second in-contexts)))))
        (when test-discover?
          (testing "(discovery)"
            (is (= (sort-by :ts (list context)) file-specs))))
        (testing "(delete)"
          (println "deleting" (in-context context tmp-1))
          (delete context tmp-1)
          (println "deleting" (in-context context tmp-2))
          (delete context tmp-2)
          (is (not (.isFile (io/file (in-context context tmp-1)))))
          (is (not (.isFile (io/file (in-context context tmp-2))))))
        (when clean-up
          (clean-up))))))

(comment
  (run-tests))
