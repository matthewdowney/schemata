(ns schemata.core-test
  (:refer-clojure :exclude [list resolve])
  (:require [clojure.test :refer :all]
            [schemata.core :refer :all]
            [schemata.core :as s]
            [clojure.java.io :as io]))

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

(deftest context-test
  ;; Test with no naming convention
  (let [ctx (local-context "schemata-test")]
    (test-with
      {:context ctx
       :spec "file-a.txt"
       :resolved (.getCanonicalPath (io/file "schemata-test" "file-a.txt"))
       :desc "local context / default naming"}
      {:context ctx
       :spec "file-b.txt"
       :resolved (.getCanonicalPath (io/file "schemata-test" "file-b.txt"))
       :desc "local context / default naming"}))

  ;; With naming convention like ticker/Bitstamp_2019-04-21.log
  (let [naming (path-convention
                 :type
                 (file-convention
                   (split-by "_" :name (utc :ts "yyyy-MM-dd"))
                   "log"))
        ctx (local-context "schemata-test" naming)]
    (test-with
      {:context ctx
       :spec {:ts 1555804800000 :type "ticker" :name "Bitstamp"}
       :resolved (.getCanonicalPath
                   (io/file
                     "schemata-test" "ticker" "Bitstamp_2019-04-21.log"))
       :desc "local context / custom naming"}
      {:context ctx
       :spec {:ts 1555891200000 :type "ticker" :name "Bitstamp"}
       :resolved (.getCanonicalPath
                   (io/file
                     "schemata-test" "ticker" "Bitstamp_2019-04-22.log"))
       :desc "local context / custom naming"})))

(comment
  (run-tests))
