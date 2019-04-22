(ns
  ^{:doc "Core `NamingConvention` and `Context` protocols + default
          implementations and helpers."
    :author "Matthew Downey"}
  schemata.core
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:refer-clojure :exclude [list resolve])
  (:import (java.io File)
           (java.text SimpleDateFormat)
           (java.util TimeZone Date UUID)
           (clojure.lang IDeref ExceptionInfo)))

(set! *warn-on-reflection* true)

;;; Grammar for naming things

(defprotocol NamingConvention
  "A bidirectional mapping between a file specification and a file path.

  The file specification is anything sufficient to precisely describe a file
  and the path is the thing we need to store or retrieve the file's data within
  the storage context.

  The spec is usually a map and the path is normally a vector of path parts --
  e.g. ['user' 'data' 'my-file.log'] -- that can be assembled in different
  contexts."
  (spec->path [this spec])
  (path->spec [this path]))

(defrecord
  ^{:doc "The default naming convention. Works with a coll of {:in ... :out ...}
          maps whose functions read/render a part of the path. Renders paths as
          a coll of path part strings."}
  PathNamingConvention [path-part-functions]

  NamingConvention
  (spec->path [this spec]
    (mapv #((:out %) spec) path-part-functions))

  (path->spec [this path]
    ;; Reverse so that we parse from end to beginning (maybe there's a root to
    ;; the path that's not part of the naming convention)
    (->> (mapv #((:in %1) %2) (reverse path-part-functions) (reverse path))
         reverse
         (apply merge))))

(def ^:private format->cached-formatter
  (atom {}))

(defmacro ^:private thread-local
  "For easy thread-safe re-use of mutable objects."
  [init-form]
  `(let [factory# (proxy [ThreadLocal] [] (initialValue [] ~init-form))]
     (reify IDeref
       (deref [_]
         (.get ^ThreadLocal factory#)))))

(defn- utc-date-format
  "A thread-local, UTC SimpleDateFormat for the given format string."
  [format-str]
  (thread-local
    (let [fmt (SimpleDateFormat. format-str)]
      (.setTimeZone fmt (TimeZone/getTimeZone "UTC"))
      fmt)))

(defn utc
  "A bidirectional UTC date formatter/parser, that formats the contents of
  `attribute` as a string using `format`, or parses the string into `attribute`
  on the way back in.

  Only as precise as the format string allows for.

  E.g.
    ((:out (utc :ts \"MMM\")) {:ts (System/currentTimeMillis)})
    ; => \"Apr\"

    ((:in (utc :ts \"MMM\")) \"Apr\")
    ; => {:ts 7776000000}"
  [attribute format]
  (let [sdf (-> format->cached-formatter
                (swap! update format #(or % (utc-date-format format)))
                (get format))]
    {:in  (fn [s]
            {attribute (.getTime (.parse ^SimpleDateFormat @sdf s))})
     :out (fn [attributes]
            (let [ts (get attributes attribute)]
              (.format ^SimpleDateFormat @sdf (if (instance? Date ts) ts (Date. ^Long ts)))))}))

(defn- compile-convention-part
  "Leave {:in ... :out ... } maps unaltered, coerce strings/keywords."
  [part]
  (cond
    (map? part) part
    (keyword? part) {:out part
                     :in  #(->{part %})}
    (string? part) {:out (constantly part)
                    :in  (constantly {})}
    :else
    (throw (ex-info "Invalid naming convention!" {:convention part}))))

(defn- char-split
  [s c]
  (assert (= 1 (count (str c))) "Given delimiter is 1-length.")
  (let [c-char (if (instance? Character c) c (.charAt (str c) 0))
        c-str  (str c-char)
        xform (comp
                (partition-by #(= c-char %))
                (map string/join)
                (filter #(not= c-str %)))]
    (into [] xform s)))

(defn split-by
  "A part of a path that's split by the character / single character string `d`.
  (Obviously, that means that none of the `parts` may contain `d`.)

  For example, a path part called 'name_yyyy-MM-dd' is a :name attribute and a
  timestamp split by the '_' character, so we'd define it as:
    (split-by \"_\" :name (utc :ts \"yyyy-MM-dd\"))"
  [d & parts]
  (assert (= 1 (count (str d))) "The delimiter is a single character string.")
  (let [parts' (map compile-convention-part parts)]
    {:in  (fn [s]
            (apply merge (map #((:in %1) %2) parts' (char-split s d))))
     :out (fn [attrs]
            (->> parts'
                 (sequence (comp (map #((:out %) attrs)) (filter some?)))
                 (string/join d)))}))

(defn file-convention
  "The naming convention for a file's base name & its extension.

  Everything after the first '.' in the last part of the path is considered the
  extension. E.g. foo.log.gz has the extension log.gz."
  [base-name-convention ext-convention]
  (let [base' (compile-convention-part base-name-convention)
        ext' (compile-convention-part ext-convention)]
    {:in  (fn [s]
            (let [[base-str & ext] (string/split s #"\.")
                  ext (string/join "." ext)]
              (merge ((:in base') base-str) ((:in ext') ext))))
     :out (fn [attrs]
            (str ((:out base') attrs) "." ((:out ext') attrs)))}))

;; TODO: Some way to partially render a path convention to use as a search
;;       prefix. Something that lets spec->path use a partial spec and return
;;       either ["path" "parts" "x.log"] on success or
;;       {::partial ["path" "parts" {::partial "x"}]}
(defn path-convention
  "Create a naming convention from different path parts, the last of which
  is the file's base name & extension.

  Each part is a map with attributes :in and :out, where :in takes a path part
  string and returns a map of attributes from that part and :out takes an
  attribute map and produces a string.

  Keywords are 'getters/setters' for their attributes, and strings are treated
  as constants."
  [& parts]
  (->PathNamingConvention (mapv compile-convention-part parts)))

;;; Abstractions that give context to names

(defprotocol Context
  "IOFactory + things that are missing if we want to be able to discover & move
  files."
  (io [this spec]
    "Get an IOFactory for the data described by `spec`.")
  (resolve [this spec]
    "Resolve the spec by rendering its path in a way appropriate for the
    context. Useful for REPL exploration.")
  (delete [this] [this spec]
    "In the one-arg version, delete the context root after checking that
    there's nothing under it. In the two-arg version, delete the file
    described by `spec`.")
  (info [this spec]
    "Whatever information is available. Recommended to include
    {:size          <n bytes>
     :last-modified <ms timestamp>}")
  (list [this] [this opts]
    "List the file specifications that can be discovered within this context.

    Default implementation accepts a `strict?` option that defaults to false,
    but if true, breaks upon encountering a file that doesn't match the context's
    naming convention."))

(defn- ^File ->file [root naming-convention spec]
  (let [path-parts (spec->path naming-convention spec)]
    (if root
      (apply io/file root path-parts)
      (apply io/file path-parts))))

(defrecord LocalContext [root naming-convention]
  Context
  (io [this spec]
    (let [f (->file root naming-convention spec)]
      (io/make-parents f)
      f))

  (resolve [this spec]
    (let [f (->file root naming-convention spec)]
      (.getCanonicalPath f)))

  (delete [this]
    (let [things-under-root (try
                              (into [] (list this {:strict? true}))
                              (catch Throwable t t))]
      ;; When there are not file items under the root, recursively delete
      ;; the directory structure
      (cond
        (instance? Exception things-under-root)
        (-> "Couldn't verify that context was empty before deletion."
            (ex-info {} #_things-under-root)
            (throw))

        (not-empty things-under-root)
        (-> "Cannot delete context; it's not empty."
            (ex-info {:contains things-under-root})
            (throw))

        :else
        (doseq [dir (tree-seq #(.isDirectory ^File %)
                              #(.listFiles ^File %)
                              (io/file root))]
          (.delete ^File dir)))))

  (delete [this spec]
    (.delete (->file root naming-convention spec)))

  (info [this spec]
    (let [f (->file root naming-convention spec)]
      {:size (.length f)
       :last-modified (.lastModified f)}))

  (list [this]
    (list this {}))

  (list [this {:keys [strict?]}]
    (let [root (.getPath (io/file root))
          file->path #(char-split (.getPath ^File %) File/separator)]
      (->>
        ;; Depth-first search of all files under the root directory
        (tree-seq #(.isDirectory ^File %) #(.listFiles ^File %) (io/file root))
        ;; Keep only the files (no partial paths)
        (filter #(.isFile ^File %))
        ;; Read 'em in
        (map #(try
                (path->spec naming-convention (file->path %))
                (catch Exception e (when strict? (throw e)))))
        (filter some?)))))

(defn- path-relative-to [root path]
  (str
    (.relativize
      (.toPath (io/file root))
      (.toPath (io/file path)))))

(defn local-context
  "A `Context` that discovers files & performs IO on the local filesystem.

  If no `naming-convention` argument is given, takes plain string paths (e.g.
  root/foo/bar/baz.log).

  The `root` is a relative path to the directory under which the given
  `naming-convention` applies."
  ([root]
   (local-context
     root
     (reify NamingConvention
       (spec->path [_ path]
         (let [parts (char-split path File/separator)]
           (if (.startsWith ^String path File/separator)
             (update parts 0 #(str File/separator %))
             parts)))
       (path->spec [_ path]
         (path-relative-to (or root ".") (string/join File/separator path))))))
  ([root naming-convention]
   (->LocalContext root naming-convention)))

;;; General IO utils

(defmulti copy
  "A helper to copy from one `Context` to another. Allows specific
  implementations by `Context` type, but by default copies from one's
  (io/input-stream) to the other's (io/output-stream).

  f: (from-spec, to-spec, from-context, to-context) => side effects"
  (fn [from-spec to-spec from-context to-context]
    [(type from-context) (type to-context)]))

;; io/copy has in implementation just for File objects
(defmethod copy [LocalContext LocalContext]
  [from-spec to-spec from-context to-context]
  (io/copy (io from-context from-spec) (io to-context to-spec)))

(defmethod copy :default
  [from-spec to-spec from-context to-context]
  (with-open [in-stream (io/input-stream (io from-context from-spec))
              out-stream (io/output-stream (io to-context to-spec))]
    (io/copy in-stream out-stream)))

;;; A helper for tests, put here so that it can also be invoked by
;;; other libraries

(defn test-with
  "Test all of the `Context` methods on two files of the same or different
  contexts. Must be run within a `clojure.test/deftest.

  Each file is a map composed of
    - :context    A Context implementation.
    - :spec       A file spec for the :context.
    - :resolved   The expected response for (resolve :context :spec).
    - :desc       A quick description, e.g. 'default local context'.

  Note that the given :contexts must be empty (use a test directory)."
  [{:keys [context spec resolved desc] :as file-a} file-b]
  (println "\nTesting context implementations with IO...")
  (assert (= #{:context :spec :resolved :desc} (set (keys file-a))))
  (assert (= #{:context :spec :resolved :desc} (set (keys file-b))))
  (assert (not= file-a file-b) "The given files are distinct.")

  (testing "The given test contexts are empty."
    (is (empty? (list (:context file-a) {:strict? true})))
    (is (empty? (list (:context file-b) {:strict? true}))))

  (testing "The given specs resolve correctly."
    (is (= (resolve (:context file-a) (:spec file-a)) (:resolved file-a)))
    (is (= (resolve (:context file-b) (:spec file-b)) (:resolved file-b))))

  (testing "One-by-one tests for each of the given test files."
    (doseq [f [file-a file-b]]
      (let [io' (io (:context f) (:spec f))
            path (resolve (:context f) (:spec f))]
        (testing (format "Write / read with %s" (:desc f))
          (println (format "Writing to %s..." path))
          (spit io' "foo\n")
          (is (= (slurp io') "foo\n")))

        (testing (format "Overwrite / read with %s" (:desc f))
          (println (format "Overwriting %s..." path))
          (spit io' "bar\n")
          (is (= (slurp io') "bar\n")))

        (testing (format "Append / read with %s" (:desc f))
          (println (format "Appending to %s..." path))
          (spit io' "baz\n" :append true)
          (is (= (slurp io') "bar\nbaz\n")))

        (testing (format "File info with %s" (:desc f))
          (let [{:keys [last-modified size] :as init-info} (info (:context f) (:spec f))]
            (println "Got file info of" init-info)
            (when last-modified
              (is (> 5000 (- (System/currentTimeMillis) last-modified))
                  "Last modified in the last 5 seconds."))

            (when size
              (is (= size 8) "8 bytes written.")))))))

  (testing "File discovery"
    (let [;; Trim the contents of discovered file specs to keep only
          ;; the keys in the original spec: it's okay if discover
          ;; includes extra keys, but not okay if it leaves out keys.
          trim-discovered
          (fn [discovered {:keys [spec]}]
            (if (map? spec)
              (into #{} (map #(select-keys % (keys spec))) discovered)
              (into #{} discovered)))]
      (if (= (:context file-a) (:context file-b))
        (let [discovered (trim-discovered (list (:context file-a)) file-a)]
          (println "Discovered files" discovered)
          (is (= discovered (set (map :spec [file-a file-b])))))
        (let [discovered-a (trim-discovered (list (:context file-a)) file-a)
              discovered-b (trim-discovered (list (:context file-b)) file-b)]
          (println "Discovered files" discovered-a)
          (is (= discovered-a #{(:spec file-a)}))
          (println "Discovered files" discovered-b)
          (is (= discovered-b #{(:spec file-b)}))))))

  (testing "Copying"
    ;; Test copying in both directions
    (doseq [[from to] [[file-a file-b] [file-b file-a]]]
      (testing (format "(from %s to %s)" (:desc from) (:desc to))
        (let [unique-string (str (UUID/randomUUID))]
          (println (format "Writing %s to %s, then copying to %s..."
                           unique-string
                           (resolve (:context from) (:spec from))
                           (resolve (:context to) (:spec to))))
          (spit (io (:context from) (:spec from)) unique-string)
          (copy (:spec from) (:spec to) (:context from) (:context to))
          (is (= (slurp (io (:context to) (:spec to))) unique-string))))))

  (testing "Deleting"

    (is (thrown-with-msg? ExceptionInfo #"Cannot delete context; it's not empty."
                          (delete (:context file-a)))
        "We are only allowed to delete empty contexts.")
    (is (thrown-with-msg? ExceptionInfo #"Cannot delete context; it's not empty."
                          (delete (:context file-b)))
        "We are only allowed to delete empty contexts.")

    (println "Cleaning up and deleting files/context(s)...")
    (doseq [f [file-a file-b]]
      (println (format "Deleting %s..." (resolve (:context f) (:spec f))))
      (delete (:context f) (:spec f)))

    (is (empty? (list (:context file-a))) "Context emptied.")
    (is (empty? (list (:context file-b))) "Context emptied.")

    (doseq [f [file-a file-b]]
      (println (format "Deleting context (%s)" (:desc f)))
      (delete (:context f)))))
