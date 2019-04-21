(ns
  ^{:doc "Core `NamingConvention` and `Context` protocols + default
          implementations and helpers."
    :author "Matthew Downey"}
  schemata.core
  (:require [clojure.string :as string]
            [clojure.java.io :as io])
  (:refer-clojure :exclude [list])
  (:import (java.io File)
           (java.text SimpleDateFormat)
           (java.util TimeZone Date)
           (clojure.lang IDeref)))

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
  (in-context [this spec]
    "Puts the spec 'in context' by rendering its path in a way appropriate
    for the context. Useful for REPL exploration.")
  (delete [this spec]
    "Delete the file described by `spec`.")
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

  (in-context [this spec]
    (let [f (->file root naming-convention spec)]
      (.getCanonicalPath f)))

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
        (filter #(not (.isDirectory ^File %)))
        ;; Read 'em in
        (map #(try
                (path->spec naming-convention (file->path %))
                (catch Exception e (when strict? (throw e)))))
        (filter some?)))))

(defn local-context
  "A `Context` that discovers files & performs IO on the local filesystem.

  If no `naming-convention` argument is given, takes plain string paths (e.g.
  root/foo/bar/baz.log).

  The `root` is a relative path to the directory under which the given
  `naming-convention` applies."
  ([]
   (local-context nil))
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
         (string/join File/separator path)))))
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
