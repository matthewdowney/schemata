# schemata

A utility for abstracting away names and places for file-like data storage.

I originally wrote this for use with a market data collection server that 
produced thousands of flat files per day — each named to indicate the trading 
venue, market, type of market, and day of collection — and syncd them to S3. At 
a certain point I needed to be able to discover and read the files in, gzip/unzip 
arbitrary subsets of the files, and store them on S3 with a different naming 
convention than the one used locally.

---

Naming revolves around the `NamingConvention` protocol, which defines a
bidirectional relationship between a file's specification and a file path.

```clojure
;; The relationship between these attributes ...
{:log-type "error" :ts 1555632000000}

;; ... and this file path ...
"2019-04-19/error.log"

;; ... is defined by
(require '[schemata.core :as s])
(def naming
  (s/path-convention
    (s/utc :ts "yyyy-MM-dd") ;; first directory in the path
    (s/file-convention :log-type "log"))) ;; file name & extension

(s/spec->path naming {:log-type "error" :ts 1555632000000})
; => ["2019-04-19" "error.log"]

(s/path->spec naming ["2019-04-19" "error.log"])
; => {:ts 1555632000000, :log-type "error"}
```

Under the hood, utilities like `s/utc` or `s/file-convention` just return a map 
with two functions, `:in` and `:out`, that handle each direction of rendering/parsing
a part of a file path.

IO has to do with providing context to a naming convention. The `Context`
protocol describes how to list files, delete them, and build `clojure.java.io/IOFactory`
implementations for file specifications. The default context is the local
filesystem, but could be any "file-like" place (there's already one 
implementation for S3).

```clojure
;; A context for our previous naming convention
(def context 
  (s/local-context "root-dir" naming))

;; With which we can perform IO
(let [todays-info-log {:log-type "info" :ts 1555632000000}]
  ;; Same as (spit "root-dir/2019-04-19/info.log" ...)
  (spit (s/io context todays-info-log) "abcxyz")
  
  ;; Same as (slurp "root-dir/2019-04-19/info.log")
  (slurp (s/io context todays-info-log)))
; => "abcxyz"

;; And discover files matching our specification
(s/list context)
; => ({:ts 1555632000000, :log-type "info"})
```

The idea here is to make explicit the relationships between what data is,
what you call it, and where you store it. 

With that explicit set of relationships you can do things that really ought to 
be simple but might otherwise not be. For example, you could map between schemas, 
syncing files on your machine called `yyyy-MM-dd/log-type.log` with an S3 bucket 
where you name things `yyyy/MM/log-type.yyyy-MM-dd.log`, just as easily as you'd 
`slurp` / `spit` / `io/copy` any set of local files.

## Quick Example

```clojure 
;; (Make sure the naming & context from the intro are still defined)

;; Maybe we want to migrate to a new naming convention of
;; root-dir/yyyy/log-type_MMM-dd.log
(def new-naming
  (s/path-convention
    (s/utc :ts "yyyy")
    (s/file-convention
      (s/split-by "_" :log-type (s/utc :ts "MMM-dd"))
      "log")))
      
;; We'll keep the files local here, but realistically this 
;; is most useful with a remote context 
(def new-context 
  (s/local-context "root-dir" new-naming))

(require '[clojure.java.io :as io])

;; List files with the old naming / context
(doseq [old-file (s/list context naming)]
  ;; Map from the old context to the new context
  (io/copy 
    (s/io context old-file) 
    (s/io new-context old-file))
    
  ;; Get rid of the file in the old context
  (s/delete context old-file)
  (println "Moved" old-file))
; => Moved {:ts 1555632000000, :log-type info}
```

Check out the [S3]() context implementation to use with syncing files.

Near-term plans include adding a `gzip` function that modifies a context,
so that you could gzip files by writing to `(gzip context)` or read 
gzipped content by reading from a gzipped context.
