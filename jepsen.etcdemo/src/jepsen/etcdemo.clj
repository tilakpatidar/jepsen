(ns jepsen.etcdemo
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]
            [jepsen.support :as util]
            [jepsen.set :as set]
            ))


; Constants
(def binary "etcd")
(def dir "/opt/etcd")
(def logfile (str dir "/etcd.log"))
; Directory where etcd will be installed
(def pidfile (str dir "/etcd.pid"))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))


; The ->> threading macro takes each form and inserts it into the next
; form as a final argument. So (->> test :nodes) becomes (:nodes test),
; and (->> test :nodes (map-indexed (fn ...))) becomes (map-indexed (fn ...)
; (:nodes test)), and so on. Normal function calls often look "inside out",
; but the ->> macro lets us write a chain of operations "in order"--like an
; object-oriented language's foo.bar().baz() notation.
(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test) ; loops on each nodes and then create foo.bar().baz() kind of chaining
       (map (fn [node]
              (str node "=" (util/peer-url node))))
       (str/join ",")))

; Note also that the object reify returns closes over its lexical scope, remembering the value of version
; reify defines both an anonymous type and creates an instance of that type.
; The use case is where you need a one-off implementation of one or more protocols
; or interfaces and would like to take advantage of the local context.
; In this respect it is use case similar to proxy, or anonymous inner classes in Java.
; The method bodies of reify are lexical closures, and can refer to the
; surrounding local scope. reify differs from proxy in that:

; The names of functions/macros that are not safe in STM transactions
; should end with an exclamation mark. What the heck is STM?

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (teardown! [_ test node]
      (info node "tearing down etcd")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))

    (setup! [_ test node]
      (info node "installing etcd" version)
      (c/su
        (let [url (str "https://storage.googleapis.com/etcd/" version
                       "/etcd-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))

        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :--log-output                   :stderr
          :--name                         (name node)
          :--listen-peer-urls             (util/peer-url   node)
          :--listen-client-urls           (util/client-url node)
          :--advertise-client-urls        (util/client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (util/peer-url node)
          :--initial-cluster              (initial-cluster test))

        (Thread/sleep 10000)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])

    ))

;; Client for the test

;These are functions that construct Jepsen operations: an
; abstract representation of things you can do to a database. :invoke means that we're going to
; try an operation--when it completes, we'll use a type like :ok or :fail to tell what happened.
; The :f tells us what function we're applying to the database--for instance,
; that we want to perform a read or a write. These can be any values--Jepsen doesn't know what they mean.

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})


;defrecord defines a new type of data structure, which we're calling Client.
; Each Client has a single field, called conn, which will hold our connection to a particular network server.
; Clients support Jepsen's Client protocol, and just like a reify, we'll provide implementations for Client functions.

;Clients have a five-part lifecycle. We begin with a single seed client (client).
; When we call open! on that client, we get a copy of the client bound to a particular node.
; The setup! function initializes any data structures the test needs--for instance,
; creating tables or setting up fixtures. invoke! applies operations to the system and returns
; corresponding completion operations. teardown! cleans up any tables setup! may have created.
; close! closes network connections and completes the lifecycle for the client.
(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (util/client-url node)
                                 {:timeout 5000})))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (try+
        (case (:f op)
          :read (let [value (-> conn
                                (v/get k {:quorum? (:quorum test)})
                                parse-long)]
                  (assoc op :type :ok, :value (independent/tuple k value)))

          :write (do (v/reset! conn k v)
                     (assoc op :type :ok))

          ; The let binding here uses destructuring:
          ; it breaks apart the [old-value new-value] pair from the operation's :value field into value and value'.
          ; Since all values except false and nil are logically true, we can use the result of the cas!
          ; call as our predicate in if.


          :cas (let [[old new] v]
                 (assoc op :type (if (v/cas! conn k old new)
                                   :ok
                                   :fail))))

        (catch java.net.SocketTimeoutException e
          (assoc op
            :type  (if (= :read (:f op)) :fail :info)
            :error :timeout))

        (catch [:errorCode 100] e
          (assoc op :type :fail, :error :not-found)))))

  (teardown! [this test])

  (close! [_ test]
    ; If our connection were stateful, we'd close it here. Verschlimmmbesserung
    ; doesn't actually hold connections, so there's nothing to close.
    ))


(defn register-workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
                (checker/compose
                  {:linear   (checker/linearizable)
                   :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
                10
                (range)
                (fn [k]
                  (->> (gen/mix [r w cas])
                       (gen/limit (:ops-per-key opts)))))})

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"set"      set/workload
   "register" register-workload})

; def are evaluated only once
; defn are evaluated every time
; defining fn using def =>  (def some-fn (fn [a] (+ a 1)))
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
    :concurrency ...), constructs a test map. Special options:

        :quorum       Whether to use quorum reads
        :rate         Approximate number of requests per second, per thread
        :ops-per-key  Maximum number of operations allowed on any given key
        :workload     Type of workload."
  [opts]
  (let [quorum (boolean (:quorum opts)) workload ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           {:name       (str "etcd q=" quorum " "
                             (name (:workload opts)))
            :quorum     quorum
            :os         debian/os
            :db         (db "v3.1.5")
            :client     (:client workload)
            :nemesis    (nemesis/partition-random-halves)
            :model      (model/cas-register)
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :workload (:checker workload)})
            :generator  (gen/phases
                          (->> (:generator workload)
                               (gen/stagger (/ (:rate opts)))
                               (gen/nemesis
                                 (gen/seq (cycle [(gen/sleep 5)
                                                  {:type :info, :f :start}
                                                  (gen/sleep 5)
                                                  {:type :info, :f :stop}])))
                               (gen/time-limit (:time-limit opts)))
                          (gen/log "Healing cluster")
                          (gen/nemesis (gen/once {:type :info, :f :stop}))
                          (gen/log "Waiting for recovery")
                          (gen/sleep 10)
                          (gen/clients (:final-generator workload)))
          }))
  )

; Notice the -main - is mandatory for main functions in leiningen
; [] Vectors () list

;Remember, the initial client has no connections--like a stem cell,
; it has the potential to become an active client but doesn't do any work directly.
; We call (Client. nil) to construct that initial client--its conn will be filled in when Jepsen calls setup!.
; The nil here is the value of conn stored in the defrecord
(def cli-opts
  "Additional command line options."
  [["-q" "--quorum" "Use quorum reads, instead of reading from any primary."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   ["-w" "--workload NAME" "What workload should we run?"
     :missing  (str "--workload " (cli/one-of workloads))
     :validate [workloads (cli/one-of workloads)]]
   ])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))

