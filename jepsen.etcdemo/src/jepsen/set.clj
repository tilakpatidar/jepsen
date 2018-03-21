(ns jepsen.set
  (:require [jepsen [client :as client]
            [generator :as gen]]
            [slingshot.slingshot :refer [try+]]
            [verschlimmbesserung.core :as v]
            [jepsen.support :as util]
            [jepsen.checker :as checker]
            ))

(defrecord SetClient [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (util/client-url node))))
  (setup! [this test]
    (v/reset! conn k "#{}"))
  (invoke! [_ test op]
    (try+
      (case (:f op)
        :read (assoc op
                :type :ok,
                :value (read-string
                         (v/get conn k {:quorum? (:quorum test)})))

        :add (do (v/swap! conn k (fn [value]
                                   (-> value
                                       read-string
                                       (conj (:value op))
                                       pr-str)))
                 (assoc op :type :ok)))

      (catch java.net.SocketTimeoutException e
        (assoc op
          :type  (if (= :read (:f op)) :fail :info)
          :error :timeout))))
  (teardown! [_ test])

  (close! [_ test])
  )

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client (SetClient. "a-set" nil)
   :checker (checker/set)
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x}))
                   gen/seq)
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})