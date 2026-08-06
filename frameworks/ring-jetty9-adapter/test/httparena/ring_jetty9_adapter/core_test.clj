(ns httparena.ring-jetty9-adapter.core-test
  (:require
   [clojure.data.json :as json]
   [clojure.test :as test :refer [deftest is]]
   [httparena.ring-jetty9-adapter.core :as core])
  (:import
   [io.vertx.sqlclient Pool PreparedQuery]))

(defn observed-result [responses raises]
  {:responses (mapv #(-> %
                         (select-keys [:status :body])
                         (update :body json/read-str :key-fn keyword))
                    @responses)
   :raises @raises})

(deftest synchronous-initialization-failures-fall-back-and-retry
  (let [responses (atom [])
        raises    (atom [])
        attempts  (atom 0)]
    (with-redefs [core/init-async-db! (fn []
                                        (when (= 1 (swap! attempts inc))
                                          (throw (ex-info "initialization failed" {}))))]
      (dotimes [_ 2]
        (core/async-db-response {:params {}}
                                #(swap! responses conj %)
                                #(swap! raises conj %))))
    (is (= {:responses [{:status 200 :body {:items [] :count 0}}
                        {:status 200 :body {:items [] :count 0}}]
            :raises []
            :attempts 2}
           (assoc (observed-result responses raises) :attempts @attempts)))))

(deftest synchronous-prepared-query-failures-use-empty-fallback
  (let [responses (atom [])
        raises    (atom [])
        database  (reify Pool
                    (preparedQuery [_ _]
                      (throw (ex-info "prepared query failed" {}))))]
    (with-redefs [core/init-async-db! (constantly database)]
      (core/async-db-response {:params {}}
                              #(swap! responses conj %)
                              #(swap! raises conj %)))
    (is (= {:responses [{:status 200 :body {:items [] :count 0}}]
            :raises []}
           (observed-result responses raises)))))

(deftest synchronous-query-dispatch-failures-use-empty-fallback
  (let [responses (atom [])
        raises    (atom [])
        query     (reify PreparedQuery
                    (execute [_ _]
                      (throw (ex-info "dispatch failed" {}))))
        database  (reify Pool
                    (preparedQuery [_ _] query))]
    (with-redefs [core/init-async-db! (constantly database)]
      (core/async-db-response {:params {}}
                              #(swap! responses conj %)
                              #(swap! raises conj %)))
    (is (= {:responses [{:status 200 :body {:items [] :count 0}}]
            :raises []}
           (observed-result responses raises)))))

(defn -main [& _args]
  (let [results (test/run-tests 'httparena.ring-jetty9-adapter.core-test)]
    (when (pos? (+ (:fail results) (:error results)))
      (throw (ex-info "tests failed" results)))))
