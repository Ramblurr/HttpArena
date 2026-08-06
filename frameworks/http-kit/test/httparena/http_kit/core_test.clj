(ns httparena.http-kit.core-test
  (:require
   [clojure.test :refer [deftest is]]
   [httparena.http-kit.core :as core]
   [org.httpkit.server :as http-kit])
  (:import
   [io.vertx.sqlclient Pool PreparedQuery]
   [java.lang.reflect InvocationHandler Method Proxy]))

(def empty-response
  {:status 200
   :headers {"content-type" "application/json"}
   :body "{\"items\":[],\"count\":0}"})

(defn interface-proxy [^Class interface handler]
  (Proxy/newProxyInstance
   (.getClassLoader interface)
   (into-array Class [interface])
   (reify InvocationHandler
     (invoke [_ proxy method args]
       (handler proxy method args)))))

(defn request-responses [database]
  (let [responses (atom [])]
    (with-redefs [core/init-async-db! (fn [] database)
                  http-kit/as-channel (fn [_ options]
                                        ((:on-open options) ::channel))
                  http-kit/send! (fn [_ response]
                                   (swap! responses conj response))]
      (core/async-db-response {:params {}}))
    @responses))

(deftest prepared-query-failure-falls-back-and-retries
  (let [attempts (atom 0)
        database (interface-proxy
                  Pool
                  (fn [_ ^Method method _]
                    (when (= "preparedQuery" (.getName method))
                      (swap! attempts inc)
                      (throw (ex-info "preparedQuery failed" {})))))
        responses [(request-responses database)
                   (request-responses database)]]
    (is (= {:attempts 2
            :responses [[empty-response] [empty-response]]}
           {:attempts @attempts
            :responses responses}))))

(deftest execute-dispatch-failure-falls-back
  (let [query (interface-proxy
               PreparedQuery
               (fn [_ ^Method method _]
                 (when (= "execute" (.getName method))
                   (throw (ex-info "execute failed" {})))))
        database (interface-proxy
                  Pool
                  (fn [_ ^Method method _]
                    (when (= "preparedQuery" (.getName method))
                      query)))]
    (is (= [empty-response]
           (request-responses database)))))
