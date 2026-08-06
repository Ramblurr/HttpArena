(ns httparena.ring.core-test
  (:require
   [clojure.data.json :as json]
   [clojure.test :as test :refer [are deftest is]]
   [httparena.ring.core :as core])
  (:import
   [io.vertx.core AsyncResult]))

(def dataset
  [{:id 1
    :name "Alpha"
    :category "tools"
    :price 2
    :quantity 4
    :active true
    :tags ["new"]
    :rating {:score 5 :count 6}}
   {:id 2
    :name "Beta"
    :category "tools"
    :price 3
    :quantity 5
    :active false
    :tags ["sale"]
    :rating {:score 7 :count 8}}])

(deftest json-route-computes-request-specific-totals
  (with-redefs [core/dataset (delay dataset)]
    (is (= {:items [{:id 1
                     :name "Alpha"
                     :category "tools"
                     :price 2
                     :quantity 4
                     :active true
                     :tags ["new"]
                     :rating {:score 5 :count 6}
                     :total 24}]
            :count 1}
           (json/read-str (:body (core/app {:request-method :get
                                            :uri "/json/1"
                                            :params {"m" "3"}}))
                          :key-fn keyword)))))

(deftest declared-routes-reject-unsupported-methods
  (are [request] (= 405 (:status (core/app request)))
    {:request-method :post :uri "/json/1" :params {}}
    {:request-method :post :uri "/async-db" :params {}}
    {:request-method :get :uri "/upload" :params {}}
    {:request-method :post :uri "/pipeline" :params {}}
    {:request-method :post :uri "/static/app.js" :params {}}))

(deftest async-db-route-responds-with-empty-fallback
  (with-redefs [core/init-async-db! (constantly nil)]
    (let [response (promise)]
      (core/app {:request-method :get
                 :uri "/async-db"
                 :params {}}
                #(deliver response %)
                #(deliver response %))
      (is (= {:items [] :count 0}
             (json/read-str (:body @response) :key-fn keyword))))))

(deftest async-db-query-failures-use-empty-fallback
  (let [completion (reify AsyncResult
                     (succeeded [_] false)
                     (failed [_] true)
                     (result [_] nil)
                     (cause [_] (Exception. "database unavailable")))
        response (promise)]
    (core/complete-async-db! completion
                             #(deliver response %)
                             #(deliver response %))
    (is (= {:items [] :count 0}
           (json/read-str (:body @response) :key-fn keyword)))))

(deftest async-db-conversion-errors-use-raise
  (let [exception (ex-info "conversion failed" {})
        response (promise)]
    (with-redefs [core/vertx-row->item (fn [_] (throw exception))]
      (core/respond-with-db-rows! [::row]
                                  #(deliver response %)
                                  #(deliver response %)))
    (is (identical? exception @response))))

(deftest application-errors-use-raise
  (let [exception (ex-info "unexpected" {})
        response (promise)]
    (with-redefs [core/static-response (fn [_] (throw exception))]
      (core/app {:request-method :get :uri "/static/app.js"}
                #(deliver response %)
                #(deliver response %)))
    (is (identical? exception @response))))

(defn -main [& _args]
  (let [results (test/run-tests 'httparena.ring.core-test)]
    (when (pos? (+ (:fail results) (:error results)))
      (throw (ex-info "tests failed" results)))))
