(ns httparena.ring.core-test
  (:require
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is]]
   [httparena.ring.core :as core]))

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

(deftest async-db-route-responds-through-callback
  (with-redefs [core/init-async-db! (constantly nil)]
    (let [response (promise)]
      (core/app {:request-method :get
                 :uri "/async-db"
                 :params {}}
                #(deliver response %)
                (fn [exception] (deliver response exception)))
      (is (= {:items [] :count 0}
             (json/read-str (:body @response) :key-fn keyword))))))
