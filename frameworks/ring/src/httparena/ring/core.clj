(ns httparena.ring.core
  (:gen-class)
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.adapter.jetty :as jetty]
   [ring.middleware.params :as params]
   [ring.util.response :as response]
   [sqlite4clj.core :as sqlite])
  (:import
   [io.vertx.core Handler Vertx]
   [io.vertx.pgclient PgBuilder PgConnectOptions]
   [io.vertx.sqlclient PoolOptions Tuple]
   (java.io InputStream)
   (java.util.zip Deflater)
   (org.eclipse.jetty.server Server)
   (org.eclipse.jetty.server.handler.gzip GzipHandler)
   (org.eclipse.jetty.util.compression DeflaterPool)))

(set! *warn-on-reflection* true)

(def json-content-type "application/json")
(def static-root "/data/static")
(def benchmark-db-path "/data/benchmark.db")
(def db-query
  "SELECT id, name, category, price, quantity, active, tags, rating_score, rating_count
   FROM items
   WHERE price BETWEEN ? AND ?
   LIMIT 50")
(def async-db-query
  "SELECT id, name, category, price, quantity, active, tags, rating_score, rating_count
   FROM items
   WHERE price BETWEEN $1 AND $2
   LIMIT $3")
(def static-content-types
  {"css" "text/css"
   "js" "application/javascript"
   "html" "text/html"
   "woff2" "font/woff2"
   "svg" "image/svg+xml"
   "webp" "image/webp"
   "json" "application/json"})

(defn parse-long-safe [value]
  (cond
    (nil? value) 0
    (string? value)
    (let [trimmed (.trim ^String value)]
      (if (.isEmpty trimmed)
        0
        (Long/parseLong trimmed)))
    :else
    (recur (str value))))

(defn parse-double-safe [value default]
  (cond
    (nil? value) default
    (string? value)
    (let [trimmed (.trim ^String value)]
      (if (.isEmpty trimmed)
        default
        (try
          (Double/parseDouble trimmed)
          (catch NumberFormatException _
            default))))
    :else
    (recur (str value) default)))

(defn load-dataset [path]
  (when (.exists (io/file path))
    (json/read-str (slurp path) :key-fn keyword)))

(declare compute-json-items)

(defn build-json-body [items]
  (json/write-str {:items (compute-json-items items 1)
                   :count (count items)}))

(defonce dataset
  (delay (load-dataset "/data/dataset.json")))

(defonce compression-body
  (delay
    (some-> (load-dataset "/data/dataset-large.json")
            build-json-body)))

(defonce db
  (delay
    (when (.exists (io/file benchmark-db-path))
      (let [database (sqlite/init-db! benchmark-db-path
                                      {:pool-size (max 1 (.availableProcessors (Runtime/getRuntime)))
                                       :default-result-set-fn sqlite/qualified-keyword-result-set-fn})]
        (sqlite/q (:reader database) ["SELECT 1"])
        database))))

(defonce async-db (atom nil))

(defn compute-json-items [items multiplier]
  (mapv (fn [{:keys [price quantity] :as item}]
          (assoc item :total (* price quantity multiplier)))
        items))

(defn request-sum [request]
  (let [params (:params request)
        a (parse-long-safe (get params "a"))
        b (parse-long-safe (get params "b"))
        body (if (= :post (:request-method request))
               (parse-long-safe (slurp (:body request)))
               0)]
    (+ a b body)))

(defn count-stream-bytes [^InputStream in]
  (with-open [stream in]
    (let [buffer (byte-array 16384)]
      (loop [total 0]
        (let [read-count (.read stream buffer 0 (alength buffer))]
          (if (neg? read-count)
            total
            (recur (+ total read-count))))))))

(defn text-response [status body]
  {:status status
   :headers {"content-type" "text/plain"}
   :body body})

(defn json-response [status body]
  {:status status
   :headers {"content-type" json-content-type}
   :body (json/write-str body)})

(defn json-endpoint-response [request item-count]
  (if-let [source @dataset]
    (let [items (take (min 50 (parse-long-safe item-count)) source)
          multiplier (parse-long-safe (get (:params request) "m"))]
      (json-response 200 {:items (compute-json-items items multiplier)
                          :count (count items)}))
    (text-response 500 "dataset.json not available")))

(defn compression-response []
  (if-let [body @compression-body]
    {:status 200
     :headers {"content-type" json-content-type}
     :body body}
    (text-response 500 "dataset-large.json not available")))

(defn sqlite-row->item [row]
  {:id (:items/id row)
   :name (:items/name row)
   :category (:items/category row)
   :price (:items/price row)
   :quantity (:items/quantity row)
   :active (not (zero? (long (:items/active row))))
   :tags (json/read-str ^String (:items/tags row))
   :rating {:score (:items/rating_score row)
            :count (:items/rating_count row)}})

(defn vertx-row->item [row]
  {:id       (.getInteger row "id")
   :name     (.getString row "name")
   :category (.getString row "category")
   :price    (.getInteger row "price")
   :quantity (.getInteger row "quantity")
   :active   (.getBoolean row "active")
   :tags     (vec (.getList (.getJsonArray row "tags")))
   :rating   {:score (.getInteger row "rating_score")
              :count (.getInteger row "rating_count")}})

(defn async-db-pool-size []
  (max 1 (int (parse-long-safe (or (System/getenv "DATABASE_MAX_CONN") "256")))))

(defn init-async-db! []
  (when-let [database-url (System/getenv "DATABASE_URL")]
    (or @async-db
        (locking async-db
          (or @async-db
              (try
                (let [database (-> (PgBuilder/pool)
                                   (.with (doto (PoolOptions.)
                                            (.setMaxSize (async-db-pool-size))))
                                   (.connectingTo (PgConnectOptions/fromUri database-url))
                                   (.using (Vertx/vertx))
                                   (.build))]
                  (reset! async-db database))
                (catch Throwable _
                  nil)))))))

(defn db-response [request]
  (let [params (:params request)
        min-price (parse-double-safe (get params "min") 10.0)
        max-price (parse-double-safe (get params "max") 50.0)
        items (if-let [database @db]
                (try
                  (mapv sqlite-row->item
                        (or (sqlite/q (:reader database) [db-query min-price max-price]) []))
                  (catch Throwable _
                    []))
                [])]
    (json-response 200 {:items items
                        :count (count items)})))

(defn async-db-response [request respond]
  (let [params    (:params request)
        min-price (int (parse-long-safe (get params "min" "10")))
        max-price (int (parse-long-safe (get params "max" "50")))
        limit     (int (min 50 (max 1 (parse-long-safe (get params "limit" "50")))))
        database  (init-async-db!)]
    (if database
      (-> (.preparedQuery database async-db-query)
          (.execute (Tuple/of min-price max-price limit))
          (.onComplete
           (reify Handler
             (handle [_ result]
               (respond
                (if (.succeeded result)
                  (let [items (mapv vertx-row->item (.result result))]
                    (json-response 200 {:items items
                                        :count (count items)}))
                  (json-response 200 {:items [] :count 0})))))))
      (respond (json-response 200 {:items [] :count 0})))))

(defn method-not-allowed-response []
  (text-response 405 "method not allowed"))

(defn static-filename [uri]
  (when (str/starts-with? uri "/static/")
    (let [filename (subs uri 8)]
      (when (and (not (str/blank? filename))
                 (not (str/includes? filename "/"))
                 (not (str/includes? filename "..")))
        filename))))

(defn static-content-type [filename]
  (let [extension (some-> filename (str/split #"\.") last str/lower-case)]
    (get static-content-types extension "application/octet-stream")))

(defn static-response [uri]
  (when-let [filename (static-filename uri)]
    (if-let [file-response (response/file-response filename {:root static-root
                                                            :index-files? false})]
      (response/content-type file-response (static-content-type filename))
      (text-response 404 "not found"))))

(defn app
  ([request]
   (if-let [file-response (static-response (:uri request))]
     file-response
     (if-let [[_ item-count] (re-matches #"/json/(\d+)" (:uri request))]
       (if (= :get (:request-method request))
         (json-endpoint-response request item-count)
         (method-not-allowed-response))
       (case (:uri request)
         "/baseline11" (case (:request-method request)
                         (:get :post) (text-response 200 (str (request-sum request)))
                         (method-not-allowed-response))
         "/compression" (if (= :get (:request-method request))
                          (compression-response)
                          (method-not-allowed-response))
         "/db" (if (= :get (:request-method request))
                 (db-response request)
                 (method-not-allowed-response))
         "/async-db" (json-response 200 {:items [] :count 0})
         "/upload" (if (= :post (:request-method request))
                     (text-response 200 (str (count-stream-bytes (:body request))))
                     (method-not-allowed-response))
         "/pipeline" (if (= :get (:request-method request))
                       (text-response 200 "ok")
                       (method-not-allowed-response))
         (text-response 404 "not found")))))
  ([request respond _raise]
   (if (and (= :get (:request-method request))
            (= "/async-db" (:uri request)))
     (async-db-response request respond)
     (respond (app request)))))

(def handler
  (params/wrap-params app))

(defn -main [& _args]
  (init-async-db!)
  (jetty/run-jetty handler
                    {:host         "0.0.0.0"
                     :configurator (fn [^Server server]
                                     (let [gzip-handler (doto (GzipHandler.)
                                                          (.setMinGzipSize 1)
                                                          (.setDeflaterPool (DeflaterPool. -1 Deflater/BEST_SPEED true))
                                                          (.setHandler (.getHandler server)))]
                                       (.setHandler server gzip-handler)))
                     :port         8080
                     :join?        true
                     :async?       true}))
