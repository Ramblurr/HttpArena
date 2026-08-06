(ns httparena.pedestal.core
  (:gen-class)
  (:require
   [clojure.core.async :as async]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [io.pedestal.connector :as conn]
   [io.pedestal.http.jetty :as jetty]
   [io.pedestal.http.route :as route]
   [io.pedestal.service.interceptors :as interceptors]
   [ring.util.response :as response])
  (:import
   [io.vertx.core AsyncResult Handler Vertx]
   [io.vertx.core.json JsonArray]
   [io.vertx.pgclient PgBuilder PgConnectOptions]
   [io.vertx.sqlclient Pool PoolOptions Row Tuple]
   (java.io InputStream)
   (org.eclipse.jetty.ee10.servlet ServletContextHandler)
   (org.eclipse.jetty.server.handler.gzip GzipHandler)))

(set! *warn-on-reflection* true)

(def json-content-type "application/json")
(def static-root "/data/static")
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

(defn async-db-pool-size []
  (max 1 (int (parse-long-safe (or (System/getenv "DATABASE_MAX_CONN") "256")))))

(defn round2 [value]
  (/ (Math/round (* (double value) 100.0)) 100.0))

(defn load-dataset [path]
  (when (.exists (io/file path))
    (json/read-str (slurp path) :key-fn keyword)))

(defonce dataset
  (delay (load-dataset "/data/dataset.json")))

(defonce async-db (atom nil))

(defn compute-json-items [items multiplier]
  (mapv (fn [{:keys [price quantity] :as item}]
          (assoc item :total (round2 (* price quantity multiplier))))
        items))

(defn request-sum [request]
  (let [query-params (:query-params request)
        a (parse-long-safe (get query-params :a))
        b (parse-long-safe (get query-params :b))
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
   :headers {"Content-Type" json-content-type}
   :body (json/write-str body)})

(defn baseline-handler [request]
  (text-response 200 (str (request-sum request))))

(defn pipeline-handler [_request]
  (text-response 200 "ok"))

(defn json-handler [request]
  (if-let [source @dataset]
    (let [requested-count (min (parse-long-safe (get-in request [:path-params :count]))
                                (count source))
          multiplier (parse-double-safe (get-in request [:query-params :m]) 1.0)
          items (compute-json-items (take requested-count source) multiplier)]
      (json-response 200 {:items items
                          :count (count items)}))
    (text-response 500 "dataset.json not available")))

(defn upload-handler [request]
  (text-response 200 (str (count-stream-bytes (:body request)))))

(defn vertx-row->item [^Row row]
  {:id       (.getInteger row "id")
   :name     (.getString row "name")
   :category (.getString row "category")
   :price    (.getInteger row "price")
   :quantity (.getInteger row "quantity")
   :active   (.getBoolean row "active")
   :tags     (vec (.getList ^JsonArray (.getJsonArray row "tags")))
   :rating   {:score (.getInteger row "rating_score")
              :count (.getInteger row "rating_count")}})

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

(defn async-db-handler [request]
  (let [query-params (:query-params request)
        min-price (int (parse-long-safe (or (get query-params :min) "10")))
        max-price (int (parse-long-safe (or (get query-params :max) "50")))
        limit (int (min 50 (max 1 (parse-long-safe (or (get query-params :limit) "50")))))
        ^Pool database (init-async-db!)
        response-ch (async/promise-chan)
        empty-response (json-response 200 {:items []
                                           :count 0})
        respond! (fn [response]
                   (async/put! response-ch response
                               (fn [_]
                                 (async/close! response-ch))))]
    (if database
      (try
        (-> (.preparedQuery database async-db-query)
            (.execute (Tuple/of min-price max-price limit))
            (.onComplete
             (reify Handler
               (handle [_ result]
                 (let [^AsyncResult result result
                       response (if (.succeeded result)
                                  (try
                                    (json-response 200
                                                   (let [items (mapv vertx-row->item (.result result))]
                                                     {:items items
                                                      :count (count items)}))
                                    (catch Throwable _
                                      empty-response))
                                  empty-response)]
                   (respond! response))))))
        (catch Throwable _
          (respond! empty-response)))
      (respond! empty-response))
    response-ch))

(defn static-content-type [filename]
  (let [extension (some-> filename (str/split #"\.") last str/lower-case)]
    (get static-content-types extension "application/octet-stream")))

(defn static-response [filename]
  (when (and filename
             (not (str/blank? filename))
             (not (str/includes? filename "/"))
             (not (str/includes? filename "..")))
    (if-let [file-response (response/file-response filename {:root static-root
                                                            :index-files? false})]
      (response/content-type file-response (static-content-type filename))
      (text-response 404 "not found"))))

(defn static-handler [request]
  (or (static-response (get-in request [:path-params :filename]))
      (text-response 404 "not found")))

(def routes
  #{["/baseline11" :get baseline-handler :route-name ::baseline-get]
    ["/baseline11" :post baseline-handler :route-name ::baseline-post]
    ["/json/:count" :get json-handler :route-name ::json]
    ["/async-db" :get async-db-handler :route-name ::async-db]
    ["/upload" :post upload-handler :route-name ::upload]
    ["/static/:filename" :get static-handler :route-name ::static]
    ["/pipeline" :get pipeline-handler :route-name ::pipeline]})

(defn create-connector []
  (-> (conn/default-connector-map "0.0.0.0" 8080)
      (assoc :join? true)
      (conn/with-interceptor interceptors/not-found)
      (conn/with-interceptor route/query-params)
      (conn/with-routes routes)
      (jetty/create-connector
       {:container-options {:h2c? false
                            :context-configurator (fn [^ServletContextHandler context]
                                                    (let [gzip-handler (doto (GzipHandler.)
                                                                         (.addExcludedPaths
                                                                          (into-array String ["/static/*"])))]
                                                      (.insertHandler context gzip-handler)
                                                      context))}})))

(defn -main [& _args]
  (conn/start! (create-connector)))
