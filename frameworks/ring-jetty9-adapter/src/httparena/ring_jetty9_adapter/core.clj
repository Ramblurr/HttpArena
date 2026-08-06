(ns httparena.ring-jetty9-adapter.core
  (:gen-class)
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.adapter.jetty9 :as jetty]
   [ring.middleware.params :as params]
   [ring.util.response :as response])
  (:import
   (io.vertx.core AsyncResult Future Handler Vertx)
   (io.vertx.core.json JsonArray)
   (io.vertx.pgclient PgBuilder PgConnectOptions)
   (io.vertx.sqlclient ClientBuilder Pool PoolOptions PreparedQuery Row RowSet Tuple)
   (java.io InputStream OutputStream)
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
  (or (some-> value str str/trim not-empty parse-long)
      0))

(defn async-db-pool-size []
  (max 1 (int (parse-long-safe (or (System/getenv "DATABASE_MAX_CONN") "256")))))

(defn load-dataset [path]
  (when (.exists (io/file path))
    (json/read-str (slurp path) :key-fn keyword)))

(defonce dataset
  (delay (load-dataset "/data/dataset.json")))

(defonce vertx (atom nil))

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
  (with-open [^InputStream stream in]
    (.transferTo stream (OutputStream/nullOutputStream))))

(defn text-response [status body]
  {:status status
   :headers {"content-type" "text/plain"}
   :body body})

(defn json-response [status body]
  {:status status
   :headers {"Content-Type" json-content-type}
   :body (json/write-str body)})

(defn postgres-row->item [^Row row]
  {:id       (.getInteger row "id")
   :name     (.getString row "name")
   :category (.getString row "category")
   :price    (.getInteger row "price")
   :quantity (.getInteger row "quantity")
   :active   (.getBoolean row "active")
   :tags     (if-let [^JsonArray tags (.getJsonArray row "tags")]
               (vec (.getList tags))
               [])
   :rating   {:score (.getInteger row "rating_score")
              :count (.getInteger row "rating_count")}})

(defn init-async-db! []
  (when-let [database-url (System/getenv "DATABASE_URL")]
    (or @async-db
        (locking async-db
          (or @async-db
              (let [^PgConnectOptions connect-options (PgConnectOptions/fromUri database-url)
                    ^PoolOptions pool-options (doto (PoolOptions.)
                                                (.setMaxSize (async-db-pool-size)))
                    ^ClientBuilder builder (PgBuilder/pool)
                    ^Vertx vertx-instance (or @vertx
                                              (reset! vertx (Vertx/vertx)))
                    ^Pool database (-> builder
                                       (.with pool-options)
                                       (.connectingTo connect-options)
                                       (.using vertx-instance)
                                       (.build))]
                (reset! async-db database)))))))

(defn complete-async-db! [result respond raise]
  (let [^AsyncResult async-result result]
    (if (.succeeded async-result)
      (try
        (let [^RowSet rows  (.result async-result)
              items         (mapv postgres-row->item rows)]
          (respond (json-response 200 {:items items
                                       :count (count items)})))
        (catch Throwable error
          (raise error)))
      (respond (json-response 200 {:items [] :count 0})))))

(defn async-db-response [request respond raise]
  (let [params    (:params request)
        min-price (parse-long-safe (get params "min" "10"))
        max-price (parse-long-safe (get params "max" "50"))
        limit     (-> (get params "limit" "50")
                      parse-long-safe
                      (max 1)
                      (min 50))]
    (when-not
     (try
       (if-let [^Pool database (init-async-db!)]
         (let [^PreparedQuery query (.preparedQuery database async-db-query)
               ^Tuple query-params (doto (Tuple/tuple)
                                     (.addInteger (int min-price))
                                     (.addInteger (int max-price))
                                     (.addInteger (int limit)))
               ^Future query-result (.execute query query-params)]
           (.onComplete query-result
                        (reify Handler
                          (handle [_ result]
                            (complete-async-db! result respond raise))))
           true)
         false)
       (catch Throwable _
         false))
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

(defn json-data-response [request]
  (let [uri        (:uri request)
        item-count (-> (subs uri (count "/json/"))
                       parse-long-safe
                       (max 1)
                       (min 50))
        multiplier (if-let [value (get-in request [:params "m"])]
                     (parse-long-safe value)
                     1)]
    (if-let [source @dataset]
      (let [items (compute-json-items (take item-count source) multiplier)]
        (json-response 200 {:items items
                            :count (count items)}))
      (text-response 500 "dataset.json not available"))))

(defn sync-app [request]
  (let [uri    (:uri request)
        method (:request-method request)]
    (cond
      (str/starts-with? uri "/static/")
      (if (= :get method)
        (or (static-response uri)
            (text-response 404 "not found"))
        (method-not-allowed-response))

      (re-matches #"/json/[0-9]+" uri)
      (if (= :get method)
        (json-data-response request)
        (method-not-allowed-response))

      :else
      (case uri
        "/baseline11" (case method
                        (:get :post) (text-response 200 (str (request-sum request)))
                        (method-not-allowed-response))
        "/upload" (if (= :post method)
                    (text-response 200 (str (count-stream-bytes (:body request))))
                    (method-not-allowed-response))
        "/pipeline" (if (= :get method)
                      (text-response 200 "ok")
                      (method-not-allowed-response))
        (text-response 404 "not found")))))

(defn app
  ([request]
   (sync-app request))
  ([request respond raise]
   (if (= "/async-db" (:uri request))
     (if (= :get (:request-method request))
       (async-db-response request respond raise)
       (respond (method-not-allowed-response)))
     (respond (sync-app request)))))

(defn handler
  ([request]
   (app (params/params-request request)))
  ([request respond raise]
   (try
     (app (params/params-request request) respond raise)
     (catch Throwable error
       (raise error)))))

(defn -main [& _args]
  (when-not (vector? @dataset)
    (throw (ex-info "dataset.json must contain a JSON array"
                    {:path "/data/dataset.json"})))
  (jetty/run-jetty handler
                   {:async? true
                    :host "0.0.0.0"
                    :join? true
                    :port 8080
                    :wrap-jetty-handler (fn [^org.eclipse.jetty.server.Handler ring-handler]
                                          (doto (GzipHandler.)
                                            (.setExcludedPaths (into-array String ["/static/*"]))
                                            (.setHandler ring-handler)))}))
