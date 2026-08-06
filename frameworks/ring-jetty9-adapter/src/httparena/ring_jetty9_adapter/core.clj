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
   (io.vertx.core Handler Vertx)
   (io.vertx.core.json JsonArray)
   (io.vertx.pgclient PgBuilder PgConnectOptions)
   (io.vertx.sqlclient PoolOptions Tuple)
   (java.io InputStream)
   (java.net URI)
   (java.util.zip Deflater)
   (org.eclipse.jetty.server Server)
   (org.eclipse.jetty.server.handler.gzip GzipHandler)
   (org.eclipse.jetty.util.compression DeflaterPool)))

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
  (int (parse-long-safe (or (System/getenv "DATABASE_MAX_CONN") "256"))))

(defn load-dataset [path]
  (when (.exists (io/file path))
    (json/read-str (slurp path) :key-fn keyword)))

(declare compute-json-items)

(defn build-json-body [items]
  (json/write-str {:items (compute-json-items items)
                   :count (count items)}))

(defonce dataset
  (delay (load-dataset "/data/dataset.json")))

(defonce compression-body
  (delay
    (some-> (load-dataset "/data/dataset-large.json")
            build-json-body)))

(defonce vertx
  (delay (Vertx/vertx)))

(defonce async-db (atom nil))

(defn compute-json-items
  ([items]
   (compute-json-items items 1))
  ([items multiplier]
   (mapv (fn [{:keys [price quantity] :as item}]
           (assoc item :total (* price quantity multiplier)))
         items)))

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
   :headers {"Content-Type" json-content-type}
   :body (json/write-str body)})

(defn compression-response []
  (if-let [body @compression-body]
    {:status 200
     :headers {"Content-Type" json-content-type}
     :body body}
    (text-response 500 "dataset-large.json not available")))

(defn pg-tags->value [value]
  (cond
    (nil? value) []
    (instance? JsonArray value) (.getList ^JsonArray value)
    (instance? java.util.List value) (vec value)
    (string? value) (json/read-str ^String value)
    :else (json/read-str (str value))))

(defn postgres-row->item [row]
  {:id (.getInteger row "id")
   :name (.getValue row "name")
   :category (.getValue row "category")
   :price (.getInteger row "price")
   :quantity (.getInteger row "quantity")
   :active (.getBoolean row "active")
   :tags (pg-tags->value (.getValue row "tags"))
   :rating {:score (.getInteger row "rating_score")
            :count (.getInteger row "rating_count")}})

(defn init-async-db! []
  (when-let [database-url (System/getenv "DATABASE_URL")]
    (or @async-db
        (locking async-db
          (or @async-db
              (let [uri (URI. database-url)
                    [username password] (str/split (.getUserInfo uri) #":" 2)
                    connect-options (-> (PgConnectOptions.)
                                        (.setHost (.getHost uri))
                                        (.setPort (if (neg? (.getPort uri)) 5432 (.getPort uri)))
                                        (.setDatabase (subs (.getPath uri) 1))
                                        (.setUser username)
                                        (.setPassword (or password "")))
                    pool-options (doto (PoolOptions.)
                                   (.setMaxSize (async-db-pool-size)))
                    database (-> (PgBuilder/pool)
                                 (.with pool-options)
                                 (.connectingTo connect-options)
                                 (.using @vertx)
                                 (.build))]
                (reset! async-db database)))))))

(defn async-db-response [request respond]
  (let [params (:params request)
        min-price (parse-long-safe (get params "min" "10"))
        max-price (parse-long-safe (get params "max" "50"))
        limit (-> (if-let [value (get params "limit")]
                    (parse-long-safe value)
                    50)
                  (max 1)
                  (min 50))]
    (if-let [database (init-async-db!)]
      (-> (.preparedQuery database async-db-query)
          (.execute (doto (Tuple/tuple)
                      (.addInteger (int min-price))
                      (.addInteger (int max-price))
                      (.addInteger (int limit))))
          (.onSuccess (reify Handler
                        (handle [_ rows]
                          (let [items (mapv postgres-row->item (iterator-seq (.iterator rows)))]
                            (respond (json-response 200 {:items items :count (count items)}))))))
          (.onFailure (reify Handler
                        (handle [_ _]
                          (respond (json-response 200 {:items [] :count 0}))))))
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
  (let [uri (:uri request)
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
  (let [uri (:uri request)]
    (if-let [file-response (static-response uri)]
      file-response
      (cond
        (str/starts-with? uri "/json/")
        (if (= :get (:request-method request))
          (json-data-response request)
          (method-not-allowed-response))

        :else
        (case uri
          "/baseline11" (case (:request-method request)
                          (:get :post) (text-response 200 (str (request-sum request)))
                          (method-not-allowed-response))
          "/json" (if (= :get (:request-method request))
                    (json-data-response (assoc request :uri "/json/50"))
                    (method-not-allowed-response))
          "/compression" (if (= :get (:request-method request))
                           (compression-response)
                           (method-not-allowed-response))
          "/upload" (if (= :post (:request-method request))
                      (text-response 200 (str (count-stream-bytes (:body request))))
                      (method-not-allowed-response))
          "/pipeline" (if (= :get (:request-method request))
                        (text-response 200 "ok")
                        (method-not-allowed-response))
          (text-response 404 "not found"))))))

(defn app
  ([request]
   (sync-app request))
  ([request respond raise]
   (if (and (= :get (:request-method request))
            (= "/async-db" (:uri request)))
     (async-db-response request respond)
     (try
       (respond (sync-app request))
       (catch Throwable error
         (raise error))))))

(defn handler
  ([request]
   (app (params/params-request request)))
  ([request respond raise]
   (app (params/params-request request) respond raise)))

(defn -main [& _args]
  (jetty/run-jetty handler
                   {:host "0.0.0.0"
                    :configurator
                    (fn [^Server server]
                      (let [gzip-handler (doto (GzipHandler.)
                                           (.setMinGzipSize 1)
                                           (.setDeflaterPool (DeflaterPool. -1 Deflater/BEST_SPEED true))
                                           (.setHandler (.getHandler server)))]
                        (.setHandler server gzip-handler)))
                    :port 8080
                    :async? true
                    :join? true}))
