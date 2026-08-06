(ns httparena.ring.core
  (:gen-class)
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.adapter.jetty :as jetty]
   [ring.middleware.params :as params]
   [ring.util.response :as response])
  (:import
   [io.vertx.core AsyncResult Handler Vertx]
   [io.vertx.pgclient PgBuilder PgConnectOptions]
   [io.vertx.sqlclient Pool PoolOptions Row RowSet Tuple]
   (java.io InputStream OutputStream)
   (org.eclipse.jetty.server Server)
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

(defn load-dataset [path]
  (when (.exists (io/file path))
    (json/read-str (slurp path) :key-fn keyword)))

(defonce dataset
  (delay (load-dataset "/data/dataset.json")))

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
   :headers {"content-type" json-content-type}
   :body (json/write-str body)})

(defn json-endpoint-response [request item-count]
  (if-let [source @dataset]
    (let [items (take (min 50 (parse-long-safe item-count)) source)
          multiplier (parse-long-safe (get (:params request) "m" 1))]
      (json-response 200 {:items (compute-json-items items multiplier)
                          :count (count items)}))
    (text-response 500 "dataset.json not available")))

(defn vertx-row->item [^Row row]
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
                (let [^Pool database (-> (PgBuilder/pool)
                                         (.with (doto (PoolOptions.)
                                                  (.setMaxSize (async-db-pool-size))))
                                         (.connectingTo (PgConnectOptions/fromUri database-url))
                                         (.using (Vertx/vertx))
                                         (.build))]
                  (reset! async-db database))
                (catch Throwable _
                  nil)))))))

(defn empty-db-response []
  (json-response 200 {:items [] :count 0}))

(defn respond-with-db-rows! [rows respond raise]
  (try
    (let [items (mapv vertx-row->item rows)]
      (respond (json-response 200 {:items items
                                   :count (count items)})))
    (catch Exception exception
      (raise exception))))

(defn complete-async-db! [^AsyncResult result respond raise]
  (if (.succeeded result)
    (let [^RowSet rows (.result result)]
      (respond-with-db-rows! rows respond raise))
    (respond (empty-db-response))))

(defn async-db-response [request respond raise]
  (let [params         (:params request)
        min-price      (int (parse-long-safe (get params "min" "10")))
        max-price      (int (parse-long-safe (get params "max" "50")))
        limit          (int (min 50 (max 1 (parse-long-safe (get params "limit" "50")))))
        ^Pool database (init-async-db!)]
    (if database
      (try
        (-> (.preparedQuery database async-db-query)
            (.execute (Tuple/of min-price max-price limit))
            (.onComplete
             (reify Handler
               (handle [_ result]
                 (complete-async-db! result respond raise)))))
        (catch Exception _
          (respond (empty-db-response))))
      (respond (empty-db-response)))))

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
   (let [uri    (:uri request)
         method (:request-method request)]
     (cond
       (str/starts-with? uri "/static/")
       (if (= :get method)
         (or (static-response uri)
             (text-response 404 "not found"))
         (method-not-allowed-response))

       :else
       (if-let [[_ item-count] (re-matches #"/json/(\d+)" uri)]
         (if (= :get method)
           (json-endpoint-response request item-count)
           (method-not-allowed-response))
         (case uri
           "/baseline11" (case method
                           (:get :post) (text-response 200 (str (request-sum request)))
                           (method-not-allowed-response))
           "/async-db" (if (= :get method)
                         (empty-db-response)
                         (method-not-allowed-response))
           "/upload" (if (= :post method)
                       (text-response 200 (str (count-stream-bytes (:body request))))
                       (method-not-allowed-response))
           "/pipeline" (if (= :get method)
                         (text-response 200 "ok")
                         (method-not-allowed-response))
           (text-response 404 "not found"))))))
  ([request respond raise]
   (try
     (if (and (= :get (:request-method request))
              (= "/async-db" (:uri request)))
       (async-db-response request respond raise)
       (respond (app request)))
     (catch Exception exception
       (raise exception)))))

(def handler
  (params/wrap-params app))

(defn -main [& _args]
  (when-not (vector? @dataset)
    (throw (ex-info "dataset.json must contain a JSON array"
                    {:path "/data/dataset.json"})))
  (init-async-db!)
  (jetty/run-jetty handler
                   {:host         "0.0.0.0"
                    :configurator (fn [^Server server]
                                    (let [gzip-handler (doto (GzipHandler.)
                                                         (.setExcludedPaths
                                                          (into-array String ["/static/*"]))
                                                         (.setHandler (.getHandler server)))]
                                      (.setHandler server gzip-handler)))
                    :port         8080
                    :join?        true
                    :async?       true}))
