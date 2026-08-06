(ns httparena.http-kit.core
  (:gen-class)
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [org.httpkit.server :as http-kit]
   [ring.middleware.gzip :as gzip]
   [ring.middleware.params :as params]
   [ring.util.response :as response])
  (:import
   [io.vertx.core AsyncResult Handler Vertx]
   [io.vertx.pgclient PgBuilder PgConnectOptions]
   [io.vertx.sqlclient Pool PoolOptions Row RowSet Tuple]
   (java.io InputStream OutputStream)))

(set! *warn-on-reflection* true)

(def json-content-type "application/json")
(def max-request-body-bytes (* 32 1024 1024))
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

(defn parse-long-safe
  ([value]
   (parse-long-safe value 0))
  ([value default]
   (or (some-> value str str/trim not-empty parse-long)
       default)))

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

(defn json-items-response [request]
  (if-let [source @dataset]
    (let [uri            (:uri request)
          [_ path-count] (re-matches #"/json/([0-9]+)" uri)
          item-count     (if path-count (parse-long-safe path-count) (count source))
          multiplier     (parse-long-safe (get-in request [:params "m"]) 1)
          items          (take item-count source)]
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
                (let [database (-> (PgBuilder/pool)
                                   (.with (doto (PoolOptions.)
                                            (.setMaxSize (async-db-pool-size))))
                                   (.connectingTo (PgConnectOptions/fromUri database-url))
                                   (.using (Vertx/vertx))
                                   (.build))]
                  (reset! async-db database))
                (catch Throwable _
                  nil)))))))

(defn async-db-response [request]
  (let [params         (:params request)
        min-price      (parse-long-safe (get params "min" "10"))
        max-price      (parse-long-safe (get params "max" "50"))
        limit          (min 50 (max 1 (parse-long-safe (get params "limit" "50"))))
        ^Pool database (init-async-db!)]
    (if database
      (http-kit/as-channel
       request
       {:on-open (fn [channel]
                   (-> (.preparedQuery database async-db-query)
                       (.execute (Tuple/of min-price max-price limit))
                       (.onComplete
                        (reify Handler
                          (handle [_ result]
                            (let [^AsyncResult async-result result]
                              (http-kit/send!
                               channel
                               (if (.succeeded async-result)
                                 (let [^RowSet rows (.result async-result)
                                       items        (mapv vertx-row->item rows)]
                                   (json-response 200 {:items items
                                                       :count (count items)}))
                                 (json-response 200 {:items [] :count 0})))))))))})
      (json-response 200 {:items [] :count 0}))))

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

(defn app [request]
  (case (:uri request)
    "/baseline11" (text-response 200 (str (request-sum request)))
    "/async-db" (async-db-response request)
    "/upload" (text-response 200 (str (count-stream-bytes (:body request))))
    "/pipeline" (text-response 200 "ok")
    (if (re-matches #"/json/[0-9]+" (:uri request))
      (json-items-response request)
      (text-response 404 "not found"))))

(def handler
  (let [compressed-handler (-> app
                               params/wrap-params
                               gzip/wrap-gzip)]
    (fn [request]
      (if (str/starts-with? (:uri request) "/static/")
        (or (static-response (:uri request))
            (text-response 404 "not found"))
        (compressed-handler request)))))

(defn -main [& _args]
  (when-not (vector? @dataset)
    (throw (ex-info "dataset.json must contain a JSON array"
                    {:path "/data/dataset.json"})))
  (init-async-db!)
  (http-kit/run-server handler {:ip "0.0.0.0"
                                :max-body max-request-body-bytes
                                :port 8080})
  @(promise))
