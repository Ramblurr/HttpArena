(ns httparena.pedestal.core
  (:gen-class)
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [io.pedestal.http :as http]
   [io.pedestal.http.route :as route]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as jdbc.connection]
   [next.jdbc.result-set :as rs]
   [sqlite4clj.core :as sqlite]
   [ring.util.response :as response])
  (:import
   (java.io InputStream)
   (java.net URI)
   (java.util.zip Deflater)
   (org.eclipse.jetty.ee10.servlet ServletContextHandler)
   (org.eclipse.jetty.server.handler.gzip GzipHandler)
   (org.eclipse.jetty.util.compression DeflaterPool)
   (org.postgresql.util PGobject)))

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
   WHERE price BETWEEN ? AND ?
   LIMIT 50")
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

(defn database-url->jdbc-url [database-url]
  (let [uri (URI. ^String database-url)
        [username password] (when-let [user-info (.getUserInfo uri)]
                              (str/split user-info #":" 2))
        query-parts (cond-> []
                      (seq (.getQuery uri)) (conj (.getQuery uri))
                      username (conj (str "user=" username))
                      password (conj (str "password=" password)))]
    (str "jdbc:postgresql://" (.getHost uri)
         (let [port (.getPort uri)]
           (when-not (neg? port)
             (str ":" port)))
         (.getPath uri)
         (when (seq query-parts)
           (str "?" (str/join "&" query-parts))))))

(defn async-db-pool-size []
  (let [cpu-target (* 4 (.availableProcessors (Runtime/getRuntime)))
        max-conn (parse-long-safe (or (System/getenv "DATABASE_MAX_CONN") "256"))]
    (max 1 (int (min max-conn cpu-target)))))

(defn round2 [value]
  (/ (Math/round (* (double value) 100.0)) 100.0))

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

(defonce db
  (delay
    (when (.exists (io/file benchmark-db-path))
      (let [database (sqlite/init-db! benchmark-db-path
                                      {:pool-size (max 1 (.availableProcessors (Runtime/getRuntime)))
                                       :default-result-set-fn sqlite/qualified-keyword-result-set-fn})]
        (sqlite/q (:reader database) ["SELECT 1"])
        database))))

(defonce async-db (atom nil))

(defn compute-json-items [items]
  (mapv (fn [{:keys [price quantity] :as item}]
          (assoc item :total (round2 (* price quantity))))
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

(defn json-handler [_request]
  (if-let [source @dataset]
    (json-response 200 {:items (compute-json-items source)
                        :count (count source)})
    (text-response 500 "dataset.json not available")))

(defn compression-handler [_request]
  (if-let [body @compression-body]
    {:status 200
     :headers {"Content-Type" json-content-type}
     :body body}
    (text-response 500 "dataset-large.json not available")))

(defn upload-handler [request]
  (text-response 200 (str (count-stream-bytes (:body request)))))

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

(defn pg-tags->value [value]
  (cond
    (nil? value) []
    (vector? value) value
    (sequential? value) (vec value)
    (instance? PGobject value) (json/read-str (.getValue ^PGobject value))
    (string? value) (json/read-str ^String value)
    :else (json/read-str (str value))))

(defn postgres-row->item [row]
  {:id (:id row)
   :name (:name row)
   :category (:category row)
   :price (:price row)
   :quantity (:quantity row)
   :active (boolean (:active row))
   :tags (pg-tags->value (:tags row))
   :rating {:score (:rating_score row)
            :count (:rating_count row)}})

(defn init-async-db! []
  (when-let [database-url (System/getenv "DATABASE_URL")]
    (or @async-db
        (locking async-db
          (or @async-db
              (try
                (let [database (jdbc.connection/->pool 'hikari-cp
                                                       {:jdbc-url (database-url->jdbc-url database-url)
                                                        :maximum-pool-size (async-db-pool-size)
                                                        :minimum-idle 0
                                                        :read-only true
                                                        :connection-timeout 1000
                                                        :validation-timeout 1000})]
                  (jdbc/execute-one! database ["SELECT 1"])
                  (reset! async-db database))
                (catch Throwable _
                  nil)))))))

(defn db-handler [request]
  (let [query-params (:query-params request)
        min-price (parse-double-safe (get query-params :min) 10.0)
        max-price (parse-double-safe (get query-params :max) 50.0)
        items (if-let [database @db]
                (try
                  (mapv sqlite-row->item
                        (or (sqlite/q (:reader database) [db-query min-price max-price]) []))
                  (catch Throwable _
                    []))
                [])]
    (json-response 200 {:items items
                        :count (count items)})))

(defn async-db-handler [request]
  (let [query-params (:query-params request)
        min-price (parse-double-safe (get query-params :min) 10.0)
        max-price (parse-double-safe (get query-params :max) 50.0)
        database (init-async-db!)
        items (if database
                (try
                  (mapv postgres-row->item
                        (jdbc/execute! database
                                       [async-db-query min-price max-price]
                                       {:builder-fn rs/as-unqualified-lower-maps}))
                  (catch Throwable _
                    []))
                [])]
    (json-response 200 {:items items
                        :count (count items)})))

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

(def common-interceptors
  [route/query-params])

(def routes
  #{["/baseline11" :get (conj common-interceptors `baseline-handler) :route-name ::baseline-get]
    ["/baseline11" :post (conj common-interceptors `baseline-handler) :route-name ::baseline-post]
    ["/json" :get (conj common-interceptors `json-handler) :route-name ::json]
    ["/compression" :get (conj common-interceptors `compression-handler) :route-name ::compression]
    ["/db" :get (conj common-interceptors `db-handler) :route-name ::db]
    ["/async-db" :get (conj common-interceptors `async-db-handler) :route-name ::async-db]
    ["/upload" :post (conj common-interceptors `upload-handler) :route-name ::upload]
    ["/static/:filename" :get `static-handler :route-name ::static]
    ["/pipeline" :get (conj common-interceptors `pipeline-handler) :route-name ::pipeline]})

(def service
  {::http/routes routes
   ::http/type :jetty
   ::http/host "0.0.0.0"
   ::http/port 8080
   ::http/container-options
   {:context-configurator
    (fn [^ServletContextHandler context]
      (let [gzip-handler (doto (GzipHandler.)
                           (.setMinGzipSize 1)
                           (.setDeflaterPool (DeflaterPool. -1 Deflater/BEST_SPEED true)))]
        (.insertHandler context gzip-handler)
        context))}
   ::http/join? true})

(defn -main [& _args]
  (init-async-db!)
  (-> service
      http/create-server
      http/start))
