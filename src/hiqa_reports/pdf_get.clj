(ns hiqa-reports.pdf-get
  (:require
   [clj-http.client :as http]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [hiqa-reports.hiqa-register :refer [hiqa-reg-DB pdf-matcher scrape-all-reports output-register-file register-tbl]]))

(def destination-dir "inspection_reports/")
(def report-urls-list-dir "resources/report_urls_list/")

(def report-list
  (->> (:reports hiqa-reg-DB)
       (map edn/read-string)
       flatten
       (remove nil?)))

(defn- fetch-pdf!
  "makes an HTTP request and fetches the binary object"
  [url]
  (let [req (http/get url {:as :byte-array :throw-exceptions false})]
    (when (= (:status req) 200)
      (:body req))))

(defn- save-pdf!
  "downloads and stores the pdf to disk"
  [pdf-link]
  (let [p (fetch-pdf! pdf-link)]
    (when (not (nil? p))
      (let [filename (second (str/split (re-find pdf-matcher pdf-link) #"/"))]
        (with-open [w (io/output-stream (str destination-dir filename))]
          (.write w p))))))

(defn- today-date []
  (.format (java.text.SimpleDateFormat. "yyyyMMdd")
           (java.util.Date.)))

(defn- print-reports-list-at-date [report-list & date]
  (let [d (if date (first date) (today-date))]
    (let [fname (str report-urls-list-dir d "_report_list.txt")]
      (if (.exists (io/file fname))
        (throw (Exception. (str fname " already exists")))
        (let [data (str/join "\n" report-list)]
          (spit fname data))))))

(defn- most-recent-reports-file []
  (->> (file-seq (io/file report-urls-list-dir))
       rest
       sort
       last
       str))

(defn- diff-reports [report-list & reports-file]
  (let [file (if reports-file (first reports-file) (most-recent-reports-file))]
    (let [on-disk (str/split-lines (slurp file))]
      (into [] (set/difference (set on-disk) (set report-list))))))

(defn- update-pdfs! [& reports-file]
  (pmap save-pdf! (diff-reports report-list reports-file)))

;; update the DB
(comment
  (scrape-all-reports register-tbl output-register-file)

  (update-pdfs!))



;; Additional analysis fns
(def filename-date-matcher #"(\d{1,2})-(\w+)-(\d{4})")

(def report-list-by-year
  (reduce (fn [result report-url]
            (let [[_ _ _ year] (re-find filename-date-matcher report-url)]
              (if year
                (update result (keyword year) (fnil conj []) report-url)
                (update result :missing (fnil conj []) report-url))))
          {}
          report-list))
