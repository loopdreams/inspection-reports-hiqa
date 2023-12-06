(ns hiqa-reports.pdf-get
  (:require
   [clj-http.client :as http]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [hiqa-reports.hiqa-register :refer [hiqa-reg-tbl pdf-matcher]]))

(def destination-dir "inspection_reports/")
(def report-urls-list-dir "resources/report_urls_list/")

(defn- fetch-pdf!
  "makes an HTTP request and fetches the binary object"
  [url]
  (let [req (http/get url {:as :byte-array :throw-exceptions false})]
    (when (= (:status req) 200)
      (:body req))))

(defn- save-pdf!
  "downloads and stores the photo on disk"
  [pdf-link]
  (let [p (fetch-pdf! pdf-link)]
    (when (not (nil? p))
      (let [filename (second (str/split (re-find pdf-matcher pdf-link) #"/"))]
        (with-open [w (io/output-stream (str destination-dir filename))]
          (.write w p))))))

(def report-list
  (->> (:reports hiqa-reg-tbl)
       (map edn/read-string)
       flatten
       (remove nil?)))

(defn today-date []
  (.format (java.text.SimpleDateFormat. "yyyyMMdd")
           (java.util.Date.)))

(defn print-reports-list-at-date [report-list & date]
  (let [d (if date (first date) (today-date))]
    (let [fname (str report-urls-list-dir d "_report_list.txt")]
      (if (.exists (io/file fname))
        (throw (Exception. (str fname " already exists")))
        (let [data (str/join "\n" report-list)]
          (spit fname data))))))

(defn most-recent-reports-file []
  (->> (file-seq (io/file report-urls-list-dir))
       rest
       sort
       last
       str))

(comment
  (print-reports-list-at-date (take 5 report-list))
  (print-reports-list-at-date (take 6 report-list) "20231209"))

(defn diff-reports [report-list & reports-file]
  (let [file (if reports-file (first reports-file) (most-recent-reports-file))]
    (let [on-disk (str/split-lines (slurp file))]
      (set/difference (set on-disk) (set report-list)))))

(comment
  (diff-reports (take 5 report-list)))
  

(comment
  (count report-list)
  (time (pmap save-pdf! report-list)))

;; 3,153 Reports
;;
;; 500K size avg ...
;;
;; Around 1.5 GB total estimated
;;
;; Actual reports fetched: 3,098
;;
;; Sucess rate 98%
;;
;; Actual size on disk 1.18GB

;; Getting extra info about the reports from filenames

(def filename-date-matcher #"(\d{1,2})-(\w+)-(\d{4})")

(def report-list-by-year
  (reduce (fn [result report-url]
            (let [[_ _ _ year] (re-find filename-date-matcher report-url)]
              (if year
                (update result (keyword year) (fnil conj []) report-url)
                (update result :missing (fnil conj []) report-url))))
          {}
          report-list))
