(ns hiqa-reports.pdf-scrape
  (:require
   [clj-http.client :as http]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [hiqa-reports.hiqa-register :refer [hiqa-reg-tbl pdf-matcher]]))

(def destination-dir "inspection_reports/")

(:body (http/get
        (first
         (-> hiqa-reg-tbl
             :reports
             first
             clojure.edn/read-string))))

(defn download [uri file]
  (with-open [in (io/input-stream uri)
              out (io/output-stream file)]
    (io/copy in out)))

(defn download-entry [uri]
  (let [filename (second (str/split (re-find pdf-matcher uri) #"/"))]
    (download uri (str destination-dir filename))))

(def report-list
  (->> (:reports hiqa-reg-tbl)
       (map edn/read-string)
       flatten
       (remove nil?)))

(comment
  (count report-list))

;; 3,153 Reports
;; 500K size avg ...
;; Around 1.5 GB total estimated


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

