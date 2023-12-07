(ns hiqa-reports.hiqa-register
  (:require [tablecloth.api :as tc]))

(def register-file "resources/disability_register.csv")
(def output-register-file "outputs/hiqa_register_with_report_urls.csv")

(def register-tbl
  (tc/dataset register-file {:key-fn keyword}))

(def url-root "https://www.hiqa.ie/system/files?file=")

(def pdf-matcher #"[a-zA-Z0-9./_-]+.pdf")

(defn- scrape-report-urls [center-url]
  (let [page (slurp center-url)
        pdf-matches? (re-seq pdf-matcher page)]
    (when (seq pdf-matches?)
      (mapv #(str url-root %) pdf-matches?))))

(defn scrape-all-reports [table csv-out]
  (-> table
      (tc/map-columns :reports :URL #(scrape-report-urls %))
      (tc/write! csv-out)))

(comment
  (time (scrape-all-reports register-tbl output-register-file)))

;; Elapsed time: 1101310.326875 msecs
;;


(def hiqa-reg-DB
  (tc/dataset "resources/hiqa_register_with_report_urls.csv" {:key-fn keyword}))
