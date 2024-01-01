(ns hiqa-reports.hiqa-register
  (:require
   [tablecloth.api :as tc]
   [clj-http.client :as http]))

(def register-file "resources/datasets/imported/disability_register.csv")
(def output-register-file "resources/datasets/created/hiqa_register_with_report_urls.csv")

(def register-tbl
  (tc/dataset register-file {:key-fn keyword}))

(def url-root "https://www.hiqa.ie/system/files?file=")

(def pdf-matcher #"[a-zA-Z0-9./_-]+.pdf")

(defn- fetch-page!
  [url]
  (let [req (http/get url {:throw-exceptions false})]
    (when (= (:status req) 200)
      (:body req))))

(defn- scrape-report-urls [center-url]
  (let [page (fetch-page! center-url)]
    (if page
      (let [pdf-matches? (re-seq pdf-matcher page)]
        (when (seq pdf-matches?)
          (mapv #(str url-root %) pdf-matches?)))
      (println (str "Error, page not found: " center-url)))))

(defn scrape-all-reports [table csv-out]
  (-> table
      (tc/map-columns :reports :URL #(scrape-report-urls %))
      (tc/write! csv-out)))

(comment
  (time (scrape-all-reports register-tbl output-register-file)))

;; Elapsed time: 1101310.326875 msecs
;; Elapsed time: 1379467.587375 msecs


(def DS-hiqa-register
  (tc/dataset output-register-file {:key-fn keyword}))

(-> DS-hiqa-register
    (tc/select-rows #(= (% :Centre_ID) 1507)))
