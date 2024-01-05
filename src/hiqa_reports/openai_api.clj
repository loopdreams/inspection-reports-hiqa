(ns hiqa-reports.openai-api
  (:require
   [cheshire.core :as json]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [hiqa-reports.parsers-writers :refer [DS_pdf_info DS_pdf_info_agg_compliance]]
   [tablecloth.api :as tc]
   [wkok.openai-clojure.api :as api]))

(def auth (:auth (edn/read-string (slurp "creds.edn"))))
(def responses-dir "GPT_responses/")

(def processed-reports-DB-f-out "resources/datasets/created/GPT_responses.csv")


;; DB OUTPUT

(def DS_sentiment
  (-> (tc/dataset processed-reports-DB-f-out
                  {:key-fn keyword})
      (tc/drop-missing :report-id)))

(def DS_joined_sentiment
  (-> DS_pdf_info
      (tc/full-join DS_sentiment :report-id)))

(def DS_joined_compliance
  (-> DS_pdf_info_agg_compliance
      (tc/full-join DS_sentiment :report-id)))



(def prompt-parts
  ["Summarize the following text into 5 keywords reflecting the sentiment of the residents. Do not include the word 'residents' as a keyword."
   "Also provide 3 key phrases reflecting the sentiment of the residents, and"
   "Also assign an overall rating of 'positive', 'negative' or 'neutral' based on these sentiments."
   "Finally, summarise the text in two sentences."
   "Return the answer in edn format, where keywords are a vector of strings, phrases are a vector of strings, "
   "and the rating and summary are strings. Like this: "
   "{:rating \"rating\" :keywords [\"kw1\" \"kw2\" ...] :phrases [\"phrase1\" \"phrease2\" ...] :summary \"summary\"}"
   "Here is the text:"])

(def prompt (str/join #" " prompt-parts))

(defn- request-keyword-summary [observations]
  (api/create-chat-completion {:model "gpt-3.5-turbo"
                               :messages [{:role "user" :content (str prompt observations)}]}
                              {:api-key auth}))

(defn- fetch-and-log-response! [input-entries output-f]
  (json/generate-stream
   (reduce (fn [result {:keys [observations] :as entry}]
             (let [response (request-keyword-summary observations)]
               (conj result
                     (-> entry
                         (dissoc :observations)
                         (assoc :response response)))))
           []
           input-entries)
   (clojure.java.io/writer output-f)))


(defn- make-DB-backup []
  (when (.exists (io/file processed-reports-DB-f-out))
    (io/copy (io/file processed-reports-DB-f-out)
             (io/file (str processed-reports-DB-f-out "_backup_"
                           (str (System/currentTimeMillis)))))))

(defn- extract-response [response]
  (-> response
      :response
      :choices
      first
      :message
      :content
      edn/read-string))

(defn- add-responses-to-db! [json-f]
  (let [DB    (if (.exists (io/file processed-reports-DB-f-out))
                (-> (tc/dataset processed-reports-DB-f-out {:key-fn keyword})
                    (tc/rows :as-maps))
                [])
        input (json/parse-stream (io/reader json-f) true)
        new-DB (into DB
                     (for [entry input
                           :let [{:keys [rating keywords phrases summary] :as response}
                                 (extract-response entry)]]
                       (-> entry
                           (dissoc :response)
                           (assoc :rating rating)
                           (assoc :keywords keywords)
                           (assoc :phrases phrases)
                           (assoc :summary summary))))]
    (do
      (make-DB-backup)
      (-> (tc/dataset new-DB)
          (tc/write! processed-reports-DB-f-out)))))

(defn- build-responses-db! [json-f]
  (let [DB    (if (.exists (io/file processed-reports-DB-f-out))
                (-> (tc/dataset processed-reports-DB-f-out {:key-fn keyword})
                    (tc/rows :as-maps))
                [])
        input (json/parse-stream (io/reader json-f) true)
        new-DB (into DB
                     (for [entry input
                           :let [{:keys [rating keywords phrases summary] :as response}
                                 (extract-response entry)]]
                       (-> entry
                           (dissoc :response)
                           (assoc :rating rating)
                           (assoc :keywords keywords)
                           (assoc :phrases phrases)
                           (assoc :summary summary))))]
    (-> (tc/dataset new-DB)
        (tc/write! processed-reports-DB-f-out))))




;; Build db from responses json files (generated below)
(comment
  (add-responses-to-db! (str responses-dir "updated_20231231.json")))

(comment
  (map build-responses-db! (sort (rest (file-seq (io/file responses-dir))))))

(def batches
  (partition-all 100
                 (-> DS_pdf_info
                     (tc/select-columns [:centre-id :report-id :observations])
                     (tc/rows :as-maps))))

(defn update-responses [ds_joined]
  (-> ds_joined
      (tc/select-rows #(= (% :rating) nil))
      (tc/select-columns [:centre-id :report-id :observations])
      (tc/rows :as-maps)))


(comment
  (fetch-and-log-response! (update-responses DS_joined_sentiment) (str responses-dir "updated_20231231.json")))

;; Fetching responses in batches
(comment)
  ;; (fetch-and-log-response! (nth batches 1) (str responses-dir  "batch_01.json"))
  ;; (fetch-and-log-response! (nth batches 0) (str responses-dir  "batch_00.json"))
  ;; (fetch-and-log-response! (nth batches 2) (str responses-dir  "batch_02.json"))
  ;; (time
  ;;  (pmap #(fetch-and-log-response! %1 %2)
  ;;        [(nth batches 3)
  ;;         (nth batches 4)]
  ;;        [(str responses-dir  "batch_03.json")
  ;;         (str responses-dir  "batch_04.json")]))
  ;;
  ;; (pmap #(fetch-and-log-response! %1 %2)
  ;;       [(nth batches 5)
  ;;        (nth batches 6)
  ;;        (nth batches 7)
  ;;        (nth batches 8)]
  ;;       [(str responses-dir  "batch_05.json")
  ;;        (str responses-dir  "batch_06.json")
  ;;        (str responses-dir  "batch_07.json")
  ;;        (str responses-dir  "batch_08.json")])
  ;;
  ;; (pmap #(fetch-and-log-response! %1 %2)
  ;;       [(nth batches 9)
  ;;        (nth batches 10)
  ;;        (nth batches 11)
  ;;        (nth batches 12)]
  ;;       [(str responses-dir  "batch_09.json")
  ;;        (str responses-dir  "batch_10.json")
  ;;        (str responses-dir  "batch_11.json")
  ;;        (str responses-dir  "batch_12.json")])


  ;; (fetch-and-log-response! (nth batches 11) (str responses-dir "batch_11.json"))
  ;; (pmap #(fetch-and-log-response! %1 %2)
  ;;       [(nth batches 15)
  ;;        (nth batches 17)]
  ;;       [(str responses-dir "batch_15.json")
  ;;        (str responses-dir "batch_17.json")])
  ;; (fetch-and-log-response! (nth batches 13) (str responses-dir "batch_13.json"))
  ;;

  ;; (fetch-and-log-response! (nth batches 19) (str responses-dir "batch_19.json"))
  ;; (fetch-and-log-response! (nth batches 20) (str responses-dir "batch_20.json"))
  ;; (fetch-and-log-response! (nth batches 21) (str responses-dir "batch_21.json"))
  ;; (fetch-and-log-response! (nth batches 22) (str responses-dir "batch_22.json"))
  ;; (fetch-and-log-response! (nth batches 23) (str responses-dir "batch_23.json"))
  ;; (fetch-and-log-response! (nth batches 24) (str responses-dir "batch_24.json"))
  ;; (fetch-and-log-response! (nth batches 25) (str responses-dir "batch_25.json"))
  ;; (fetch-and-log-response! (nth batches 26) (str responses-dir "batch_26.json"))
  ;; (fetch-and-log-response! (nth batches 27) (str responses-dir "batch_27.json"))
  ;; (time
  ;;  (fetch-and-log-response! (nth batches 28) (str responses-dir "batch_28.json")))
  ;; (fetch-and-log-response! (nth batches 29) (str responses-dir "batch_29.json"))
  ;; (fetch-and-log-response! (nth batches 30) (str responses-dir "batch_30.json")))

;;Batch1: "Elapsed time: 384379.147459 msecs" 6+ minutes
;;Cost 18c
;; Estimated total $5.58
;; Actual total $6.69 (including some trail and error costs...)
;; Elapsed time: 299130.325958 msecs (different internet connection)


;; Validation/cleaning

(defn word-count [str]
  (count (str/split str #"\w+")))

(comment
  (word-count prompt)
  (map #(word-count (str (:observations %))) (nth batches 13))
  (nth (nth batches 11) 91))

;; The issue with the above entry seems to be an alternative heading for the 'observations'
;; section - "Views of people who use the service"
;; https://www.hiqa.ie/system/files?file=inspectionreports/5686-brookfield-17-february-2021.pdf
;; DONE Test whether this heading is used in other reports too...



;; Validation - entry IDs empty ratings
(comment
  (-> (tc/dataset processed-reports-DB-f-out {:key-fn keyword})
      (tc/select-rows #(= (% :rating) nil))
      :report-id))


;; Fixing a common error pattern
(defn fix-malformed-string [s]
  (let [[_ kws phrs rate summ] (str/split
                                (str/replace s #"\n" " ")
                                #"Keywords: |Phrases: |Rating: |Summary: ")
        vecs (fn [s]
               (str "[" (str/join #" "
                                  (for [x (map str/trim (str/split (str/replace  s #"\." "") #","))]
                                    (str "\"" x "\"")))
                      "] "))
        kws (vecs kws)
        phrs (vecs phrs)
        rate (str "\"" (str/trim (str/lower-case (str/replace rate #"\." ""))) "\" ")
        summ (str "\"" summ "\"")]
    (str "{"
           ":keywords " kws
           ":phrases " phrs
           ":rating " rate
           ":summary " summ
           "}")))

;; loop through entries and check which fail to render as edn

(defn print-error-idx [start-at name]
  (loop [[x & xs] (drop start-at (json/parse-stream (io/reader (str responses-dir name ".json")) true))
         c start-at]
    (println c)
    (when x
      (extract-response x)
      (recur
       xs
       (inc c)))))

(comment
  (print-error-idx 0 "updated_20231231"))

;; entry ids of failed indexes
(comment
  (let [e
        (nth (json/parse-stream (io/reader (str responses-dir "batch_14.json")) true) 71)]
    (-> e
        :response
        :choices
        first
        :message
        :content
        edn/read-string))
  (let [e (nth (json/parse-stream (io/reader (str responses-dir "batch_30.json")) true) 95)]
    (:report-id e)))

(comment
  (fix-malformed-string "Keywords: \n1. Quality of life\n2. Staff turnover \n3. Independence \n4. Improvements required \n5. Compatibility \n\nPhrases: \n1. \"Residents living in the centre had a good quality of life\"\n2. \"Significant turnover of staff in the centre\"\n3. \"Residents were happy living in the centre\"\n\nOverall rating: Positive \n\nSummary: The residents in the centre had a good quality of life and were happy with the care being provided. However, there were concerns regarding staff turnover and improvements needed in certain areas."))


;; batch 30
;; 10 28 95
;; batch 29 - ok
;; batch 28
;; 45 87
;; batch 27
;; 29
;; batch 26
;; 53 80
;; batch 25 - ok
;; batch 24 -ok
;; batch 23 -ok
;; batch 22
;; 45
;; batch 21
;; 24 99
;; batch 20
;; 27 53 65
;; batch 19 - ok
;; batch 13 - ok
;; batch 17
;; 10 55 65
;; batch 15
;; 80
;; batch 18
;; 94
;; batch 14
;; 45 65
;; batch 16
;; 4 71
;; batch 11 - ok
;; batch 12 - ok
;;
;; batch 10 - done
;; 25 51 62
;;
;; batch 09 - ok
;;
;; batch 08 - done
;; 1 81
;;
;; batch 07 - none
;;
;; batch 06 - done
;; 4 87
;;
;; batch 05 - done
;; 31
;;
;; batch 04 - done
;; 31
;;
;; batch 03 - done
;; 13 93
;;
;; batch 02 - done
;; errors at 41 44 75
;;
;; batch 01 - done
;; errors at 48 50
;;
;; entry 48 has no closing '}' after the 'summary, and no escaped strings - report-id "4442-20211123"
;; entry 50 does not enclose summary in quotes "" - report-id "3941-20220825"

;; batch 00 - done
;; errors at 44 57

