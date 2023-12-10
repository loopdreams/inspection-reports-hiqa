(ns hiqa-reports.openai-api
  (:require
   [cheshire.core :as json]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [hiqa-reports.parsers-writers :refer [DS_pdf_info pdf-info-DB]]
   [tablecloth.api :as tc]
   [wkok.openai-clojure.api :as api]))

(def auth (:auth (edn/read-string (slurp "creds.edn"))))
(def output-f "outputs/GPT_response.csv")
(def responses-dir "GPT_responses/")

(def processed-reports-DB-f "outputs/GPT_responses.csv")

(def prompt-parts
  ["Summarize the following into 5 keywords reflecting the sentiment of the residents."
   "Also summarise 3 key phrases reflecting sentiment."
   "Also assign an overall rating of 'positive', 'negative' or 'neutral' based on these sentiments."
   "Summarise the text in two sentences."
   "Return the answer in edn format"
   "{:rating rating :keywords [kw1 kw2 ...] :phrases [ph1 ph2 ...] :summary summary}."
   "Here is the text:"])

(def prompt (str/join #" " prompt-parts))

(defn request-keyword-summary [observations]
  (api/create-chat-completion {:model "gpt-3.5-turbo"
                               :messages [{:role "user" :content (str prompt observations)}]}
                              {:api-key auth}))

(defn fetch-and-log-response! [input-entries output-f]
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


(comment
  (fetch-and-log-response! (take 3 (-> DS_pdf_info
                                       (tc/select-columns [:centre-id :report-id :observations])
                                       (tc/rows :as-maps)))
                           "test2.json"))

(defn make-DB-backup []
  (when (.exists (io/file processed-reports-DB-f))
    (io/copy (io/file processed-reports-DB-f)
             (io/file (str processed-reports-DB-f "_backup_"
                           (str (System/currentTimeMillis)))))))


(defn extract-response [response]
  (or
   (-> response
       :response
       :choices
       first
       :message
       :content
       edn/read-string)))

(defn add-responses-to-db! [json-f]
  (let [DB    (if (.exists (io/file processed-reports-DB-f))
                (-> (tc/dataset processed-reports-DB-f)
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
          (tc/write! processed-reports-DB-f)))))

(def batches
  (partition-all 100
                 (-> DS_pdf_info
                     (tc/select-columns [:centre-id :report-id :observations])
                     (tc/rows :as-maps))))

(comment
  (add-responses-to-db! (str responses-dir "batch_01.json")))


(comment
  ;; (fetch-and-log-response! (nth batches 1) (str responses-dir  "batch_01.json"))
  (fetch-and-log-response! (nth batches 0) (str responses-dir  "batch_00.json"))
  (fetch-and-log-response! (nth batches 2) (str responses-dir  "batch_02.json"))
  (fetch-and-log-response! (nth batches 3) (str responses-dir  "batch_03.json"))
  (fetch-and-log-response! (nth batches 4) (str responses-dir  "batch_04.json"))
  (fetch-and-log-response! (nth batches 5) (str responses-dir  "batch_05.json"))
  (fetch-and-log-response! (nth batches 6) (str responses-dir  "batch_06.json"))
  (fetch-and-log-response! (nth batches 7) (str responses-dir  "batch_07.json"))
  (fetch-and-log-response! (nth batches 8) (str responses-dir  "batch_08.json"))
  (fetch-and-log-response! (nth batches 9) (str responses-dir  "batch_09.json"))
  (fetch-and-log-response! (nth batches 10) (str responses-dir "batch_10.json"))
  (fetch-and-log-response! (nth batches 11) (str responses-dir "batch_11.json"))
  (fetch-and-log-response! (nth batches 12) (str responses-dir "batch_12.json"))
  (fetch-and-log-response! (nth batches 13) (str responses-dir "batch_13.json"))
  (fetch-and-log-response! (nth batches 14) (str responses-dir "batch_14.json"))
  (fetch-and-log-response! (nth batches 15) (str responses-dir "batch_15.json"))
  (fetch-and-log-response! (nth batches 16) (str responses-dir "batch_16.json"))
  (fetch-and-log-response! (nth batches 17) (str responses-dir "batch_17.json"))
  (fetch-and-log-response! (nth batches 18) (str responses-dir "batch_18.json"))
  (fetch-and-log-response! (nth batches 19) (str responses-dir "batch_19.json"))
  (fetch-and-log-response! (nth batches 20) (str responses-dir "batch_20.json"))
  (fetch-and-log-response! (nth batches 21) (str responses-dir "batch_21.json"))
  (fetch-and-log-response! (nth batches 22) (str responses-dir "batch_22.json"))
  (fetch-and-log-response! (nth batches 23) (str responses-dir "batch_23.json"))
  (fetch-and-log-response! (nth batches 24) (str responses-dir "batch_24.json"))
  (fetch-and-log-response! (nth batches 25) (str responses-dir "batch_25.json"))
  (fetch-and-log-response! (nth batches 26) (str responses-dir "batch_26.json"))
  (fetch-and-log-response! (nth batches 27) (str responses-dir "batch_27.json"))
  (fetch-and-log-response! (nth batches 28) (str responses-dir "batch_28.json"))
  (fetch-and-log-response! (nth batches 29) (str responses-dir "batch_29.json"))
  (fetch-and-log-response! (nth batches 30) (str responses-dir "batch_30.json")))


;;Batch1: "Elapsed time: 384379.147459 msecs" 6+ minutes
;;Cost 18c
;; Estimated total $5.58
