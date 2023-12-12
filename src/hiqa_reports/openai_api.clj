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
  ["Summarize the following text into 5 keywords reflecting the sentiment of the residents. Do not include the word 'residents' as a keyword."
   "Also provide 3 key phrases reflecting the sentiment of the residents, and"
   "Also assign an overall rating of 'positive', 'negative' or 'neutral' based on these sentiments."
   "Finally, summarise the text in two sentences."
   "Return the answer in edn format, where keywords are a vector of strings, phrases are a vector of strings, "
   "and the rating and summary are strings. Like this: "
   "{:rating \"rating\" :keywords [\"kw1\" \"kw2\" ...] :phrases [\"phrase1\" \"phrease2\" ...] :summary \"summary\"}"
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
  (-> response
      :response
      :choices
      first
      :message
      :content
      edn/read-string))

(defn add-responses-to-db! [json-f]
  (let [DB    (if (.exists (io/file processed-reports-DB-f))
                (-> (tc/dataset processed-reports-DB-f {:key-fn keyword})
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
  (reverse
   (sort-by val
            (frequencies
             (flatten
              (map edn/read-string
                   (-> (tc/dataset processed-reports-DB-f {:key-fn keyword})
                       (tc/select-rows #(= "negative" (% :rating)))
                       :phrases)))))))

(comment
  (-> (tc/dataset processed-reports-DB-f {:key-fn keyword})
      (tc/group-by :rating)))


(comment
  (add-responses-to-db! (str responses-dir "batch_04.json")))

(comment
  ;; (fetch-and-log-response! (nth batches 1) (str responses-dir  "batch_01.json"))
  ;; (fetch-and-log-response! (nth batches 0) (str responses-dir  "batch_00.json"))
  ;; (fetch-and-log-response! (nth batches 2) (str responses-dir  "batch_02.json"))
  ;; (time
  ;;  (pmap #(fetch-and-log-response! %1 %2)
  ;;        [(nth batches 3)
  ;;         (nth batches 4)]
  ;;        [(str responses-dir  "batch_03.json")
  ;;         (str responses-dir  "batch_04.json")]))
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

;; Checking errors in GPT response
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

(comment
  (loop [[x & xs] (drop 0 (json/parse-stream (io/reader (str responses-dir "batch_03.json")) true))
         c 0]
    (println c)
    (when x
      (extract-response x)
      (recur
       xs
       (inc c)))))

(comment
  (let [e
        (nth (json/parse-stream (io/reader (str responses-dir "batch_04.json")) true) 31)]
    (-> e
        :response
        :choices
        first
        :message
        :content
        edn/read-string))
  (let [e (nth (json/parse-stream (io/reader (str responses-dir "batch_04.json")) true) 31)]
    (:report-id e)))


(comment
  (fix-malformed-string "Keywords: residents, centre, management, governance, safeguarding.\nPhrases: good quality service, residents' choices respected, friendly and caring staff.\nRating: Positive\nSummary: The residents of the centre in Co. Donegal are generally happy and satisfied with the service. The staff is friendly and caring, and the management is effective in ensuring the quality and safety of the service provided to the residents."))


;;
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

