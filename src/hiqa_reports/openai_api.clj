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

(defn build-responses-db! [json-f]
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
    (-> (tc/dataset new-DB)
        (tc/write! processed-reports-DB-f))))

;; Build db from responses json files (generated below)
(comment
  (add-responses-to-db! (str responses-dir "batch_11.json")))

(comment
  (map build-responses-db! (sort (rest (file-seq (io/file responses-dir))))))

(def batches
  (partition-all 100
                 (-> DS_pdf_info
                     (tc/select-columns [:centre-id :report-id :observations])
                     (tc/rows :as-maps))))

;; Fetching responses in batches
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
  (pmap #(fetch-and-log-response! %1 %2)
        [(nth batches 13)
         (nth batches 14)
         (nth batches 15)
         (nth batches 17)]
        [(str responses-dir "batch_13.json")
         (str responses-dir "batch_14.json")
         (str responses-dir "batch_15.json")
         (str responses-dir "batch_17.json")])
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



;; Validation/cleaning

(defn word-count [str]
  (count (str/split str #"\w+")))

(comment
  (word-count prompt)
  (map #(word-count (str (:observations %))) (nth batches 11))
  (nth (nth batches 11) 91))

;; The issue with the above entry seems to be an alternative heading for the 'observations'
;; section - "Views of people who use the service"
;; https://www.hiqa.ie/system/files?file=inspectionreports/5686-brookfield-17-february-2021.pdf
;; DONE Test whether this heading is used in other reports too...



;; TODO Delete later/move to analysis
(comment
  (reverse
   (sort-by val
            (frequencies
             (flatten
              (map edn/read-string
                   (-> (tc/dataset processed-reports-DB-f {:key-fn keyword})
                       (tc/select-rows #(= "positive" (% :rating)))
                       :phrases)))))))

;; TODO Delete later/move to analysis
(comment
  (reverse
   (sort-by val
            (frequencies
             (flatten
              (map edn/read-string
                   (-> (tc/dataset processed-reports-DB-f {:key-fn keyword})
                       (tc/select-rows #(= "neutral" (% :rating)))
                       :keywords)))))))


;; Validation - entry IDs empty ratings
(comment
  (-> (tc/dataset processed-reports-DB-f {:key-fn keyword})
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
(comment
  (loop [[x & xs] (drop 72 (json/parse-stream (io/reader (str responses-dir "batch_16.json")) true))
         c 72]
    (println c)
    (when x
      (extract-response x)
      (recur
       xs
       (inc c)))))

;; entry ids of failed indexes
(comment
  (let [e
        (nth (json/parse-stream (io/reader (str responses-dir "batch_16.json")) true) 71)]
    (-> e
        :response
        :choices
        first
        :message
        :content
        edn/read-string))
  (let [e (nth (json/parse-stream (io/reader (str responses-dir "batch_16.json")) true) 71)]
    (:report-id e)))

(comment
  (fix-malformed-string "Keywords: improvements, compliance, amenities, vehicles, renovations\nPhrases: significant improvements, high level of compliance, clean and homely, adequate fire safety systems, positive feedback\nOverall rating: positive\nSummary: The text describes a positive inspection of a residential center, highlighting significant improvements, high compliance, and positive feedback from residents. The center has amenities and vehicles to support residents, and the premises are clean and homely. The overall sentiment of the residents is positive, indicating a good quality and safe service."))


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

