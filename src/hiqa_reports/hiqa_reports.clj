(ns hiqa-reports.hiqa-reports
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [pdfboxing.text :as text]
   [tablecloth.api :as tc]))

;; For processing the pdf of inspection reports into following format:
;;   {:report-id(custom)
;;    :centre-id int
;;    :frontmatter {:name ...}
;;    :no-of-residents-present int
;;    :compliance {:regulation compliance-level}
;;    :observations string}}


(def reports-directory (io/file "inspection_reports/"))

(def all-reports-pdfs (rest (file-seq reports-directory)))

(def frontmatter-fields ["Name of designated centre:"
                         "Name of provider:"
                         "Address of centre:"
                         "Type of inspection:"
                         "Date of inspection:"
                         "Centre ID:"
                         "Fieldwork ID:"])

(def frontmatter-fields-keywords [:name-of-designated-centre
                                  :name-of-provider
                                  :address-of-centre
                                  :type-of-inspection
                                  :date-of-inspection
                                  :centre-ID-OSV
                                  :fieldwork-ID])

;; Regulations info - https://www.hiqa.ie/sites/default/files/2018-02/Assessment-of-centres-DCD_Guidance.pdf

(def hiqa-regulations
  {:capacity-and-capability
   {3 "Statement of purpose"
    4 "Written policies and procedures"
    14 "Person in charge"
    15 "Staffing"
    16 "Training and staff development"
    19 "Directory of residents"
    21 "Records"
    22 "Insurance"
    23 "Governance and management"
    24 "Admissions and contract for the provision of services"
    30 "Volunteers"
    31 "Notification of incidets"
    32 "Notifications of periods when person in charge is absent"
    33 "Notifications of procedures and arrangements for periods when person in charge is absent"
    34 "Complaints procedure"}
   :quality-and-safety
   {5 "Individualised assessment and personal plan"
    6 "Healthcare"
    7 "Positive behaviour support"
    8 "Protection"
    9 "Residents' rights"
    10 "Communication"
    11 "Visits"
    12 "Personal possessions"
    13 "General welfare and development"
    17 "Premises"
    18 "Food and nutrition"
    20 "Information for residents"
    25 "Temporary absence, transition and discharge of residents"
    26 "Risk management procedures"
    27 "Protections against infection"
    28 "Fire precautions"
    29 "Medicines and pharmaceutical services"}})

(def capacity-and-capability-nos (keys (:capacity-and-capability hiqa-regulations)))

(def quality-and-safety-nos (keys (:quality-and-safety hiqa-regulations)))

;; Parsing functions

(defn reformat-frontm-id [osvID]
  (->> osvID
       reverse
       (take 4)
       reverse
       (apply str)))


(defn generate-report-id
  "Generate custom report id, based on center ID joined with date of report yyyyMMdd.
  Date input is based on report frontmatter, example '01 January 1900'

  In cases where two dates are listed (eg 08 and 09 of June), second date is used."

  [date centre-id]
  (when date
    (let [d1 (if (re-find #"and" date)
               (second (str/split date #"and "))
               date)
          d2 (.format (java.text.SimpleDateFormat. "yyyyMMdd")
                      (.parse (java.text.SimpleDateFormat. "dd MMMM yyyy")
                              d1))]
      (str centre-id "-" d2))))

(comment
  (generate-report-id "01 December 2021" "7890"))

(defn parse-frontmatter [pdf-text]
  (let [page-1 (first (rest (str/split pdf-text #"Page")))
        info (->> frontmatter-fields
                  (str/join "|")
                  re-pattern
                  (str/split (str/replace page-1 #"\n" ""))
                  reverse
                  (take (count frontmatter-fields))
                  reverse
                  (map str/trim))]
    (zipmap frontmatter-fields-keywords info)))

(defn number-of-residents-present [pdf-text]
  (let [[_ no-of-residents] (str/split pdf-text #"Number of residents on the \ndate of inspection:")]
    (when no-of-residents
      (parse-long (re-find #"\d+" no-of-residents)))))


(defn what-residents-told-us [pdf-text]
  (str/replace
   (str/trim
    (str/replace
     (second
      (str/split pdf-text #"What residents told us and what inspectors observed \n|Capacity and capability \n")) #"\n" ""))
   #"  Page \d+ of \d+  " ""))

(defn lookup-reg
  "Capturing the area too (capacity or quality)"
  [reg-no]
  (let [candidate-a (get (:capacity-and-capability hiqa-regulations) reg-no)
        candidate-b (get (:quality-and-safety hiqa-regulations) reg-no)]
    (if candidate-a
      [:capacity-and-capability candidate-a]
      [:quality-and-safety candidate-b])))


(defn parse-compliance-table [pdf-text]
  (let [match (or
               (re-find #"Regulation Title(.*)Compliance Plan"
                        (str/replace pdf-text #"\n" " "))
               (re-find #"Regulation Title(.*)" ;; For cases where compliance table is at end of report
                        (str/replace pdf-text #"\n" " ")))]
    (when match
      (for [line (map str/trim (str/split (first match) #"Regulation"))

            :when (int? (parse-long (str (first line))))

            :let [[_ reg-no _ judgement] (first (re-seq #"(\d{1,2}):(.*)(Substantially|Not|Compliant)" line))
                  [area reg]  (when reg-no (lookup-reg (parse-long reg-no)))
                  full-reg (str "Regulation " reg-no ": " reg)
                  full-judgement (case judgement
                                   "Substantially" "Substantially compliant"
                                   "Not"           "Not compliant"
                                   "Compliant"     "Compliant"
                                   :error)]]
        {:area       area
         :reg-no     (when reg-no (parse-long reg-no))
         :regulation full-reg
         :judgement  full-judgement}))))

(defn parse-compliance-table-alt [pdf-text]
  (let [match (or
               (re-find #"Regulation Title(.*)Compliance Plan"
                        (str/replace pdf-text #"\n" " "))
               (re-find #"Regulation Title(.*)" ;; For cases where compliance table is at end of report
                        (str/replace pdf-text #"\n" " ")))]
    (when match
      (into (sorted-map)
            (for [line (map str/trim (str/split (first match) #"Regulation"))

                  :when (int? (parse-long (str (first line))))

                  :let [[_ reg-no _ judgement] (first (re-seq #"(\d{1,2}):(.*)(Substantially|Not|Compliant)" line))
                        reg-label (str "Regulation " reg-no)
                        full-judgement (case judgement
                                         "Substantially" "Substantially compliant"
                                         "Not"           "Not compliant"
                                         "Compliant"     "Compliant"
                                         :error)]]
              [reg-label full-judgement])))))

(comment
  (map (comp parse-compliance-table-alt text/extract) (take 5 all-reports-pdfs)))



(defn process-pdf [pdf-file]
  (let [text              (text/extract pdf-file)
        frontmatter       (parse-frontmatter text)
        centre-id         (reformat-frontm-id (:centre-ID-OSV frontmatter))
        report-id         (generate-report-id (:date-of-inspection frontmatter)
                                              centre-id)
        residents-present (number-of-residents-present text)
        compliance        (parse-compliance-table text)
        observations      (what-residents-told-us text)]
    (merge frontmatter
           {:report-id report-id
            :centre-id (parse-long centre-id)
            :number-of-residents-present residents-present
            :compliance-levels compliance
            :observations observations})))

(defn process-pdf-alt [pdf-file]
  (let [text              (text/extract pdf-file)
        frontmatter       (parse-frontmatter text)
        centre-id         (reformat-frontm-id (:centre-ID-OSV frontmatter))
        report-id         (generate-report-id (:date-of-inspection frontmatter)
                                              centre-id)
        residents-present (number-of-residents-present text)
        compliance        (parse-compliance-table-alt text)
        observations      (what-residents-told-us text)]
    (merge
     frontmatter
     compliance
     {:report-id report-id
      :centre-id (parse-long centre-id)
      :number-of-residents-present residents-present
      :observations observations})))




(defn reg-map-cols [type]
  (fn [rows]
    (reduce (fn [acc r]
              (if (= r type)
                (inc acc)
                acc))
            0
            rows)))

((reg-map-cols "Compliant") ["Compliant"])

(comment
  (process-pdf-alt (first all-reports-pdfs))
  (time
   (let [all-info (pmap process-pdf all-reports-pdfs)]
     (json/generate-stream all-info (io/writer "outputs/pdf_info.json"))))
  (time
   (let [all-info (pmap process-pdf-alt all-reports-pdfs)]
     (json/generate-stream all-info (io/writer "outputs/pdf_info_alt.json"))))
  (-> (tc/dataset "outputs/pdf_info_alt.json")
      (tc/drop-columns ["observations" "fieldwork-ID"])
      (tc/reorder-columns ["centre-id" "centre-ID-OSV"
                           "report-id"
                           "name-of-designated-centre"
                           "address-of-centre"
                           "name-of-provider"
                           "type-of-inspection"
                           "number-of-residents-present"])
      (tc/write! "outputs/pdf_info_alt.csv"))
  (-> (tc/dataset "outputs/pdf_info_alt.csv")
      (tc/column-names #"Regulation.*"))
  (let [DS (tc/dataset "outputs/pdf_info_alt.csv")]
    (-> DS
        (tc/map-columns :num-compliant
                        (tc/column-names DS #"Regulation.*")
                        (fn [& rows]
                          ((reg-map-cols "Compliant") rows)))
        (tc/map-columns :num-notcompliant
                        (tc/column-names DS #"Regulation.*")
                        (fn [& rows]
                          ((reg-map-cols "Not compliant") rows)))
        (tc/map-columns :num-substantiallycompliant
                        (tc/column-names DS #"Regulation.*")
                        (fn [& rows]
                          ((reg-map-cols "Substantially compliant") rows)))
        (tc/write! "outputs/pdf_info_reglevels.csv"))))

(comment
  (-> (tc/dataset "outputs/pdf_info_reglevels.csv")
      (tc/map-columns :total ["num-compliant" "num-notcompliant" "num-substantiallycompliant"]
                      (fn [& rows]
                        (reduce + rows)))
      (tc/map-columns :percent-non ["num-notcompliant" :total]
                      (fn [not tot]
                        (if (zero? not) 0
                            (* 100 (float (/ not tot))))))
      (tc/group-by "name-of-provider")
      (tc/aggregate #(float (/ (reduce + (% :percent-non)) (count (% :percent-non)))))
      (tc/order-by ["summary"] [:desc])))

(comment
  (-> (tc/dataset "outputs/pdf_info_reglevels.csv")
      (tc/map-columns :total ["num-compliant" "num-notcompliant" "num-substantiallycompliant"]
                      (fn [& rows]
                        (reduce + rows)))
      (tc/map-columns :percent-com ["num-compliant" :total]
                      (fn [not tot]
                        (if (zero? not) 0
                            (* 100 (float (/ not tot))))))
      (tc/group-by "name-of-provider")
      (tc/aggregate #(float (/ (reduce + (% :percent-com)) (count (% :percent-com)))))
      (tc/order-by ["summary"] [:desc])
      (tc/print-dataset {:print-line-policy :repl})))


;; Number of inspections per centre:
(frequencies (map (comp count second)
                  (-> (tc/dataset "outputs/pdf_info_alt.csv")
                      (tc/group-by "centre-id" {:result-type :as-indexes}))))

;; Elapsed time: 191234.397125 msecs
;; Elapsed time: 190448.940459 msecs

(def pdf-info (json/parse-string (slurp "outputs/pdf_info.json") true))

;; Utilities
(defn keywordise-when-spaces [string]
  (if (string? string)
    (keyword (str/lower-case (str/replace string #" " "")))
    string))

;; Drafts for analysis

;; Compliance Levels per reg

(defn judgement-for-reg-no [entry reg-no]
  (:judgement (first (filter #(= reg-no (:reg-no %)) (:compliance-levels entry)))))

(defn compliance-levels-for-reg [entries reg-no]
  (reduce (fn [result entry]
            (let [id (:report-id entry)]
              (assoc result (keyword id) (judgement-for-reg-no entry reg-no))))
          {}
          entries))

(defn compliance-levels-table [entries area area-nos]
  (reduce (fn [tbl reg-no]
            (conj tbl
                  (let [area-name area
                        counts (set/rename-keys (frequencies (vals (compliance-levels-for-reg entries reg-no)))
                                                {nil :unchecked})]
                    (into counts
                          {:area area-name
                           :regulation (get (area-name hiqa-regulations) reg-no)}))))
          []
          area-nos))


(defn compliance-levels-table-quality [entries]
  (compliance-levels-table entries :quality-and-safety quality-and-safety-nos))

(defn compliance-levels-table-capacity [entries]
  (compliance-levels-table entries :capacity-and-capability capacity-and-capability-nos))

(defn make-compliance-tc-table [entries data-fn]
  (let [data (data-fn entries)]
    (-> data
        (tc/dataset {:key-fn keywordise-when-spaces})
        (tc/reorder-columns [:area :regulation :compliant]))))


;; Compliance grouping
;;
(defn compliance-levels-by-frontmatter-area [entries frontmatter-area reg-no]
  (reduce (fn [result entry]
            (let [group      (get (:frontmatter entry) frontmatter-area)
                  compliance (compliance-levels-for-reg entry reg-no)]
              (update-in result [group compliance] (fnil + 0) 1)))
          {}
          entries))

(defn make-regulation-table
  "Group is either :provider, :area or :id"
  [entries reg-no group]
  (let [extracts
        (reduce (fn [table entry]
                  (conj table
                        (let [fm        (:frontmatter entry)
                              judgement (judgement-for-reg-no entry reg-no)]
                          {:id                      (:centre-id entry)
                           :report-id               (:report-id entry)
                           :provider                (:name-of-provider fm)
                           :area                    (:address-of-centre fm)
                           :compliant               (if (= judgement "Compliant") 1 0)
                           :notcompliant           (if (= judgement "Not compliant") 1 0)
                           :substantiallycompliant (if (= judgement "Substantially compliant") 1 0)})))
                []
                entries)]
    (-> extracts
        tc/dataset
        (tc/group-by group)
        (tc/aggregate {:compliant #(reduce + (% :compliant))
                       :notcompliant #(reduce + (% :notcompliant))
                       :substantiallycompliant #(reduce + (% :substantiallycompliant))})
        (tc/rename-columns {:$group-name group}))))


(comment
  (->
   (make-regulation-table pdf-info 23 :area)
   (tc/order-by [:not-compliant] [:desc])))


;; Frequencies

(defn observations-by-report-id [entries]
  (reduce #(assoc %1 (keyword (:report-id %2)) (:observations %2)) {} entries))

(def stopwords
  (str/split-lines
   (slurp "https://gist.githubusercontent.com/sebleier/554280/raw/7e0e4a1ce04c2bb7bd41089c9821dbcf6d0c786c/NLTK's%2520list%2520of%2520english%2520stopwords")))

;; (reverse
;;  (sort-by val
;;           (frequencies
;;            (remove (set stopwords)
;;                    (map str/lower-case
;;                         (str/split
;;                          (str/replace
;;                           (str/join (vals (observations-by-report-id test-group)))
;;                           #"\.|," "")
;;                          #"\s+"))))))

(comment
  (number-of-residents-present (text/extract (second all-reports-pdfs)))
  (what-residents-told-us (text/extract (first all-reports-pdfs)))
  (reformat-frontm-id (get (parse-frontmatter (text/extract (last all-reports-pdfs))) "Centre ID:")))
