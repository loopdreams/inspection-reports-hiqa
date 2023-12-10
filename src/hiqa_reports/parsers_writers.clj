(ns hiqa-reports.parsers-writers
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [pdfboxing.text :as text]
   [tablecloth.api :as tc]
   [java-time.api :as jt]))

(def reports-directory (io/file "inspection_reports/"))

(def all-reports-pdfs (rest (file-seq reports-directory)))

(def full-json-out "outputs/pdf_data_full.json")
(def json-out "outputs/pdf_data.json")
(def full-csv-out "outputs/pdf_data_full.csv")

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
;;
;; DATE

(defn- DOI->dateobj
  "In cases where there are two inspection days, chooses second one."
  [date-of-inspection]
  (when date-of-inspection
    (let [dt-str
          (if (re-find #"and|&" date-of-inspection)
            (second (str/split date-of-inspection #"and "))
            date-of-inspection)]
      (when dt-str
        (jt/local-date "ddMMMMyyyy" (str/replace dt-str #" " ""))))))

(defn- reformat-frontm-id
  "Extract ID compatible with 'centre-id' from the longer IDs used in the reports"
  [osvID]
  (->> osvID
       reverse
       (take 4)
       reverse
       (apply str)))

(defn- generate-report-id
  "Generate custom report id, based on center ID joined with date of report yyyyMMdd.
  Date input is based on report frontmatter, example '01 January 1900'  "
  
  [date centre-id]
  (when date
    (str centre-id "-" (jt/format "yyyyMMdd" date))))

(defn- parse-frontmatter [pdf-text]
  (let [page-1 (first (rest (str/split pdf-text #"Page")))
        info   (->> frontmatter-fields
                    (str/join "|")
                    re-pattern
                    (str/split (str/replace page-1 #"\n" ""))
                    reverse
                    (take (count frontmatter-fields))
                    reverse
                    (map str/trim))]
    (zipmap frontmatter-fields-keywords info)))

(defn- number-of-residents-present [pdf-text]
  (let [[_ no-of-residents] (str/split pdf-text
                                       #"Number of residents on the \ndate of inspection:")]
    (when no-of-residents
      (parse-long (re-find #"\d+" no-of-residents)))))

(defn- what-residents-told-us [pdf-text]
  (str/replace
   (str/trim
    (str/replace
     (second
      (str/split pdf-text
                 #"What residents told us and what inspectors observed \n|Capacity and capability \n")) #"\n" ""))
   #"  Page \d+ of \d+  " ""))

(defn- lookup-reg
  "Capturing the area too (capacity or quality)"
  [reg-no]
  (let [candidate-a (get (:capacity-and-capability hiqa-regulations) reg-no)
        candidate-b (get (:quality-and-safety hiqa-regulations) reg-no)]
    (if candidate-a
      [:capacity-and-capability candidate-a]
      [:quality-and-safety candidate-b])))

(defn- parse-compliance-table [pdf-text]
  (let [match (or
               (re-find #"Regulation Title(.*)Compliance Plan"
                        (str/replace pdf-text #"\n" " "))
               (re-find #"Regulation Title(.*)" ;; For cases where compliance table is at end of report
                        (str/replace pdf-text #"\n" " ")))]
    (when match
      (for [line (map str/trim (str/split (first match) #"Regulation"))

            :when (int? (parse-long (str (first line))))

            :let [[_ reg-no _ judgement] (-> (re-seq
                                              #"(\d{1,2}):(.*)(Substantially|Not|Compliant)" line)
                                             first)
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

(defn- parse-compliance-table-alt [pdf-text]
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
                        reg-label (str "Regulation_" reg-no)
                        full-judgement (case judgement
                                         "Substantially" "Substantially compliant"
                                         "Not"           "Not compliant"
                                         "Compliant"     "Compliant"
                                         :error)]]
              [reg-label full-judgement])))))

(comment
  (map (comp parse-compliance-table-alt text/extract) (take 5 all-reports-pdfs)))

(defn- process-pdf
  "Heirarchical version of output. For 'flat' version see 'process-pdf-alt'"
  [pdf-file]
  (let [text              (text/extract pdf-file)
        frontmatter       (parse-frontmatter text)
        centre-id         (reformat-frontm-id (:centre-ID-OSV frontmatter))
        date              (DOI->dateobj (:date-of-inspection frontmatter))
        report-id         (generate-report-id date centre-id)
        residents-present (number-of-residents-present text)
        compliance        (parse-compliance-table text)
        observations      (what-residents-told-us text)]
    (merge frontmatter
           {:report-id report-id
            :centre-id (parse-long centre-id)
            :number-of-residents-present residents-present
            :compliance-levels compliance
            :observations observations})))

(defn- process-pdf-alt [pdf-file]
  (let [text              (text/extract pdf-file)
        frontmatter       (parse-frontmatter text)
        centre-id         (reformat-frontm-id (:centre-ID-OSV frontmatter))
        date              (DOI->dateobj (:date-of-inspection frontmatter))
        report-id         (generate-report-id date centre-id)
        year              (when date (jt/as date :year))
        residents-present (number-of-residents-present text)
        compliance        (parse-compliance-table-alt text)
        observations      (what-residents-told-us text)]
    (merge
     frontmatter
     compliance
     {:report-id                   report-id
      :centre-id                   (parse-long centre-id)
      :number-of-residents-present residents-present
      :observations                observations
      :year                        year
      :date                        (when date (jt/format "YYYY-MM-dd" (jt/zoned-date-time date "UTC")))})))


;; File Outputs

(defn process-pdfs-to-json! [process-fn out]
  (let [data (pmap process-fn all-reports-pdfs)]
    (json/generate-stream data (io/writer out))))

;; TODO Come back and look into the single nil value in centre-id (dropped in the fn below)
(defn full-csv-write! []
  (-> (tc/dataset full-json-out)
      (tc/reorder-columns ["centre-id" "centre-ID-OSV"
                           "year"
                           "date-of-inspection"
                           "report-id"
                           "name-of-designated-centre"
                           "address-of-centre"
                           "name-of-provider"
                           "type-of-inspection"
                           "number-of-residents-present"])
      (tc/drop-missing "centre-id")
      (tc/write! full-csv-out)))

(defn reg-map-cols
  "Returns function for use with Tablecloth, for summarising compliance levels."
  [type]
  (fn [rows]
    (reduce #(if (= %2 type) (inc %1) %1) 0 rows)))

;; Datasets

(def DS_pdf_info
  (-> (tc/dataset full-csv-out {:key-fn keyword})))

(def pdf-info-DB (json/parse-string (slurp "outputs/pdf_info.json") true))

(def DS_pdf_info_compliance
  (-> DS_pdf_info
      (tc/map-columns :num-compliant
                      (tc/column-names DS_pdf_info #":Regulation.*")
                      (fn [& rows]
                        ((reg-map-cols "Compliant") rows)))
      (tc/map-columns :num-notcompliant
                      (tc/column-names DS_pdf_info #":Regulation.*")
                      (fn [& rows]
                        ((reg-map-cols "Not compliant") rows)))
      (tc/map-columns :num-substantiallycompliant
                      (tc/column-names DS_pdf_info #":Regulation.*")
                      (fn [& rows]
                        ((reg-map-cols "Substantially compliant") rows)))
      (tc/map-columns :total [:num-compliant :num-notcompliant :num-substantiallycompliant]
                          (fn [& rows]
                            (reduce + rows)))
      (tc/map-columns :percent-noncompliant [:num-notcompliant :total]
                      (fn [non tot]
                        (if (zero? non) 0
                            (* 100 (float (/ non tot))))))
      (tc/map-columns :percent-fully-compliant [:num-compliant :total]
                      (fn [com tot]
                        (if (zero? com) 0
                            (* 100 (float (/ com tot))))))))

;; OUTPUTS
(comment
  (time
   (process-pdfs-to-json! process-pdf "outputs/pdf_info.json"))
  (time
   (process-pdfs-to-json! process-pdf-alt full-json-out))
  ;; "Elapsed time: 184113.473958 msecs"
  ;; Elapsed time: 181533.344375 msecs
  ;; Elapsed time: 184602.791167 msecs
  ;; 3 mins

  (full-csv-write!))
  ;; 3098



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


;;; *** End of parsing/generation functions
