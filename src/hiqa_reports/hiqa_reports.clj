(ns hiqa-reports.hiqa-reports
  (:require
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
  Date input is based on report frontmatter, example '01 January 1900'"
  [date centre-id]
  (let [date (.format (java.text.SimpleDateFormat. "yyyyMMdd")
                      (.parse (java.text.SimpleDateFormat. "dd MMMM yyyy")
                              date))]
    (str centre-id "-" date)))

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
    (zipmap frontmatter-fields info)))

(defn number-of-residents-present [pdf-text]
  (parse-long
   (re-find #"\d+" (second
                    (str/split pdf-text
                               #"Number of residents on the \ndate of inspection:")))))

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
  (for [line (map str/trim
                  (str/split
                   (first
                    (or
                     (re-find #"Regulation Title(.*)Compliance Plan"
                              (str/replace pdf-text #"\n" " "))
                     (re-find #"Regulation Title(.*)" ;; For cases where compliance table is at end of report
                              (str/replace pdf-text #"\n" " "))))
                   #"Regulation"))

        :when (int? (parse-long (str (first line))))

        :let [[_ reg-no _ judgement] (first (re-seq #"(\d{1,2}):(.*)(Substantially|Not|Compliant)" line))
              [area reg] (lookup-reg (parse-long reg-no))
              full-reg (str "Regulation " reg-no ": " reg)
              full-judgement (case judgement
                               "Substantially" "Substantially compliant"
                               "Not" "Not compliant"
                               "Compliant" "Compliant"
                               :error)]]
    {:area       area
     :reg-no     (parse-long reg-no)
     :regulation full-reg
     :judgement  full-judgement}))

(defn process-pdf [pdf-file]
  (let [text              (text/extract pdf-file)
        frontmatter       (parse-frontmatter text)
        centre-id         (reformat-frontm-id (get frontmatter "Centre ID:"))
        report-id         (generate-report-id (get frontmatter "Date of inspection:")
                                              centre-id)
        residents-present (number-of-residents-present text)
        compliance        (parse-compliance-table text)
        observations      (what-residents-told-us text)]
    {:report-id report-id
     :centre-id (parse-long centre-id)
     :frontmatter frontmatter
     :number-of-residents-present residents-present
     :compliance-levels compliance
     :observations observations}))

(def test-group (mapv process-pdf all-reports-pdfs))

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
                           :provider                (get fm "Name of provider:")
                           :area                    (get fm "Address of centre:")
                           :compliant               (if (= judgement "Compliant") 1 0)
                           :non-compliant           (if (= judgement "Not compliant") 1 0)
                           :substantially-compliant (if (= judgement "Substantially compliant") 1 0)})))
                []
                entries)]
    (-> extracts
        tc/dataset
        (tc/group-by group)
        (tc/aggregate {:compliant #(reduce + (% :compliant))
                       :non-compliant #(reduce + (% :non-compliant))
                       :substantially-compliant #(reduce + (% :substantially-compliant))})
        (tc/rename-columns {:$group-name group}))))


(comment
  (make-regulation-table test-group 27 :provider)
  (compliance-levels-by-frontmatter-area test-group "Name of provider:" 28))


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
