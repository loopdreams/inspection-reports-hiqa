(ns hiqa-reports.hiqa-reports
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [pdfboxing.info :as info]
   [pdfboxing.text :as text]
   [java.time.api :as jt]
   [cheshire.core :as json]))

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

(comment
  (def test-group (mapv process-pdf all-reports-pdfs)))

(defn compliance-levels-for-reg [entries reg-no]
  (reduce (fn [result entry]
            (let [regs (:compliance-levels entry)
                  id (:report-id entry)]
              (assoc result (keyword id) (:judgement (first (filter #(= reg-no (:reg-no %)) regs))))))
          {}
          entries))

(comment
  (frequencies (vals (compliance-levels-for-reg test-group 28))))


          
  


        
        
        
               

(comment
  (number-of-residents-present (text/extract (second all-reports-pdfs)))
  (what-residents-told-us (text/extract (first all-reports-pdfs)))
  (reformat-frontm-id (get (parse-frontmatter (text/extract (last all-reports-pdfs))) "Centre ID:"))
  (vals (compliance-table (text/extract (second all-reports-pdfs)))))
