(ns hiqa-reports.hiqa-reports
  (:require
   [clojure.string :as str]
   [pdfboxing.info :as info]
   [pdfboxing.text :as text]))

(def test-pdf "hiqa_test.pdf")
(def test-pdf-2 "hiqa_test_2.pdf")


(def pages (rest (str/split  (text/extract test-pdf) #"Page")))
(def pages2 (rest (str/split (text/extract test-pdf-2) #"Page")))

(def frontmatter-fields ["Name of designated centre:"
                         "Name of provider:"
                         "Address of centre:"
                         "Type of inspection:"
                         "Date of inspection:"
                         "Centre ID:"
                         "Fieldwork ID:"])

(def report-headings ["What residents told us and what inspectors observed \n"
                      "Capacity and capability \n"
                      "Regulation 15: Staffing \n"
                      "Regulation 16: Training and staff development \n"
                      "Regulation 21: Records \n"
                      "Regulation 23: Governance and Management \n"
                      "Regulation 31: Novification of Incidents \n"
                      "Quality and Safety \n"
                      "Regulation 26: Risk Management Procedures \n"
                      "Regulation 27: Protection against infection \n"
                      "Regulation 28: Fire precautions \n"
                      "Regulation 8: Protection"])

(defn frontmatter [page-1]
  (let [info (->> frontmatter-fields
                  (str/join "|")
                  re-pattern
                  (str/split (str/replace page-1 #"\n" ""))
                  reverse
                  (take (count frontmatter-fields))
                  reverse
                  (map str/trim))]
    (zipmap frontmatter-fields info)))

(frontmatter (first pages2))

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

(defn compliance-table [pdf-text]
  (let [relevant-text (rest
                       (str/split
                        (second (str/split pdf-text #"Regulation Title|Compliance Plan"))
                        #"\n"))]
    (into {}
          (for [line relevant-text
                :when (re-find #"Regulation" line)
                :let [status (cond
                               (re-find #"Not" line) "Not Compliant"
                               (re-find #"Substantially" line) "Substantially Compliant"
                               (re-find #"Compliant" line) "Compliant"
                               :else "ERR: Not Found")
                      regulation (first (str/split line #":"))]]
            [regulation status]))))
               
(str/replace "Hello Page 6 of 23" #"Page \d+ of \d+" "")

(number-of-residents-present (text/extract test-pdf-2))
(what-residents-told-us (text/extract test-pdf))
(vals (compliance-table (text/extract test-pdf-2)))

(str/split "Regulation 15: Staffing Compliant " #":")

(info/page-number test-pdf)
