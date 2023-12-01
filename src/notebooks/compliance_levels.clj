(ns notebooks.compliance-levels
  #:nextjournal.clerk{:visibility {:code :fold}, :toc true}
  (:require
   [hiqa-reports.hiqa-reports :as reports]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))


;; # Aggregation of compliance levels for regulated residential centres for persons with disabilities
;;
;; All raw data from [HIQA inspection reports](https://www.hiqa.ie/reports-and-publications/inspection-reports)
;;
;; [TODO Add link : Supplementary info on methodology around extraction](link).
;;
;; ## Info about regulations
;;
;; The HIQA inspection reports provide 'judgements' on the compliance levels across the various regulations. Judgements are either:
;; - Compliant
;; - Substantially compliant
;; - Not compliant
;;
;; ### Regulations
;; #### Capacity and Capability
;;  - Regulation 3  "Statement of purpose"
;;  - Regulation 4  "Written policies and procedures"
;;  - Regulation 14 "Person in charge"
;;  - Regulation 15 "Staffing"
;;  - Regulation 16 "Training and staff development"
;;  - Regulation 19 "Directory of residents"
;;  - Regulation 21 "Records"
;;  - Regulation 22 "Insurance"
;;  - Regulation 23 "Governance and management"
;;  - Regulation 24 "Admissions and contract for the provision of services"
;;  - Regulation 30 "Volunteers"
;;  - Regulation 31 "Notification of incidets"
;;  - Regulation 32 "Notifications of periods when person in charge is absent"
;;  - Regulation 33 "Notifications of procedures and arrangements for periods when person in charge is absent"
;;  - Regulation 34 "Complaints procedure"
;; #### Quality and Safety
;;  - Regulation 5 "Individualised assessment and personal plan"
;;  - Regulation 6 "Healthcare"
;;  - Regulation 7 "Positive behaviour support"
;;  - Regulation 8 "Protection"
;;  - Regulation 9 "Residents' rights"
;;  - Regulation 10 "Communication"
;;  - Regulation 11 "Visits"
;;  - Regulation 12 "Personal possessions"
;;  - Regulation 13 "General welfare and development"
;;  - Regulation 17 "Premises"
;;  - Regulation 18 "Food and nutrition"
;;  - Regulation 20 "Information for residents"
;;  - Regulation 25 "Temporary absence, transition and discharge of residents"
;;  - Regulation 26 "Risk management procedures"
;;  - Regulation 27 "Protections against infection"
;;  - Regulation 28 "Fire precautions"
;;  - Regulation 29 "Medicines and pharmaceutical services"
;;
;; ## Overall Levels per Regulation

;; ### Capacity and Capability
;;
;;

(clerk/table
 (->
  (reports/make-compliance-tc-table reports/test-group reports/compliance-levels-table-capacity)
  (tc/drop-columns [:area :unchecked])))

;; ### Quality and Safety

(clerk/table
 (->
  (reports/make-compliance-tc-table reports/test-group reports/compliance-levels-table-quality)
  (tc/drop-columns [:area :unchecked])))
