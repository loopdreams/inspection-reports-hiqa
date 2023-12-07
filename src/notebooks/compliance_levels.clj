(ns notebooks.compliance-levels
  #:nextjournal.clerk{:visibility {:code :fold}, :toc true}
  (:require
   [hiqa-reports.parsers-writers :as dat]
   [hiqa-reports.tables :as tables]
   [nextjournal.clerk :as clerk]
   [nextjournal.markdown :as md]
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
;; [Additional info on regulations](https://www.hiqa.ie/sites/default/files/2018-02/Assessment-of-centres-DCD_Guidance.pdf)
;;
;; ## General Info
;;

(clerk/md
 (str "- There were **"
      (-> dat/DS_pdf_info
          (tc/group-by :centre-id)
          :name
          count)
      "** centres included across **"
      (-> dat/DS_pdf_info tc/row-count)
      "** reports.\n"
      "- There were **"
      (-> dat/DS_pdf_info
          (tc/group-by :name-of-provider)
          :name
          count)
      "** providers."))

;; ### Inspections by Year

(clerk/table
 (-> dat/DS_pdf_info
     (tc/group-by :year)
     (tc/process-group-data #(tc/row-count %))
     (tc/as-regular-dataset)
     (tc/drop-columns :group-id)
     (tc/rename-columns {:name "Year"
                         :data "Inspections"})
     (tc/order-by "Year" [:desc])
     (tc/rows :as-maps)))

;; ### Inspections per Region

^{::clerk/visibility {:result :hide}}
(def DS_dublin_grouped
  (-> dat/DS_pdf_info
      (tc/map-columns :area [:address-of-centre]
                      (fn [v]
                        (when v
                          (if (re-find #"Dublin" v)
                            "Dublin"
                            v))))))

;; ### Regional Info

{::clerk/visibility {:result :hide}}
(defn regional-tbl [k name]
  (-> DS_dublin_grouped
     (tc/group-by :area)
     (tc/aggregate-columns [k]
                           #(count (distinct %)))
     (tc/rename-columns {:$group-name "Region"
                         k name})))

(def region-inspections
  (regional-tbl :report-id "Number of Inspections"))
(def region-centres
  (regional-tbl :centre-id "Number of Centres"))
(def region-providers
  (regional-tbl :name-of-provider "Number of Providers"))


{::clerk/visibility {:result :show}}
(clerk/table
 (-> region-inspections
     (tc/full-join region-centres "Region")
     (tc/full-join region-providers "Region")
     (tc/order-by ["Number of Inspections"] [:desc])
     (tc/rows :as-maps)))

;; ### Inspection Types

(clerk/table
 (-> dat/DS_pdf_info
     (tc/group-by :type-of-inspection)
     (tc/aggregate-columns [:report-id]
                           #(count %))
     (tc/rename-columns {:$group-name "Type of Inspection"
                         :report-id "Number of Inspections"})
     (tc/rows :as-maps)))


;;
;; ### Number of Inspections Per Centre


(clerk/table
 {:head ["Number of Inspections" "Number of Centres"]
  :rows
  (sort
   (into []
         (frequencies
          (map
           (comp count second)
           (-> (tc/dataset "outputs/pdf_info_alt.csv")
               (tc/group-by "centre-id" {:result-type :as-indexes}))))))})

;; ## Overall Levels per Regulation

(defn add-percent-noncompliant [table]
  (-> table
      
      (tc/map-columns :total [:compliant :notcompliant :substantiallycompliant]
                      (fn [& rows]
                        (reduce + (remove nil? rows))))
      (tc/map-columns :percent-noncompliant [:notcompliant :total]
                      (fn [a b]
                        (if a
                          (if (pos? a)
                            (* 100 (float (/ a b)))
                            0)
                          0)))
      (tc/order-by [:total] [:desc])

      (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                      #(format "%.2f" %))))
;; ### Capacity and Capability
;;
;;


(def cap-and-cap-data
  (->
   (tables/make-compliance-tc-table dat/pdf-info-DB tables/compliance-levels-table-capacity)
   (tc/drop-columns [:area :unchecked])
   (add-percent-noncompliant)))

(clerk/table cap-and-cap-data)

(def cap-and-cap-data-v2
  (reduce (fn [result entry]
            (let [reg (:regulation entry)
                  com (:compliant entry)
                  noncom (:notcompliant entry)
                  subcom (:substantiallycompliant entry)]
              (conj result
                    {:regulation reg
                     :type "compliant"
                     :value com}
                    {:regulation reg
                     :type "notcompliant"
                     :value noncom}
                    {:regulation reg
                     :type "substantially"
                     :value subcom})))
          []
          (-> cap-and-cap-data (tc/rows :as-maps))))

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:values cap-and-cap-data-v2}
  :mark "rect"
  :encoding {:x {:field "regulation" :type "nominal" :axis {:labelAngle -45}}
             :y {:field "type" :type "nominal" :axis {:title "Judgement"}}
             :color {:field "value" :type "quantitative"}}

  :config {:axis {:grid true :tickBand "extent"}}
  :width 400
  :height 100})

 

;; ### Quality and Safety

(clerk/table
 (->
  (tables/make-compliance-tc-table dat/pdf-info-DB tables/compliance-levels-table-quality)
  (tc/drop-columns [:area :unchecked])
  (add-percent-noncompliant)))

;; ## Regulations by Provider and Area
;;
(def ireland-map (slurp "resources/irish-counties-segmentized.topojson"))

;; ### Regulation 23: Governance and Management
;;
;; Indicators of compliance include:
;; - the management structure is clearly defined and identifies the lines of authority and
;; accountability, specifies roles and details responsibilities for all areas of service
;; provision and includes arrangements for a person to manage the centre during
;; absences of the person in charge, for example during annual leave or absence due to
;; illness.
;; - where there is more than one identified person participating in the management of the
;; centre, the operational governance arrangement are clearly defined. Decisions are
;; communicated, implemented and evaluated.
;; - management systems are in place to ensure that the service provided is safe,
;; appropriate to residents’ needs, consistent and effectively monitored
;; - the person in charge demonstrates sufficient knowledge of the legislation and his/her
;; statutory responsibilities and has complied with the regulations and or standards
;; - there is an annual review of the quality and safety of care and support in the
;; designated centre
;; - a copy of the annual review is made available to residents
;; - residents and their representatives are consulted with in the completion of the annual
;; review of the quality and safety of care
;; - the registered provider (or nominated person) visits the centre at least once every six
;; months and produces a report on the safety and quality of care and support provided in
;; the centre
;; - arrangements are in place to ensure staff exercise their personal and professional
;; responsibility for the quality and safety of the services that they are delivering
;; - there are adequate resources to support residents achieving their individual personal
;; plans
;; - the facilities and services in the centre reflect the statement of purpose
;; - practice is based on best practice and complies with legislative, regulatory and
;; contractual requirements.
;;
;;Indicators of non-compliance include:
;; - there are insufficient resources in the centre and the needs of residents are not met
;; - there are sufficient resources but they are not appropriately managed to adequately
;; meet residents’ needs
;; - due to a lack of resources, the delivery of care and support is not in accordance with
;; the statement of purpose
;; - there is no defined management structure
;; - governance and management systems are not known nor clearly defined
;; - there are no clear lines of accountability for decision making and responsibility for the
;; delivery of services to residents
;; - staff are unaware of the relevant reporting mechanisms
;; - there are no appropriate arrangements in place for periods when the person in charge
;; is absence from the centre
;; - the person in charge is absent from the centre but no suitable arrangements have been
;; made for his or her absence
;; - the person in charge is ineffective in his/her role and outcomes for residents are poor
;; - the centre is managed by a suitably qualified person in charge; however, there are
;; some gaps in his/her knowledge of their responsibilities under the regulations and this
;; has resulted in some specific requirements not been met
;; - the person in charge is inaccessible to residents and their families, and residents do not
;; know who is in charge of the centre
;; - an annual review of the quality and safety of care in the centre does not take place
;; - an annual review of the quality and safety of care in the centre takes place but there is
;; no evidence of learning from the review
;; - a copy of the annual review is not made available to residents and or to the Chief
;; Inspector
;; - the registered provider (or nominated person) does not make an unannounced visit to
;; the centre at least once every six months
;; - the registered provider (or nominated person) does not produce a report on the safety
;; and quality of care and support provided in the centre
;; - effective arrangements are not in place to support, develop or manage all staff to
;; exercise their responsibilities appropriately.
;;

(clerk/table
 (->
  (tables/make-regulation-table dat/pdf-info-DB 23 :area)
  (add-percent-noncompliant)))

(defn county-data [tbl]
  (reduce (fn [result entry]
            (if-not (:area entry) result
                    (let [area (:area entry)]
                      (if (re-find #"Dublin" area)
                        (conj result
                              (assoc entry :area "Dublin"))
                        (conj result entry)))))
          []
          tbl))
          
          

(defn area-compliance-map [title type values h w]
  {:$schema   "https://vega.github.io/schema/vega-lite/v5.json"
   :data      {:format {:feature "counties" :type "topojson"}
               :values ireland-map}
   :height    h
   :width     w
   :title     title
   :transform [{:lookup "id"
                :from   {:data   {:values values}
                         :fields [type]
                         :key    "area"}}]

   :mark     "geoshape"
   :encoding {:color {:field type :type "quantitative"}}})


(clerk/vl
 (area-compliance-map "NonCompliance % Rgulation 23" "percent-noncompliant"
                      (county-data
                       (-> (tables/make-regulation-table dat/pdf-info-DB 23 :area)
                           (add-percent-noncompliant)
                           (tc/rows :as-maps)))
                      400
                      400))


(clerk/row
 (clerk/vl
  (area-compliance-map "Number of NonCompliant Reg 23" "notcompliant"
                       (county-data
                        (-> (tables/make-regulation-table dat/pdf-info-DB 23 :area)
                            (add-percent-noncompliant)
                            (tc/rows :as-maps)))
                       300
                       300))


 (clerk/vl
  (area-compliance-map "Number of Compliant Reg 23" "compliant"
                       (county-data
                        (-> (tables/make-regulation-table dat/pdf-info-DB 23 :area)
                            (add-percent-noncompliant)
                            (tc/rows :as-maps)))
                       300
                       300)))


(clerk/table
 (->
  (tables/make-regulation-table dat/pdf-info-DB 23 :provider)
  (add-percent-noncompliant)))

;; ### Regulation 28: Fire Precautions
;;
;;Indicators of compliance include:
;; - suitable fire equipment is provided and serviced when required, for example, the fire
;; alarm is serviced on a quarterly basis and fire-fighting equipment is serviced on an
;; annual basis
;; - there is adequate means of escape, including emergency lighting. For example,
;; escape routes are clear from obstruction and sufficiently wide to enable evacuation,
;; taking account of residents’ needs and evacuation methods likely to be employed
;; - there is a procedure for the safe evacuation of residents and staff in the event of fire
;; prominently displayed and or readily available, as appropriate
;; - the mobility and cognitive understanding of residents has been adequately accounted
;; for in the evacuation procedure
;; - residents are involved in fire drills whenever possible
;; - staff are trained annually or more frequently if required
;; - staff know what to do in the event of a fire
;; - there are fire drills at suitable intervals, usually twice yearly or more often if required
;; - fire records are kept which include details of fire drills, fire alarm tests, fire-fighting
;; equipment, regular checks of escape routes, exits and fire doors
;; - appropriate maintenance of laundry equipment and proper ventilation of dryers
;; - appropriate storage of equipment such as medical gases and combustible material.
;;
;;Indicators of non-compliance include:
;; - the evacuation procedure for the centre is not fit for purpose, for example, the
;; procedure includes residents remaining in their bedroom to await rescue by the fire
;; service
;; - the mobility and cognitive understanding of residents has not been considered in the
;; fire and evacuation procedure
;; - residents do not know what to do in the event of a fire taking into account their
;; abilities
;; - escape routes are obstructed or not suitable for the residents, staff and visitors
;; expected to use them
;; - there are no records of regular fire drills, fire alarm tests or maintenance of equipment
;; - fire safety equipment has not been serviced in the previous 12 months
;; - some fire doors are wedged open
;; - the building is not adequately subdivided with fire resistant construction such as fire
;; doors as appropriate
;; - poor housekeeping and or inappropriate storage or use of medical gases and
;; combustible materials, represents an unnecessary risk of fire in the centre
;; - an adequate fire alarm has not been provided
;; - an adequate emergency lighting system has not been provided
;; - staff do not know what to do in the event of a fire
;; - staff are not trained in fire safety and or, if required for evacuation, resident-handling
;; - fire evacuation procedures are not prominently displayed throughout the building, as
;; appropriate.
(clerk/table
 (->
  (tables/make-regulation-table dat/pdf-info-DB 28 :area)
  (add-percent-noncompliant)))


(clerk/vl
 (area-compliance-map "NonCompliance % Rgulation 28" "percent-noncompliant"
                      (county-data
                       (-> (tables/make-regulation-table dat/pdf-info-DB 28 :area)
                           (add-percent-noncompliant)
                           (tc/rows :as-maps)))
                      400
                      400))


(clerk/row
 (clerk/vl
  (area-compliance-map "Number of NonCompliant Reg 28" "notcompliant"
                       (county-data
                        (-> (tables/make-regulation-table dat/pdf-info-DB 28 :area)
                            (add-percent-noncompliant)
                            (tc/rows :as-maps)))
                       300
                       300))


 (clerk/vl
  (area-compliance-map "Number of Compliant Reg 28" "compliant"
                       (county-data
                        (-> (tables/make-regulation-table dat/pdf-info-DB 28 :area)
                            (add-percent-noncompliant)
                            (tc/rows :as-maps)))
                       300
                       300)))


(clerk/table
 (->
  (tables/make-regulation-table dat/pdf-info-DB 28 :provider)
  (add-percent-noncompliant)))



