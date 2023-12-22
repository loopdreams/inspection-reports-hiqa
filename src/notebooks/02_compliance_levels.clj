(ns notebooks.02-compliance-levels
  #:nextjournal.clerk{:visibility {:code :fold}, :toc true}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [hiqa-reports.parsers-writers :as dat]
   [hiqa-reports.tables :as tables]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))

^{::clerk/visibility {:result :hide}}
(swap! hc/_defaults assoc :BACKGROUND "white")

;; # HIQA Inspection Reports: Compliance
;;
;; All raw data from [HIQA inspection reports](https://www.hiqa.ie/reports-and-publications/inspection-reports)
;;
;; [TODO Add link : Supplementary info on methodology around extraction](link).
;;
;; ## HIQA Regulations
;;
;; The HIQA inspection reports provide 'judgements' on the compliance levels across the various regulations. Judgements are either:
;; - Compliant
;; - Substantially compliant
;; - Not compliant
;;
;; The regulations headings are as follows:
;;

(clerk/md
 (str "**Capacity and Capability**\n"
      (str/join "\n"
                (for [reg (sort (:capacity-and-capability dat/hiqa-regulations))]
                  (str "- Regulation " (first reg) ": " (second reg))))))
(clerk/md
 (str "**Quality and Safety**\n"
      (str/join "\n"
                (for [reg (sort (:quality-and-safety dat/hiqa-regulations))]
                  (str "- Regulation " (first reg) ": " (second reg))))))

;; [Additional info on regulations](https://www.hiqa.ie/sites/default/files/2018-02/Assessment-of-centres-DCD_Guidance.pdf)

;; ### Inspections per Region

{::clerk/visibility {:result :hide}}
(def DS_dublin_grouped
  (-> dat/DS_pdf_info
      (tc/map-columns :area [:address-of-centre]
                      (fn [v]
                        (when v
                          (if (re-find #"Dublin" v)
                            "Dublin"
                            v))))))


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

(def regional-joined
  (-> region-inspections
     (tc/full-join region-centres "Region")
     (tc/full-join region-providers "Region")
     (tc/order-by ["Number of Inspections"] [:desc])
     (tc/rows :as-maps)))

{::clerk/visibility {:result :show}}
(clerk/table
 regional-joined)

(clerk/row
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA regional-joined
   :X "Region" :XTYPE :nominal :XSORT "-y"
   :Y "Number of Inspections" :TYPE :quantitative))
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA regional-joined
   :X "Region" :XTYPE :nominal :XSORT "-y"
   :Y "Number of Centres" :TYPE :quantitative))
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA regional-joined
   :X "Region" :XTYPE :nominal :XSORT "-y"
   :Y "Number of Providers" :TYPE :quantitative)))
 


;;
;; ## Overall Compliance Levels per Regulation

{::clerk/visibility {:result :hide}}
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

(defn regulations-graph [data]
  (reduce (fn [result entry]
            (let [reg (:reg-name entry)
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
          (-> data (tc/rows :as-maps))))


(def cap-and-cap-data
  (-> tables/DS_agg_compliance_per_reg
      (tc/select-rows #(= :capacity-and-capability (% :area)))
      (add-percent-noncompliant)
      (tc/drop-columns [:area :number :name])))

(defn reg-heatmap-char [data title]
  {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
   :data {:values (regulations-graph data)}
   :mark {:type "rect" :tooltip true}
   :title title
   :encoding {:y {:field "regulation" :type "nominal" :axis { :labelLimit 0 :labelFontSize 12} :title false}
              :x {:field "type" :type "nominal" :axis {:title "Judgement" :labelFontSize 12}}
              :color {:field "value" :type "quantitative" :title "No. of Centres" :scale {:scheme "bluegreen"}}
              :config {:axis {:grid true :tickBand "extent"}}}
   :width 200
   :height 600})

{::clerk/visibility {:result :show}}
(clerk/vl
 (reg-heatmap-char cap-and-cap-data "Capacity and Capability"))

(clerk/table cap-and-cap-data)

;; ### Quality and Safety

{::clerk/visibility {:result :hide}}
(def qual-and-saf-data
  (-> tables/DS_agg_compliance_per_reg
      (tc/select-rows #(= :quality-and-safety (% :area)))
      (add-percent-noncompliant)
      (tc/drop-columns [:area :number :name])))


{::clerk/visibility {:result :show}}
(clerk/vl
 (reg-heatmap-char qual-and-saf-data "Quality and Safety"))

(clerk/table qual-and-saf-data)


(def ireland-map (slurp "resources/datasets/imported/irish-counties-segmentized.topojson"))
;; ### Aggregate Compliance Levels by Area and Provider
;;
;; Figures are average, and for indicative purposes only. They do not caputre the types of compliance and the nuances around the difference regulations.

;;** % Fully Compliant by Provider** (Top 10)
(clerk/table
 (-> dat/DS_pdf_info_agg_compliance
     (tc/group-by :name-of-provider)
     (tc/aggregate {:total-regs-checked #(reduce + (% :total))
                    :avg-percent-fully-compliant #(float (/ (reduce + (% :percent-fully-compliant))
                                                            (count (% :percent-fully-compliant))))})
     (tc/order-by :avg-percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     (tc/rename-columns {:$group-name :provider})))

;; **% Fully Compliant by Provider where there were > 200 Regulations Checked** (Top 10)

(clerk/table
 (-> dat/DS_pdf_info_agg_compliance
     (tc/group-by :name-of-provider)
     (tc/aggregate {:total-regs-checked #(reduce + (% :total))
                    :avg-percent-fully-compliant #(float (/ (reduce + (% :percent-fully-compliant))
                                                            (count (% :percent-fully-compliant))))})
     (tc/select-rows #(< 200 (% :total-regs-checked)))
     (tc/order-by :avg-percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     (tc/rename-columns {:$group-name :provider})))

;; **% Fully Compliant by Area** (Top 10)
(clerk/table
 (-> dat/DS_pdf_info_agg_compliance
     (tc/group-by :address-of-centre)
     (tc/aggregate {:total-regs-checked #(reduce + (% :total))
                    :avg-percent-fully-compliant #(float (/ (reduce + (% :percent-fully-compliant))
                                                            (count (% :percent-fully-compliant))))})
     (tc/order-by :avg-percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     (tc/rename-columns {:$group-name :area})))


{::clerk/visibility {:result :hide}}
(def compliance-by-year-m
  (reduce (fn [result ds]
            (let [year (first (:year ds))
                  full-c (reduce + (:num-compliant ds))
                  non-c (reduce + (:num-notcompliant ds))
                  subs-c (reduce + (:num-substantiallycompliant ds))]
              (conj result
                    (-> {}
                        (assoc :year year)
                        (assoc :type "Fully Compliant")
                        (assoc :val full-c))
                    (-> {}
                        (assoc :year year)
                        (assoc :type "Non Compliant")
                        (assoc :val non-c))
                    (-> {}
                        (assoc :year year)
                        (assoc :type "Substantially Compliant")
                        (assoc :val subs-c)))))
          []
          (-> dat/DS_pdf_info_agg_compliance
              (tc/group-by :year)
              :data)))

{::clerk/visibility {:result :show}}
(clerk/vl
 {:data {:values compliance-by-year-m}
  :transform [{:calculate "if(datum.type === 'Fully Compliant', 0, if(datum.type === 'Substantially Compliant',1,2))"
               :as "typeOrder"}]
  :mark {:type "bar" :tooltip true}
  :width 400
  :height 400
  :encoding {:y {:aggregate :sum :field :val
                 :stack :normalize
                 :title "Average %"}
             :x {:field :year}
             :color {:field :type
                     :sort [ "Non Compliant" "Substantially Compliant" "Fully Compliant"]
                     :title "Compliance Level"}
             :order {:field :typeOrder}}})


{::clerk/visibility {:result :hide}}
(def compliance-by-region-m
  (reduce (fn [result ds]
            (let [address (first (:address-of-centre ds))
                  full-c (reduce + (:num-compliant ds))
                  non-c (reduce + (:num-notcompliant ds))
                  subs-c (reduce + (:num-substantiallycompliant ds))]
              (conj result
                    (-> {}
                        (assoc :address address)
                        (assoc :type "Fully Compliant")
                        (assoc :val full-c))
                    (-> {}
                        (assoc :address address)
                        (assoc :type "Non Compliant")
                        (assoc :val non-c))
                    (-> {}
                        (assoc :address address)
                        (assoc :type "Substantially Compliant")
                        (assoc :val subs-c)))))
          []
          (-> dat/DS_pdf_info_agg_compliance
              (tc/group-by :address-of-centre)
              :data)))

(def total-regs-checked-by-region
  (reduce (fn [result ds]
            (conj result
                  (-> {}
                      (assoc :address-of-centre (first (:address-of-centre ds)))
                      (assoc :total-regs-checked (reduce + (:total ds))))))
          []
          (-> dat/DS_pdf_info_agg_compliance
              (tc/group-by :address-of-centre)
              :data)))

{::clerk/visibility {:result :show}}
(clerk/row
 (clerk/vl
  {:data {:values compliance-by-region-m}
   :transform [{:calculate "if(datum.type === 'Fully Compliant', 0, if(datum.type === 'Substantially Compliant',1,2))"
                :as "typeOrder"}]
   :mark {:type "bar" :tooltip true}
   :title "Average Compliance Levels By Region"
   :width 300
   :height 700
   :encoding {:x {:aggregate :sum :field :val
                  :stack :normalize
                  :title "Average %"}
              :y {:field :address
                  :sort "-x"}
              :color {:field :type
                      :sort [ "Non Compliant" "Substantially Compliant" "Fully Compliant"]
                      :title "Compliance Level"}
              :order {:field :typeOrder}}})


 (clerk/vl
  {:data {:values total-regs-checked-by-region}
   :mark {:type "bar" :tooltip true}
   :title "Total Regulations Checked"
   :width 300
   :height 700
   :encoding {:x {:field :total-regs-checked
                  :type :quantitative
                  :title "Total"}
              :y {:field :address-of-centre
                  :title nil
                  :sort "-x"}}}))


(def first-20-providers-by-num-checked
  (-> dat/DS_pdf_info_agg_compliance
      (tc/group-by :name-of-provider)
      (tc/aggregate {:total-checked #(reduce + (% :total))
                     :num-compliant #(reduce + (% :num-compliant))
                     :num-notcompliant #(reduce + (% :num-notcompliant))
                     :num-substantiallycompliant #(reduce + (% :num-substantiallycompliant))})
      (tc/order-by :total-checked :desc)
      (tc/rename-columns {:$group-name :provider})
      (tc/select-rows (range 20))))

(def first-20-providers-by-num-checked-m
  (reduce (fn [result entry]
            (conj result
                  (-> {}
                      (assoc :provider (:provider entry))
                      (assoc :type "Fully Compliant")
                      (assoc :val (:num-compliant entry)))
                  (-> {}
                      (assoc :provider (:provider entry))
                      (assoc :type "Non Compliant")
                      (assoc :val (:num-notcompliant entry)))
                  (-> {}
                      (assoc :provider (:provider entry))
                      (assoc :type "Substantially Compliant")
                      (assoc :val (:num-substantiallycompliant entry)))
                  (-> {}
                      (assoc :provider (:provider entry))
                      (assoc :type "Total checked")
                      (assoc :val (:total-checked entry)))))
          []
          (->
           first-20-providers-by-num-checked
           (tc/rows :as-maps))))

(clerk/row
 (clerk/vl
  {:data {:values (remove #(= "Total checked" (:type %)) first-20-providers-by-num-checked-m)}
   :transform [{:calculate "if(datum.type === 'Fully Compliant', 0, if(datum.type === 'Substantially Compliant',1,2))"
                :as "typeOrder"}]
   :mark {:type "bar" :tooltip true}
   :title "Average Compliance Levels By First 20 Providers"
   :width 300
   :height 600
   :encoding {:x {:aggregate :sum :field :val
                  :stack :normalize
                  :title "Average %"}
              :y {:field :provider
                  :sort "-x"}
              :color {:field :type
                      :sort [ "Non Compliant" "Substantially Compliant" "Fully Compliant"]
                      :title "Compliance Level"}
              :order {:field :typeOrder}}})
 (clerk/vl
  {:data {:values (filter #(= "Total checked" (:type %)) first-20-providers-by-num-checked-m)}
   :mark {:type "bar" :tooltip true}
   :title "Total Regulations Checked"
   :width 300
   :height 600
   :encoding {:x {:field :val
                  :type :quantitative
                  :title "Total"}
              :y {:field :provider
                  :title nil
                  :axis nil
                  :sort "-x"}}}))



;; ## Regulations by Provider and Area
;;
;; I will look more closely below at two of the areas with a higher proportion of Not compliant"
;; judgements, **Governance and Mannagement** and **Fire Precautions**.
;;

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

{::clerk/visibility {:result :hide}}
(defn average-Dublin [entries val]
  (let [e-dub (remove #(= "Dublin" (:address-of-centre %)) entries)
        dubs (filter #(= "Dublin" (:address-of-centre %)) entries)
        c (count dubs)
        tot (reduce + (map val dubs))
        avg (float (/ tot c))]
    (conj e-dub {:address-of-centre "Dublin" val avg})))


(defn county-data [entries]
  (let [val (second (keys (first entries)))]
    (average-Dublin
     (reduce (fn [result entry]
               (if-not (:address-of-centre entry) result
                       (let [area (:address-of-centre entry)]
                         (if (re-find #"Dublin" area)
                           (conj result
                                 (assoc entry :address-of-centre "Dublin"))
                           (conj result entry)))))
             []
             entries)
     val)))


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
                         :key    "address-of-centre"}}]

   :mark     "geoshape"
   :encoding {:color {:field type :type "quantitative"}}})


(defn convert-%-to-string [DS]
  (-> DS
      (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                       #(format "%.2f" %))
      (tc/map-columns :percent-fully-compliant [:percent-fully-compliant]
                      #(format "%.2f" %))))



(def compliance-tbl-reg-23
  (-> dat/DS_pdf_info
      (dat/agg-compliance-levels-for-reg-by-group 23 :address-of-centre)))


{::clerk/visibility {:result :show}}
(clerk/table
 (-> compliance-tbl-reg-23
     (tc/order-by :total :desc)
     (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                     #(format "%.2f" %))
     (tc/map-columns :percent-fully-compliant [:percent-fully-compliant]
                     #(format "%.2f" %))))



(clerk/row
 (clerk/vl
  (area-compliance-map "NonCompliance % Regulation 23" "percent-noncompliant"
                       (county-data
                        (-> compliance-tbl-reg-23
                            (tc/select-columns [:address-of-centre :percent-noncompliant])
                            (tc/rows :as-maps)))
                       300
                       300))
 (clerk/table
  (->
   compliance-tbl-reg-23
   (tc/select-columns [:address-of-centre :percent-noncompliant])
   (tc/rows :as-maps)
   county-data
   tc/dataset
   (tc/order-by :percent-noncompliant :desc)
   (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                   #(format "%.2f" %))
   (tc/select-rows (range 10))))
 (clerk/table
  (->
   compliance-tbl-reg-23
   (tc/select-columns [:address-of-centre :percent-noncompliant])
   (tc/order-by :percent-noncompliant :desc)
   (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                   #(format "%.2f" %))
   (tc/select-rows (range 10)))))


(clerk/row
 (clerk/vl
  (area-compliance-map "Number of NonCompliant Reg 23" "num-notcompliant"
                       (county-data
                        (-> compliance-tbl-reg-23
                            (tc/select-columns [:address-of-centre :num-notcompliant])
                            (tc/rows :as-maps)))
                       300
                       300))
 (clerk/table
  (-> compliance-tbl-reg-23
      (tc/select-columns [:address-of-centre :num-notcompliant])
      (tc/order-by :num-notcompliant :desc)
      (tc/select-rows (range 10)))))

(clerk/row
 (clerk/vl
  (area-compliance-map "Number of Compliant Reg 23" "num-compliant"
                       (county-data
                        (-> compliance-tbl-reg-23
                            (tc/select-columns [:address-of-centre :num-compliant])
                            (tc/rows :as-maps)))
                       300
                       300))
 (clerk/table
  (-> compliance-tbl-reg-23
     (tc/select-columns [:address-of-centre :num-compliant])
     (tc/order-by :num-compliant :desc)
     (tc/select-rows (range 10)))))
       

(def compliance-tbl-reg-23-providers
  (-> dat/DS_pdf_info
      (dat/agg-compliance-levels-for-reg-by-group 23 :name-of-provider)))


;; **Total Inspections** (Top 10)
(clerk/table
 (-> compliance-tbl-reg-23-providers
     (tc/order-by :total :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))

;; **% Fully Compliant** (Top 10)
(clerk/table
 (-> compliance-tbl-reg-23-providers
     (tc/order-by :percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))


;; **% Fully Compliant with > 50 inspections** (Top 10)
(clerk/table
 (-> compliance-tbl-reg-23-providers
     (tc/select-rows #(< 50 (% :total)))
     (tc/order-by :percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))

;; **% Non Compliant** (Top 10)

(clerk/table
 (-> compliance-tbl-reg-23-providers
     (tc/order-by :percent-noncompliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))

;; **% Non Compliant with > 50 inspections** (Top 10)

(clerk/table
 (-> compliance-tbl-reg-23-providers
     (tc/select-rows #(< 50 (% :total)))
     (tc/order-by :percent-noncompliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))
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

(def compliance-tbl-reg-28
  (-> dat/DS_pdf_info
      (dat/agg-compliance-levels-for-reg-by-group 28 :address-of-centre)))


(clerk/table
 (-> compliance-tbl-reg-28
     (tc/order-by :total :desc)
     (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                     #(format "%.2f" %))
     (tc/map-columns :percent-fully-compliant [:percent-fully-compliant]
                     #(format "%.2f" %))))



(clerk/row
 (clerk/vl
  (area-compliance-map "NonCompliance % Regulation 28" "percent-noncompliant"
                       (county-data
                        (-> compliance-tbl-reg-28
                            (tc/select-columns [:address-of-centre :percent-noncompliant])
                            (tc/rows :as-maps)))
                       300
                       300))
 (clerk/table
  (->
   compliance-tbl-reg-28
   (tc/select-columns [:address-of-centre :percent-noncompliant])
   (tc/rows :as-maps)
   county-data
   tc/dataset
   (tc/order-by :percent-noncompliant :desc)
   (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                   #(format "%.2f" %))
   (tc/select-rows (range 10))))
 (clerk/table
  (->
   compliance-tbl-reg-28
   (tc/select-columns [:address-of-centre :percent-noncompliant])
   (tc/order-by :percent-noncompliant :desc)
   (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                   #(format "%.2f" %))
   (tc/select-rows (range 10)))))


(clerk/row
 (clerk/vl
  (area-compliance-map "Number of NonCompliant Reg 28" "num-notcompliant"
                       (county-data
                        (-> compliance-tbl-reg-28
                            (tc/select-columns [:address-of-centre :num-notcompliant])
                            (tc/rows :as-maps)))
                       300
                       300))
 (clerk/table
  (-> compliance-tbl-reg-28
      (tc/select-columns [:address-of-centre :num-notcompliant])
      (tc/order-by :num-notcompliant :desc)
      (tc/select-rows (range 10)))))

(clerk/row
 (clerk/vl
  (area-compliance-map "Number of Compliant Reg 28" "num-compliant"
                       (county-data
                        (-> compliance-tbl-reg-28
                            (tc/select-columns [:address-of-centre :num-compliant])
                            (tc/rows :as-maps)))
                       300
                       300))
 (clerk/table
  (-> compliance-tbl-reg-28
     (tc/select-columns [:address-of-centre :num-compliant])
     (tc/order-by :num-compliant :desc)
     (tc/select-rows (range 10)))))


(def compliance-tbl-reg-28-providers
  (-> dat/DS_pdf_info
      (dat/agg-compliance-levels-for-reg-by-group 28 :name-of-provider)))


;; **Total Inspections** (Top 10)
(clerk/table
 (-> compliance-tbl-reg-28-providers
     (tc/order-by :total :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))

;; **% Fully Compliant** (Top 10)
(clerk/table
 (-> compliance-tbl-reg-28-providers
     (tc/order-by :percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))


;; **% Fully Compliant with > 50 inspections** (Top 10)
(clerk/table
 (-> compliance-tbl-reg-28-providers
     (tc/select-rows #(< 50 (% :total)))
     (tc/order-by :percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))

;; **% Non Compliant** (Top 10)

(clerk/table
 (-> compliance-tbl-reg-28-providers
     (tc/order-by :percent-noncompliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))

;; **% Non Compliant with > 50 inspections** (Top 10)

(clerk/table
 (-> compliance-tbl-reg-28-providers
     (tc/select-rows #(< 50 (% :total)))
     (tc/order-by :percent-noncompliant :desc)
     (tc/select-rows (range 10))
     convert-%-to-string))
