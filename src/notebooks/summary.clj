(ns notebooks.summary
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [hiqa-reports.hiqa-register :refer [DS-hiqa-register]]
   [hiqa-reports.parsers-writers :as dat]
   [notebooks.01-general-information :as gen-info]
   [notebooks.02-compliance-levels :as com]
   [notebooks.03-sentiment-levels :as sen]
   [scicloj.kindly.v4.kind :as kind]
   [tablecloth.api :as tc]))

;; # HIQA Inspection Reports

(kind/md
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
;; ## Inspections by Year

(kind/vega-lite
 (hc/xform
  ht/bar-chart
  :DATA (-> dat/DS_pdf_info
            (tc/group-by :year)
            (tc/process-group-data #(tc/row-count %))
            (tc/as-regular-dataset)
            (tc/drop-columns :group-id)
            (tc/rename-columns {:name "Year"
                                :data "Inspections"})
            (tc/order-by "Year" [:desc])
            (tc/rows :as-maps))
  :TITLE "Number of Inspections by Year"
  :X "Year" :XTYPE :nominal
  :Y "Inspections" :YTYPE :quantitative))


;; ## Number of Residents Present

(kind/md
 (str
  "- The **total** maximum occupancy across all centres on the HIQA register was: **"
  (->> (:Maximum_Occupancy DS-hiqa-register)
       (reduce +))
  "**"
  "\n"
  "- The total 'residents present' at time of inspection was: **"
  (->> (:no-residents-most-recent gen-info/joined-occupancy)
       (reduce +))
  "**"))

;; ### Congregated Settings
(kind/vega-lite
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json",
  :data {:values [{:category "Congregated"  :value gen-info/congregated-total-present}
                  {:category "Decongregated"  :value gen-info/decongregated-total-present}]}
  :title "Number of Residents Present"
  :encoding {:theta {:field :value, :type "quantitative" :stack "normalize"},
             :color {:field :category, :type "nominal"}}
  :layer [{:mark
           {:type "arc" :outerRadius 100 :tooltip true}}
          {:mark
           {:type "text"
            :radius 60
            :fontSize 18
            :fontWeight "bold"}
           :encoding
           {:text {:field :value :type :quantitative}
            :color {:value "white"}}}]})

(kind/vega-lite
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
   :data {:values [{:category "Congregated"  :value gen-info/congregated-max}
                   {:category "Decongregated"  :value gen-info/decongregated-max}]}
   :title "Maximum Occupancy (HIQA Register)"
   :encoding {:theta {:field :value, :type "quantitative" :stack "normalize"}
              :color {:field :category, :type "nominal"}}
   :layer [{:mark
            {:type "arc" :outerRadius 100 :tooltip true}}
           {:mark
            {:type "text"
             :radius 60
             :fontSize 18
             :fontWeight "bold"}
            :encoding
            {:text {:field :value :type :quantitative}
             :color {:value "white"}}}]})


;; ## Compliance Levels
;;
;; ### By Year

(kind/vega-lite
 {:data {:values com/compliance-by-year-m}
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


;;
;; ### By Regulation
;;
;; Across all regulations, **Governance and Management** (Regulation 23) had both
;; the highest proportion of non compliance as well as the highest number of non-compliant inspections

(kind/vega-lite
 (com/compliance-graph com/compliance-by-reg-m
                       "Number Compliance by Regulation"
                       :number))

;; **Capacity and Capability**

(kind/table com/cap-and-cap-data)

(kind/vega-lite
 (com/compliance-graph (filter #(= (:category %) "Capacity and Capability") com/compliance-by-reg-m)
                       "% Compliance - Capacity and Capability"
                       :percentage))

;; **Quality and Safety**
(kind/table com/qual-and-saf-data)
 

(kind/vega-lite
  (com/compliance-graph (filter #(= (:category %) "Quality and Safety") com/compliance-by-reg-m)
                   "% Compliance - Quality and Safety"
                   :percentage))

;; ### By Provider
;;
;; Only the first 20 providers (by total number of regulations checked) are included here.
;; Nua Healthcare stands out as the provider with the lowest aggregate level of non-compliance

(kind/vega-lite
 {:data {:values (remove #(= "Total checked" (:type %)) com/first-20-providers-by-num-checked-m)}
   :transform [{:calculate "if(datum.type === 'Fully Compliant', 0, if(datum.type === 'Substantially Compliant',1,2))"
                :as "typeOrder"}]
   :mark {:type "bar" :tooltip true}
   :title "Average Compliance Levels By First 20 Providers"
   :width 300
   :height 600
   :encoding {:x {:aggregate :sum :field :val
                  :stack :normalize
                  :title "%"}
              :y {:field :provider
                  :sort "-x"}
              :color {:field :type
                      :sort [ "Non Compliant" "Substantially Compliant" "Fully Compliant"]
                      :title "Compliance Level"}
              :order {:field :typeOrder}}})

(kind/vega-lite
 {:data {:values (remove #(= "Total checked" (:type %)) com/first-20-providers-by-num-checked-m)}
   :transform [{:calculate "if(datum.type === 'Fully Compliant', 0, if(datum.type === 'Substantially Compliant',1,2))"
                :as "typeOrder"}]
   :mark {:type "bar" :tooltip true}
   :title "Total Instances of Compliance by First 20 Providers"
   :width 300
   :height 600
   :encoding {:x {:field :val
                  :title "Number"
                  :type :quantitative}
              :y {:field :provider
                  :sort "-x"}
              :color {:field :type
                      :sort [ "Non Compliant" "Substantially Compliant" "Fully Compliant"]
                      :title "Compliance Level"}
              :order {:field :typeOrder}}})

;; ## Governance and Management
;;
;; As Regulation 23: Governance and Management is both highly inspected and is approximately 20% non compliant on average,
;; it is worth looking more closely into it. From the HIQA documentation, the following
;; elements contribute to this being marked as compliant/noncompliant:
;;
;;
;; Indicators of compliance include:
;;
;; - the management structure is clearly defined and identifies the lines of authority and
;; accountability, specifies roles and details responsibilities for all areas of service
;; provision and includes arrangements for a person to manage the centre during
;; absences of the person in charge, for example during annual leave or absence due to
;; illness.
;;
;; - where there is more than one identified person participating in the management of the
;; centre, the operational governance arrangement are clearly defined. Decisions are
;; communicated, implemented and evaluated.
;;
;; - management systems are in place to ensure that the service provided is safe,
;; appropriate to residents’ needs, consistent and effectively monitored
;;
;; - the person in charge demonstrates sufficient knowledge of the legislation and his/her
;; statutory responsibilities and has complied with the regulations and or standards
;;
;; - there is an annual review of the quality and safety of care and support in the
;; designated centre
;;
;; - a copy of the annual review is made available to residents
;;
;; - residents and their representatives are consulted with in the completion of the annual
;; review of the quality and safety of care
;;
;; - the registered provider (or nominated person) visits the centre at least once every six
;; months and produces a report on the safety and quality of care and support provided in
;; the centre
;;
;; - arrangements are in place to ensure staff exercise their personal and professional
;; responsibility for the quality and safety of the services that they are delivering
;;
;; - there are adequate resources to support residents achieving their individual personal
;; plans
;;
;; - the facilities and services in the centre reflect the statement of purpose
;;
;; - practice is based on best practice and complies with legislative, regulatory and
;; contractual requirements.
;;
;; Indicators of non-compliance include:
;;
;; - there are insufficient resources in the centre and the needs of residents are not met
;;
;; - there are sufficient resources but they are not appropriately managed to adequately
;; meet residents’ needs
;;
;; - due to a lack of resources, the delivery of care and support is not in accordance with
;; the statement of purpose
;;
;; - there is no defined management structure
;;
;; - governance and management systems are not known nor clearly defined
;;
;; - there are no clear lines of accountability for decision making and responsibility for the
;; delivery of services to residents
;;
;; - staff are unaware of the relevant reporting mechanisms
;;
;; - there are no appropriate arrangements in place for periods when the person in charge
;; is absence from the centre
;;
;; - the person in charge is absent from the centre but no suitable arrangements have been
;; made for his or her absence
;;
;; - the person in charge is ineffective in his/her role and outcomes for residents are poor
;;
;; - the centre is managed by a suitably qualified person in charge; however, there are
;; some gaps in his/her knowledge of their responsibilities under the regulations and this
;; has resulted in some specific requirements not been met
;;
;; - the person in charge is inaccessible to residents and their families, and residents do not
;; know who is in charge of the centre
;;
;; - an annual review of the quality and safety of care in the centre does not take place
;;
;; - an annual review of the quality and safety of care in the centre takes place but there is
;; no evidence of learning from the review
;;
;; - a copy of the annual review is not made available to residents and or to the Chief
;; Inspector
;;
;; - the registered provider (or nominated person) does not make an unannounced visit to
;; the centre at least once every six months
;;
;; - the registered provider (or nominated person) does not produce a report on the safety
;; and quality of care and support provided in the centre
;;
;; - effective arrangements are not in place to support, develop or manage all staff to
;; exercise their responsibilities appropriately.

(kind/table
 (-> com/compliance-tbl-reg-23
     (tc/order-by :total :desc)
     (tc/map-columns :percent-noncompliant [:percent-noncompliant]
                     #(format "%.2f" %))
     (tc/map-columns :percent-fully-compliant [:percent-fully-compliant]
                     #(format "%.2f" %))))

(kind/vega-lite
 (com/area-compliance-map "Non-Compliance % Regulation 23"
                          "percent-noncompliant"
                          (com/county-data
                           (-> com/compliance-tbl-reg-23
                               (tc/select-columns [:address-of-centre :percent-noncompliant])
                               (tc/rows :as-maps)))
                          400
                          400))

;; **Total Inspections** (Top 10)
(kind/table
 (-> com/compliance-tbl-reg-23-providers
     (tc/order-by :total :desc)
     (tc/select-rows (range 10))
     com/convert-%-to-string))

;; **% Fully Compliant** (Top 10)
(kind/table
 (-> com/compliance-tbl-reg-23-providers
     (tc/order-by :percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     com/convert-%-to-string))


;; **% Fully Compliant with > 50 inspections** (Top 10)
(kind/table
 (-> com/compliance-tbl-reg-23-providers
     (tc/select-rows #(< 50 (% :total)))
     (tc/order-by :percent-fully-compliant :desc)
     (tc/select-rows (range 10))
     com/convert-%-to-string))

;; **% Non Compliant** (Top 10)

(kind/table
 (-> com/compliance-tbl-reg-23-providers
     (tc/order-by :percent-noncompliant :desc)
     (tc/select-rows (range 10))
     com/convert-%-to-string))

;; **% Non Compliant with > 50 inspections** (Top 10)

(kind/table
 (-> com/compliance-tbl-reg-23-providers
     (tc/select-rows #(< 50 (% :total)))
     (tc/order-by :percent-noncompliant :desc)
     (tc/select-rows (range 10))
     com/convert-%-to-string))

;; ## Inspection Sentiment Analysis (Experimental)

;; Aggregate 'sentiment' levels were obtained by passing the text contained under the section **What residents told us and what inspectors observed** to `GPT-3.5` for evaluation.
;;
;; The following prompt was provided:
;;
;; > "Summarize the following text into 5 keywords reflecting the sentiment of the residents. Do not include the word 'residents' as a keyword."
;;
;; > "Also provide 3 key phrases reflecting the sentiment of the residents"
;;
;; > "Also assign an overall rating of 'positive', 'negative' or 'neutral' based on these sentiments."
;;
;; > "Finally, summarise the text in two sentences."
;;
;; In other words, the following was asked for:
;;
;; - **rating** (positive/negative/neutral)
;;
;; - **keywords** (5)
;;
;; - **key phrases** (3)
;;
;; - **summary** (2 sentences)
;;
;; There are a couple of major caveats here:
;;
;; 1. I used the least powerful version of GPT. The cost was around $6.69 for 3,733 requests (this included some trail and error requests at the outset). The next most powerful api (`GPT-4`) would have cost around 30x this.
;; 2. I am not very familiar with the GPT model, especially questions around how best to formulate the prompt. There were probably better ways to formulate the requests. This exercise was primarily exploratory in nature, therefore a limited amount of time was spent engineering the prompt.

;; ### Info about the text
(kind/md
 (let [obs (-> dat/DS_pdf_info :observations)
       wc (count (str/split (str/join " " obs) #" "))
       avg-wc (float (/ wc (count obs)))]
   (str "- The total word count across all the 'observations' text was approximately: **"
        (format "%,d" wc)
        "**"
        "\n"
        "- The average word count for an entry was: **"
        (format "%.2f" avg-wc)
        "** words.")))

;; ### Ratings

;; The majority of the user experience/inspector observations in the reports
;; were assigned a rating of **positive** by the GPT model.
;;
(kind/vega-lite
  {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
   :data {:values [{:category "Positive" :value (->> (:rating sen/DS_sentiment)
                                                     (filter #(= (str %) "positive"))
                                                     count)}
                   {:category "Neutral"  :value (->> (:rating sen/DS_sentiment)
                                                     (filter #(= (str %) "neutral"))
                                                     count)}
                   {:category "Negative" :value (->> (:rating sen/DS_sentiment)
                                                     (filter #(= (str %) "negative"))
                                                     count)}]}
   :title "Overall Sentiment - Number of Centres"
   :height 400
   :width 400
   :encoding {:theta {:field :value, :type "quantitative" :stack "normalize"}
              :color {:field :category, :type "nominal"}}
   :layer [{:mark
            {:type "arc" :outerRadius 110 :tooltip true}}
           {:mark
            {:type "text"
             :radius 130
             :fontSize 18
             :fontWeight "bold"}
            :encoding
            {:text {:field :value :type :quantitative}
             :color {:value "black"}}}]})

(kind/vega-lite
  {:data {:values sen/rating-by-year-m}
   :mark {:type "bar" :tooltip true}
   :width 400
   :height 400
   :encoding {:y {:aggregate :sum :field :count
                  :stack :normalize
                  :title "%"}
              :x {:field :year}
              :color {:field :type}}})

(kind/vega-lite
  {:$schema   "https://vega.github.io/schema/vega-lite/v5.json"
   :data      {:format {:feature "counties" :type "topojson"}
               :values sen/ireland-map}
   :title     "% Positive by Area"
   :width 400
   :height 400
   :transform [{:lookup "id"
                :from   {:data   {:values sen/percentage-positive-regions-m}
                         :fields ["percentage-positive"]
                         :key    "area"}}]

   :mark     "geoshape"
   :encoding {:color {:field "percentage-positive" :type "quantitative"}}})

;; ### Keywords

;; Top keywords for centres with 'positive' rating:
(kind/vega
   (sen/word-cloud-positive (take 200 sen/positive-keywords-wc)))


;; Top keywords for centres with 'negative' rating:
(kind/vega
   (sen/word-cloud-positive sen/negative-keywords-wc))

