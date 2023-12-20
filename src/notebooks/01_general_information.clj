(ns notebooks.01-general-information
  #:nextjournal.clerk{:visibility {:code :fold}, :toc :collapsed}
  (:require
   [hiqa-reports.hiqa-register :refer [hiqa-reg-DB]]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [hiqa-reports.parsers-writers :as dat]
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [java-time.api :as jt]
   [scicloj.noj.v1.vis.hanami.templates :as vht]
   [scicloj.noj.v1.vis.hanami :as hanami]
   [scicloj.noj.v1.stats :as stats]))

(comment
  (clerk/serve! {:browse? true :watch-paths ["src/notebooks"]}))


;; # HIQA Inspection Reports - Data Aggregation
;;
;; ## Context and Method
;;
;; All of the data below was taken from the [HIQA Inspection Reports](https://www.hiqa.ie/reports-and-publications/inspection-reports) for Disability Centres, available on their website in `.pdf` format.
;;
;; Report URLs are listed under pages for [centres](https://www.hiqa.ie/find-a-centre) where an inspection has occurred ([example](https://www.hiqa.ie/areas-we-work/find-a-centre/st-dominics-services)). Using the HIQA register for disability centres, I was able to get the URLs for the centre pages, and from these, the URLs for all inspection  reports.
;;
;; Reports are written according to a **standard template**, making them relatively easy to parse. I extracted information from the following sections of the reports:
;;
;; - 'Frontmatter' area on the first page, including details of the centre id, date of inspection, type of inspection, etc.
;; - Number of residents present at the time of inspection
;; - Levels of compliance for the regulations inspected ("Compliant", "Not compliant", "Substantially compliant")
;; - The narrative text from the section titled "What residents told us and what inspectors observed"
;;
;; This is not an exhaustive list of information that could be extracted from the reports.
;;
;; ;; TODO Information on the 'success rate'
;;
;; Below is some general information and insights gathered from the data contained in the reports, using information contained in the frontmatter and the 'number of residents' present field.
;;
;; There are separate pages for :
;;
;; - **Aggregate Compliance Levels** based on the levels of compliance listed in the reports
;; - **Sentiment Levels** based on the narrative 'observations' text sections of the reports, aggregated/analysed using OpanAI's `GPT-3.5`


;; ## General Information

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


;; ### Number of Inspections

(clerk/vl
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

{::clerk/visibility {:result :hide}}
(def month-data
  (reduce concat
          (let [data (-> dat/DS_pdf_info
                         (tc/select-columns [:year :date])
                         (tc/drop-missing :date)
                         (tc/rows :as-maps))
                sorted-m
                (reduce (fn [result {:keys [year date]}]
                          (let [month (jt/as date :month-of-year)]
                            (update-in result [year month] (fnil inc 0))))
                        {}
                        data)]
            (for [year (keys sorted-m)]
              (for [month (keys (sorted-m year))]
                {:year year :month month :value ((sorted-m year) month)})))))

{::clerk/visibility {:result :show}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:values month-data}
  :mark {:type :bar :tooltip true}
  :title "Number of Inspections by Month and Year"
  :width 600
  :height 400
  :encoding
  {:x {:field :month
       :type "ordinal"}
   :y {:field :value
       :type "quantitative"
       :title "Number of Inspections"}
   :color {:field :year
           :type "nominal"}}})

;; ### Number of Centres

(clerk/md
 (str "- There were **"
      (-> dat/DS_pdf_info
          (tc/group-by :centre-id)
          :name
          count)
      "** centres included across **"
      (-> dat/DS_pdf_info tc/row-count)
      "** reports.\n"))

;; #### Number of Centres per region

{::clerk/visibility {:result :hide}}
(def DS_dublin_grouped
  (-> dat/DS_pdf_info
      (tc/map-columns :area [:address-of-centre]
                      (fn [v]
                        (when v
                          (if (re-find #"Dublin" v)
                            "Dublin"
                            v))))))

(def ireland-map (slurp "resources/irish-counties-segmentized.topojson"))

(def centre-by-region (-> DS_dublin_grouped
                          (tc/group-by :area)
                          (tc/aggregate {:centres #(count (distinct (% :centre-id)))})
                          (tc/rename-columns {:$group-name :area})))

{::clerk/visibility {:result :show}}
(clerk/row
 (clerk/vl
  {:$schema   "https://vega.github.io/schema/vega-lite/v5.json"
   :data      {:format {:feature "counties" :type "topojson"}
               :values ireland-map}
   :height 400
   :width 400
   :title     "Number of Centres by Region"
   :transform [{:lookup "id"
                :from   {:data
                         {:values (-> centre-by-region
                                      (tc/rows :as-maps))}
                         :fields ["centres"]
                         :key    "area"}}]
   :mark      "geoshape"
   :encoding  {:color {:field "centres" :type "quantitative"}}})

 (clerk/table
  (-> centre-by-region
      (tc/order-by :centres :desc))))

;; #### Number of inspections per centre

{::clerk/visibility {:code :hide :result :hide}}
(def inspections-per-centre
  (->>
   (-> dat/DS_pdf_info
       (tc/group-by :centre-id {:result-type :as-indexes}))
   (map (comp count second))
   frequencies
   (into [])
   sort))

{::clerk/visibility {:result :show}}
(clerk/md
 (let [top-inspections (apply + (map second [(nth inspections-per-centre 1)
                                             (nth inspections-per-centre 2)]))
       total-centres (apply + (map second inspections-per-centre))
       relevant-percentage (int (* 100 (/ top-inspections total-centres)))]
   (str
    "A significant majority (**"
                             relevant-percentage
                             "%**) of centres recieved either 2 or 3 inspections.")))

(clerk/table
 {:head ["Number of Inspections" "Number of Centres"]
  :rows
  inspections-per-centre})

;; #### Number of centres inspected compared to HIQA register

(clerk/md
 (let [num-inspected (-> dat/DS_pdf_info
                         (tc/group-by :centre-id)
                         :name
                         count)
       total-num (-> hiqa-reg-DB (tc/row-count))
       percentage-coverage (int (* 100 (/ num-inspected total-num)))]
   (str
    "Across the reports, there were **"
    num-inspected
    "** centres inspected. The HIQA register conatins **"
    total-num
    "** registered centres. Therefore, the inspections covered approximately **"
    percentage-coverage
    "%** of registered centres.")))



;; ### Number of Providers

(clerk/md
 (let [total-providers
       (-> dat/DS_pdf_info
           (tc/group-by :name-of-provider)
           :name
           count)]
   (str "There were a total of **"
        total-providers "** across the reports.")))

;; #### Providers by Region
{::clerk/visibility {:result :hide :code :hide}}
(def providers-by-region
  (-> DS_dublin_grouped
      (tc/group-by :area)
      (tc/aggregate {:providers #(count (distinct (% :name-of-provider)))})
      (tc/rename-columns {:$group-name :area})))


{::clerk/visibility {:result :show}}
(clerk/row
 (clerk/vl
  {:$schema   "https://vega.github.io/schema/vega-lite/v5.json"
   :data      {:format {:feature "counties" :type "topojson"}
               :values ireland-map}
   :height 400
   :width 400
   :title     "Number of Providers by Region"
   :transform [{:lookup "id"
                :from   {:data
                         {:values (-> providers-by-region
                                      (tc/rows :as-maps))}
                         :fields ["providers"]
                         :key    "area"}}]
   :mark      "geoshape"
   :encoding  {:color {:field "providers" :type "quantitative"}}})

 (clerk/table
  (-> providers-by-region
      (tc/order-by :providers :desc))))


;; #### Top 10 Providers by Number of Centres
{::clerk/visibility {:result :hide}}
(def centres-per-provider
  (-> dat/DS_pdf_info
      (tc/group-by :name-of-provider)
      (tc/aggregate {:centres #(count (distinct (% :centre-id)))})
      (tc/rename-columns {:$group-name :provider})))

(def inspections-per-provider
  (-> dat/DS_pdf_info
      (tc/group-by :name-of-provider)
      (tc/aggregate {:inspections #(count (% :report-id))})
      (tc/rename-columns {:$group-name :provider})))

(def inspections-and-centres-provider
  (-> centres-per-provider
      (tc/full-join inspections-per-provider :provider)
      (tc/map-columns :inspections-per-centre [:centres :inspections]
                      #(float (/ %2 %1)))))


{::clerk/visibility {:result :show}}
(clerk/table
 (-> inspections-and-centres-provider
     (tc/order-by :centres :desc)
     (tc/select-rows (range 10))))


;; #### Top 10 Providers by Inspections per Centre

(clerk/table
 (-> inspections-and-centres-provider
     (tc/order-by :inspections-per-centre :desc)
     (tc/select-rows (range 10))))



;; TODO Failure here probably due to dependency conflict
(comment
  (hanami/linear-regression-plot inspections-and-centres-provider :centres :inspections {})

  (-> inspections-and-centres-provider
      (stats/add-predictions :centres [:inspections]
                             {:model-type :smile.regression/ordinary-least-square})))

;; ### Inspection Types

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :description "A simple donut chart with embedded data."
  :data {:values (-> dat/DS_pdf_info
                     (tc/group-by :type-of-inspection)
                     (tc/aggregate-columns [:report-id]
                                           #(count %))
                     (tc/rename-columns {:$group-name "Type of Inspection"
                                         :report-id "Number of Inspections"})
                     (tc/rows :as-maps))}

  :mark {:type "arc" :innerRadius 50 :tooltip true}
  :encoding {:theta {:field "Number of Inspections" :type "quantitative"}
             :color {:field "Type of Inspection" :type "nominal"}}})

;; ## Number of Residents Present
;;
;; As there can be multiple inspections per centre, this takes only the number of residents
;; present at the time of the  **most recent** inspection

{::clerk/visibility {:result :hide}}
(defn most-recent-resident-no [dates resnum]
  (->> resnum
       (interleave dates)
       (partition 2)
       (sort-by first)
       reverse
       first
       second))

{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA
  (-> dat/DS_pdf_info
      (tc/drop-missing :number-of-residents-present)
      (tc/group-by :centre-id)
      (tc/aggregate {:no-residents-most-recent
                     #(most-recent-resident-no
                       (% :date)
                       (% :number-of-residents-present))})
      (tc/group-by :no-residents-most-recent)
      (tc/aggregate {:centres #(count (% :$group-name))})
      (tc/rename-columns {:$group-name :number-of-residents-present})
      (tc/rows :as-maps))
  :TITLE "Number of Residents Present at Time of Inspection"
  :X :number-of-residents-present :XTYPE :nominal
  :Y :centres :YTPE :quantitative))


(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA
  (-> hiqa-reg-DB
      (tc/group-by :Maximum_Occupancy)
      (tc/aggregate {:number-of-centres #(count (% :Centre_ID))})
      (tc/rename-columns {:$group-name :maximum-occupancy})
      (tc/rows :as-maps))
  :TITLE "Maximum Occupancy - HIQA Register"
  :X :maximum-occupancy :XTYPE :nominal
  :Y :number-of-centres))

;; Below is a table comparing the maximum occupancy for a centre as recorded in the HIQA register vs.
;; the number of residents present at the date of the last inspection. There are obviously a lot of
;; caveats around this kind of comparison, since the 'number of residents' present at the precise time
;; of the inspection is highly dependant on a lot of other factors not captured here.
;;
(def joined-occupancy
  (-> dat/DS_pdf_info
        (tc/drop-missing :number-of-residents-present)
        (tc/group-by :centre-id)
        (tc/aggregate {:no-residents-most-recent
                        #(most-recent-resident-no
                          (% :date)
                          (% :number-of-residents-present))})
        (tc/rename-columns {:$group-name :Centre_ID})
        (tc/full-join hiqa-reg-DB :Centre_ID)
        (tc/select-columns [:Centre_ID :Maximum_Occupancy :no-residents-most-recent])
        (tc/drop-missing :no-residents-most-recent)
        (tc/order-by :Centre_ID)
        (tc/map-columns :difference
                         [:Maximum_Occupancy :no-residents-most-recent]
                         #(- %1 %2))))

(clerk/table joined-occupancy)

(clerk/md
 (str
  "- The **total** maximum occupancy across all centres on the HIQA register is: **"
  (->> (:Maximum_Occupancy hiqa-reg-DB)
       (reduce +))
  "**"
  "\n"
  "- The total maximum occupancy across centres where inspections also occured is: **"
  (->> (:Maximum_Occupancy joined-occupancy)
       (reduce +))
  "**"
  "\n"
  "- The total 'residents present' at time of inspection is: **"
  (->> (:no-residents-most-recent joined-occupancy)
       (reduce +))
  "**"
  "\n"
  "- The difference between 'max' occupancy of centres inspected and 'residents present' is: **"
  (->> (:difference joined-occupancy)
       (reduce +))
  "**"
  "\n"
  "- The difference between 'max occupancy' and 'residents present' for **congregated settings** is: **"
  (->> (:difference
        (-> joined-occupancy
            (tc/select-rows #(< 9 (% :Maximum_Occupancy)))))
       (reduce +))
  "**"))

;; Surpisingly, there were also a number of centres with 'more' residents present than was listed as 'maximum occupancy'.
;;
(clerk/table
 (-> joined-occupancy
     (tc/select-rows #(> 0 (% :difference)))
     (tc/order-by :difference)))

;; After looking up the reports for the first few entries here, it seems that the HIQA register might be out of date.

;; ### 'Congregated Settings' Based on Number of Residents present at time of inspection

{::clerk/visibility {:result :hide}}
(defn totals-present [fn no]
  (reduce +
          (-> dat/DS_pdf_info
              (tc/drop-missing :number-of-residents-present)
              (tc/select-rows #(fn no (% :number-of-residents-present)))
              (tc/group-by :centre-id)
              (tc/aggregate {:no-residents-most-recent
                             #(most-recent-resident-no
                               (% :date)
                               (% :number-of-residents-present))})
              :no-residents-most-recent)))

(defn max-occupancy-totals [fn no]
  (reduce +
          (-> hiqa-reg-DB
              (tc/select-rows #(fn no (% :Maximum_Occupancy)))
              :Maximum_Occupancy)))

(def congregated-total-present
  (totals-present < 9))

(def decongregated-total-present
  (totals-present > 10))

(def congregated-max
  (max-occupancy-totals < 9))

(def decongregated-max
  (max-occupancy-totals > 10))

{::clerk/visibility {:result :show}}
(clerk/row
 (clerk/vl
  {:$schema "https://vega.github.io/schema/vega-lite/v5.json",
   :description "Maximum Occupancy (HIQA Register)",
   :data {:values [{:category "Congregated"  :value congregated-total-present}
                   {:category "Decongregated"  :value decongregated-total-present}]}
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
 (clerk/vl
  {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
   :description "Maximum Occupancy (HIQA Register)"
   :data {:values [{:category "Congregated"  :value congregated-max}
                   {:category "Decongregated"  :value decongregated-max}]}
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
             :color {:value "white"}}}]}))
