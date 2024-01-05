(ns notebooks.05-regional
  #:nextjournal.clerk{:visibility {:code :fold}, :toc true}
  (:require
   [aerial.hanami.common :as hc]
   [notebooks.01-general-information :refer [most-recent-resident-no]]
   [hiqa-reports.hiqa-register :refer [DS-hiqa-register]]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [hiqa-reports.parsers-writers :as dat]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))

;; ## Inspections per Region

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

;; ## Coverage - comparison with census 2022

{::clerk/visibility {:result :hide}}
(def census-file "resources/datasets/imported/F4005.20240102T170117.csv")
(def census-total-population-file "resources/datasets/imported/F1001.20240102T200155.csv")


(defn census-label->keyword [label]
  (keyword
   (str/replace label #"[\"\" \p{C}]" "")))

(defn county-labels->county-names [label]
  (let [dublin-local-authorities ["Dublin City Council"
                                  "Fingal County Council"
                                  "South Dublin County Council"
                                  "Dún Laoghaire Rathdown County Council"]]
    (if (some #{label} dublin-local-authorities) "Dublin"
        (str/replace label #" City & County Council| County Council| City Council|  County Council" ""))))

(def census-f1001-total-pop
  (-> census-total-population-file
      (tc/dataset {:key-fn census-label->keyword})
      (tc/drop-columns [:StatisticLabel :CensusYear :Sex :UNIT])
      (tc/drop-rows #(= (% :County) "State"))
      (tc/rename-columns {:VALUE :population})))

(def census-f4005
  (-> census-file
      (tc/dataset {:key-fn census-label->keyword})
      (tc/drop-columns [:Sex :UNIT :CensusYear])
      (tc/drop-rows #(= "Ireland" (% :AdministrativeCounties)))
      (tc/map-columns :County :AdministrativeCounties
                      county-labels->county-names)
      (tc/drop-columns :AdministrativeCounties)
      (tc/group-by :StatisticLabel)))

(def census_f4005-great-extent
  (-> (first (:data census-f4005))
      (tc/group-by :County)
      (tc/aggregate {:Great_Extent #(reduce + (% :VALUE))})
      (tc/rename-columns {:$group-name :County})))

(def census_f4005-some-extent
  (-> (second (:data census-f4005))
      (tc/group-by :County)
      (tc/aggregate {:Some_Extent #(reduce + (% :VALUE))})
      (tc/rename-columns {:$group-name :County})))

(defn residents-present-by-region [ds]
  (let [region (first (:address-of-centre ds))
        residents-present (-> ds
                              (tc/drop-missing :number-of-residents-present)
                              (tc/group-by :centre-id)
                              (tc/aggregate {:no-residents-most-recent
                                             #(most-recent-resident-no
                                               (% :date)
                                               (% :number-of-residents-present))})
                              :no-residents-most-recent)]
    [region (reduce + residents-present)]))

(def no-residents-present-by-region
  (reduce (fn [result [region val]]
            (if (re-find #"Dublin" region)
              (update result "Dublin" (fnil + 0) val)
              (assoc result region val)))
          {}
          (map residents-present-by-region
               (-> dat/DS_pdf_info
                   (tc/group-by :address-of-centre)
                   :data))))

(def no-centres-by-region
  (reduce (fn [result ds]
            (let [centres (count (distinct (:centre-id ds)))
                  region (first (:address-of-centre ds))]
              (if (re-find #"Dublin" region)
                (update result "Dublin" (fnil + 0) centres)
                (assoc result region centres))))
          {}
          (-> dat/DS_pdf_info
              (tc/group-by :address-of-centre)
              :data)))


(def inspection-reports-regions
  (tc/dataset
   (for [county (keys no-centres-by-region)
         :let [residents (no-residents-present-by-region county)
               centres (no-centres-by-region county)]]
     {:County county :Number_of_Residents_Present residents :Number_of_Centres centres})))

(comment
  ;; TODO check the reason for the difference here..
  (reduce + (vals no-centres-by-region))

  (count (distinct (:centre-id dat/DS_pdf_info))))


(def hiqa-register-regions
  (-> DS-hiqa-register
      (tc/group-by :County)
      (tc/aggregate {:Maximum_Occupancy #(reduce + (% :Maximum_Occupancy))
                     :Centres #(count (distinct (% :Centre_ID)))})
      (tc/rename-columns {:$group-name :County})))


(def joined-regional-and-pop
  (-> inspection-reports-regions
      (tc/full-join hiqa-register-regions :County)
      (tc/full-join census_f4005-great-extent :County)
      (tc/full-join census_f4005-some-extent :County)
      (tc/full-join census-f1001-total-pop :County)))

{::clerk/visibility {:result :show}}
(clerk/row
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA
   (-> joined-regional-and-pop
       (tc/map-columns :percent-of-pop [:Number_of_Residents_Present :Great_Extent]
                       #(* 100 (float (/ %1 %2))))
       (tc/rows :as-maps))
   :TITLE "Residents Present as % of Population with Disability to a Great Extent"
   :X :County :XTYPE :nominal :XSORT "-y"
   :Y :percent-of-pop :YTITLE "%"))

 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA
   (-> joined-regional-and-pop
       (tc/map-columns :percent-of-pop [:Number_of_Residents_Present :population]
                       #(* 100 (float (/ %1 %2))))
       (tc/rows :as-maps))
   :TITLE "Residents Present as % of County Population"
   :X :County :XTYPE :nominal :XSORT "-y"
   :Y :percent-of-pop :YTITLE "%")))

(clerk/row
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA
   (-> joined-regional-and-pop
       (tc/map-columns :percent-of-pop [:Maximum_Occupancy :Great_Extent]
                       #(* 100 (float (/ %1 %2))))
       (tc/rows :as-maps))
   :TITLE "Maximum Occupancy as % of Population with Disability to a Great Extent"
   :X :County :XTYPE :nominal :XSORT "-y"
   :Y :percent-of-pop :YTITLE "%"))

 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA
   (-> joined-regional-and-pop
       (tc/map-columns :percent-of-pop [:Maximum_Occupancy :population]
                       #(* 100 (float (/ %1 %2))))
       (tc/rows :as-maps))
   :TITLE "Maximum Occupancy as % of County Population"
   :X :County :XTYPE :nominal :XSORT "-y"
   :Y :percent-of-pop :YTITLE "%")))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> joined-regional-and-pop
            (tc/map-columns :Avg-Res-Per-Centre [:Number_of_Residents_Present :Number_of_Centres]
                            #(float (/ %1 %2)))
            (tc/rows :as-maps))
  :X :County :XTYPE :nominal :XSORT "-y"
  :Y :Avg-Res-Per-Centre :YTYPE :quantitative))

(clerk/col
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA (-> joined-regional-and-pop
             (tc/rows :as-maps))
   :Y :County :YTYPE :nominal :YSORT "-x"
   :X :Great_Extent))
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA (-> joined-regional-and-pop
             (tc/rows :as-maps))
   :Y :County :YTYPE :nominal :YSORT "-x"
   :X :Number_of_Residents_Present))
 (clerk/vl
  (hc/xform
   ht/bar-chart
   :DATA (-> joined-regional-and-pop
             (tc/rows :as-maps))
   :Y :County :YTYPE :nominal :YSORT "-x"
   :X :Maximum_Occupancy)))
