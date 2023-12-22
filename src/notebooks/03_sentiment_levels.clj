(ns notebooks.03-sentiment-levels
  (:require
   [clojure.edn :as edn]
   [hiqa-reports.parsers-writers :as dat]
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [tablecloth.api :as tc]
   [nextjournal.clerk :as clerk]
   [aerial.hanami.templates :as ht]))

(def DS (-> (tc/dataset "resources/datasets/created/GPT_responses.csv" {:key-fn keyword})
            (tc/drop-missing :report-id)))

(def DS_joined
  (-> dat/DS_pdf_info
      (tc/full-join DS :report-id)))


(defn percentage-rating [row type]
  (float (/ (count (filter #(= % type) row))
            (count row))))


(def rating-by-year
  (-> DS_joined
      (tc/group-by :year)
      (tc/aggregate {:percent-positive #(percentage-rating (% :rating) "positive")
                     :percent-negative #(percentage-rating (% :rating) "negative")
                     :percent-neutral #(percentage-rating (% :rating) "neutral")})
      (tc/rename-columns {:$group-name :year})))



(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:values [{:category "Positive" :value (->> (:rating DS)
                                                    (filter #(= (str %) "positive"))
                                                    count)}
                  {:category "Neutral"  :value (->> (:rating DS)
                                                    (filter #(= (str %) "neutral"))
                                                    count)}
                  {:category "Negative" :value (->> (:rating DS)
                                                    (filter #(= (str %) "negative"))
                                                    count)}]}
  :title "Overall Sentiment"
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



(clerk/row
 (clerk/vl
  (hc/xform
   ht/grouped-bar-chart
   :DATA (-> rating-by-year
             (tc/rows :as-maps))
   :X :year :XTYPE :nominal
   :Y :percent-positive :YTYPE :quantitative))
 (clerk/vl
  (hc/xform
   ht/grouped-bar-chart
   :DATA (-> rating-by-year
             (tc/rows :as-maps))
   :X :year :XTYPE :nominal
   :Y :percent-negative :YTYPE :quantitative)))

(def rating-by-year-m
  (reduce (fn [result ds]
            (let [rating (:rating ds)
                  year (first (:year ds))
                  positive (count (filter #(= % "positive") rating))
                  negative (count (filter #(= % "negative") rating))
                  neutral (count (filter #(= % "neutral") rating))]
              (conj result
                    (-> {}
                        (assoc :year year)
                        (assoc :type "positive")
                        (assoc :count positive))
                    (-> {}
                        (assoc :year year)
                        (assoc :type "negative")
                        (assoc :count negative))
                    (-> {}
                        (assoc :year year)
                        (assoc :type "neutral")
                        (assoc :count neutral)))))
          []
          (-> DS_joined
              (tc/group-by :year)
              :data)))

(clerk/vl
 {:data {:values rating-by-year-m}
  :mark "bar"
  :width 400
  :height 400
  :encoding {:y {:aggregate :sum :field :count
                 :stack :normalize}
             :x {:field :year}
             :color {:field :type}}})

   




(comment
  (reverse
   (sort-by val
            (frequencies
             (flatten
              (map edn/read-string
                   (-> DS
                       (tc/select-rows #(= "positive" (% :rating)))
                       :phrases)))))))

(comment
  (count
   (reverse
    (sort-by val
             (frequencies
              (flatten
               (map edn/read-string
                    (-> DS
                        (tc/select-rows #(= "positive" (% :rating)))
                        :keywords))))))))
