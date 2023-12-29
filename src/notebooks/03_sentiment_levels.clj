(ns notebooks.03-sentiment-levels
  #:nextjournal.clerk{:visibility {:code :fold}, :toc :collapsed}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [hiqa-reports.parsers-writers :as dat]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [cheshire.core :as json]))

{::clerk/visibility {:result :hide}}
(def DS_sentiment (-> (tc/dataset "resources/datasets/created/GPT_responses.csv" {:key-fn keyword})
                      (tc/drop-missing :report-id)))
(def DS_joined
  (-> dat/DS_pdf_info
      (tc/full-join DS_sentiment :report-id)))

(def DS_joined_compliance
  (-> dat/DS_pdf_info_agg_compliance
      (tc/full-join DS_sentiment :report-id)))

(comment
  (filter (fn [[idx v]]
            (> v 5))
          (map-indexed vector
                       (map (comp count read-string)
                            (-> DS_sentiment
                                :phrases))))
  (filter #(nil? (second %)) (map-indexed vector (-> DS_sentiment :phrases)))
  (remove #(some #{(second %)} ["positive" "negative" "neutral"])
          (map-indexed vector (-> DS_sentiment
                                  :rating)))

  (count (-> DS_sentiment :rating))
  (count (map clojure.edn/read-string (-> DS_sentiment
                                          :rating)))
  (filter #(not= 5 (second %)) (map-indexed vector (map (comp count clojure.edn/read-string) (-> DS_sentiment :keywords))))


  (nth (-> DS_sentiment
           :report-id)
       118))



;; # Sentiment Levels

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
;; - **rating** (positive/negative/neutral)
;; - **keywords** (5)
;; - **key phrases** (3)
;; - **summary** (2 sentences)
;;
;; There are a couple of major caveats here:
;;
;; 1. I used the least powerful version of GPT. The cost was around $6.69 for 3,733 requests (this included some trail and error requests at the outset). The next most powerful api (`GPT-4`) would have cost around 30x this.
;; 2. I am not very familiar with the GPT model, especially questions around how best to formulate the prompt. There were probably better ways to formulate the requests. This exercise was primarily exploratory in nature, therefore a limited amount of time was spent engineering the prompt.

;; **Information about the text**
{::clerk/visibility {:result :show}}
(clerk/md
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

;; ## Rating
;;
;; The majority of the user experience/inspector observations in the reports
;; were assigned a rating of **positive** by the GPT model.

{::clerk/visibility {:result :hide}}
(defn percentage-rating [row type]
  (float (/ (count (filter #(= % type) row))
            (count row))))

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

(def dublin-positive-rating
  (let [dub-ratings (-> DS_joined
                        (tc/select-rows #(re-find #"Dublin" (% :address-of-centre)))
                        :rating)
        num-positive (count (filter #(= "positive" %) dub-ratings))
        percent-pos (float (/ num-positive (count dub-ratings)))]
    {:area "Dublin" :percentage-positive percent-pos}))

(def outside-dub-positive-ratings
  (-> DS_joined
      (tc/drop-rows #(re-find #"Dublin" (% :address-of-centre)))
      (tc/group-by :address-of-centre)
      (tc/aggregate {:percentage-positive #(float (/
                                                   (count (filter (fn [r] (= "positive" r)) (% :rating)))
                                                   (count (% :rating))))})
      (tc/rename-columns {:$group-name :area})
      (tc/rows :as-maps)))

(def percentage-positive-regions-m (conj outside-dub-positive-ratings dublin-positive-rating))

(def ireland-map (slurp "resources/datasets/imported/irish-counties-segmentized.topojson"))

{::clerk/visibility {:result :show}}
(clerk/col
 (clerk/vl
  {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
   :data {:values [{:category "Positive" :value (->> (:rating DS_sentiment)
                                                     (filter #(= (str %) "positive"))
                                                     count)}
                   {:category "Neutral"  :value (->> (:rating DS_sentiment)
                                                     (filter #(= (str %) "neutral"))
                                                     count)}
                   {:category "Negative" :value (->> (:rating DS_sentiment)
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

 (clerk/vl
  {:data {:values rating-by-year-m}
   :mark {:type "bar" :tooltip true}
   :width 400
   :height 400
   :encoding {:y {:aggregate :sum :field :count
                  :stack :normalize
                  :title "%"}
              :x {:field :year}
              :color {:field :type}}})

 (clerk/vl
  {:$schema   "https://vega.github.io/schema/vega-lite/v5.json"
   :data      {:format {:feature "counties" :type "topojson"}
               :values ireland-map}
   :title     "% Positive by Area"
   :width 400
   :height 400
   :transform [{:lookup "id"
                :from   {:data   {:values percentage-positive-regions-m}
                         :fields ["percentage-positive"]
                         :key    "area"}}]

   :mark     "geoshape"
   :encoding {:color {:field "percentage-positive" :type "quantitative"}}}))

;; ## Keywords

{::clerk/visibility {:result :hide}}
(defn sort-words [ds rating type]
  (->>
   (-> ds
       (tc/select-rows #(= rating (% :rating)))
       type)
   (map edn/read-string)
   flatten
   (map str/upper-case)
   frequencies
   (remove #(= (first %) "INSPECTION"))
   (remove #(= (first %) "RESIDENTS"))
   (sort-by val)
   reverse))

(defn sort-words-all-rating [ds type]
  (->> (ds type)
       (map edn/read-string)
       flatten
       (map str/upper-case)
       frequencies
       (remove #(= (first %) "INSPECTION"))
       (remove #(= (first %) "RESIDENTS"))
       (sort-by val)
       reverse))
       

{::clerk/visibility {:result :show}}
(clerk/md
 (let [positive (take 5 (sort-words DS_sentiment "positive" :keywords))
       negative (take 5 (sort-words DS_sentiment "negative" :keywords))
       neutral  (take 5 (sort-words DS_sentiment "neutral" :keywords))]
   (str/join "\n\n"
             [(str "**Top 5 Keywords for Centres with 'posivite' rating:**\n"
                   (str/join "\n"
                             (for [word positive]
                               (str "- " (str/lower-case (first word))))))
              (str "**Top 5 Keywords for Centres with 'negative' rating:**\n"
                   (str/join "\n"
                             (for [word negative]
                               (str "- " (str/lower-case (first word))))))
              (str "**Top 5 Keywords for Centres with 'neutral' rating:**\n"
                   (str/join "\n"
                             (for [word neutral]
                               (str "- " (str/lower-case (first word))))))])))

{::clerk/visibility {:result :hide}}
(def positive-keywords-wc
  (reduce #(conj %1 (-> {}
                        (assoc :text (first %2))
                        (assoc "size" (second %2))))
          []
          (sort-words DS_sentiment "positive" :keywords)))

(def positive-keywords-wc-scale-size
  (map #(update % "size" (comp int /) 10)
       positive-keywords-wc))

(def positive-keywords-as-str
  (str/join " "
            (for [word positive-keywords-wc-scale-size
                  :let [w (word :text)
                        c (word "size")]]
              (str/join " "
                        (repeat c w)))))
              
(defn word-cloud [data]
 {:$schema "https://vega.github.io/schema/vega/v5.json"
  :width   800
  :height  400
  :padding 0
  :data    [{:name   "table"
             :values [data]
             :transform
             [{:type "countpattern"
               :field "data"
               :case "upper"
               :pattern "[\\w']{3,}"
               :stopwords ""}
              {:type "formula" :as "angle"
               :expr "[-45, 0, 45][~~(random() * 3)]"}]}]
  :scales  [{:name   "color"
             :type   "ordinal"
             :domain {:data "table" :field "text"}
             :range  ["#d5a928", "#652c90", "#939597"]}]
  :marks   [{:type   "text"
             :from   {:data "table"}
             :encode {:enter
                      {:text     {:field "text"}
                       :align    {:value "center"}
                       :baseline {:value "alphabetic"}
                       :fill     {:scale "color" :field "text"}}
                      :update {:fillOpacity {:value 1}}
                      :hover  {:fillOpacity {:value 0.5}}}
             :transform
             [{:type          "wordcloud"
               :size          [800 400]
               :text          {:field :text}
               :rotate        {:field "datum.angle"}
               :font          "Helvetica Neue, Arial"
               :fontSize      {:field "datum.count"}
               :fontWeight     600
               :fontSizeRange [12, 56]
               :padding       2}]}]})

(defn word-cloud-2 [data]
 {:$schema "https://vega.github.io/schema/vega/v5.json"
  :width   800
  :height  400
  :padding 0
  :data    [{:name   "table"
             :values data
             :transform
             [{:type "formula" :as "angle"
               :expr "[-45, 0, 45][~~(random() * 3)]"}]}]
  :scales  [{:name   "color"
             :type   "ordinal"
             :domain {:data "table" :field "text"}
             :range  ["#d5a928", "#652c90", "#939597"]}]
  :marks   [{:type   "text"
             :from   {:data "table"}
             :encode {:enter
                      {:text     {:field "text"}
                       :align    {:value "center"}
                       :baseline {:value "alphabetic"}
                       :fill     {:scale "color" :field "text"}}
                      :update {:fillOpacity {:value 1}}
                      :hover  {:fillOpacity {:value 0.5}}}
             :transform
             [{:type          "wordcloud"
               :size          [800 400]
               :text          {:field :text}
               :rotate        {:field "datum.angle"}
               :font          "Helvetica Neue, Arial"
               :fontSize      {:field "datum.size"}
               :fontSizeRange [12 56]
               :fontWeight    600
               :padding       1}]}]})

{::clerk/visibility {:result :show}}
(clerk/vl
 (word-cloud-2 (take 200 positive-keywords-wc)))

{::clerk/visibility {:result :hide}}
(def negative-keywords-wc
  (reduce #(conj %1 (-> {}
                        (assoc :text (first %2))
                        (assoc "size" (second %2))))
          []
          (sort-words DS_sentiment "negative" :keywords)))

{::clerk/visibility {:result :show}}
#_(clerk/vl
   (word-cloud-2 negative-keywords-wc))

;; ## Phrases
{::clerk/visibility {:result :hide}}
(def postive-phrases-wc
  (reduce #(conj %1 (-> {}
                        (assoc :text (first %2))
                        (assoc "size" (second %2))))
          []
          (sort-words-all-rating DS_sentiment :phrases)))

{::clerk/visibility {:result :show}}
#_(clerk/vl
   (word-cloud-2 postive-phrases-wc))
        
      

;; ## Example Summaries for Three Random Entries

{::clerk/visibility {:result :hide}}
(def summary-info-list
  [["Provider: " :name-of-provider]
   ["Area: " :address-of-centre]
   ["Inspection Type: " :type-of-inspection]
   ["Number of Residents Present: " :number-of-residents-present]
   ["Number of Compliant Regulations: " :num-compliant]
   ["Number of Substantially Compliant Regulations: " :num-substantiallycompliant]
   ["Number of Non Compliant Regulations: " :num-notcompliant]
   ["GPT Rating: " :rating]])

(defn random-summary-page [number]
  (let [target    (nth (:report-id DS_joined_compliance) number)
        data      (-> DS_joined_compliance (tc/select-rows #(= target (% :report-id))))
        centre-id (first (:centre-id data))
        summary   (first (:summary data))
        URL       (first (:report-URL data))
        keywords  (first (:keywords data))
        phrases   (first (:phrases data))
        date      (first (:date data))]
    [:div
     [:h3 (str "Inspection at centre " centre-id " on " (str date))]
     [:em summary]
     [:hr]
     (when URL
       [:a {:href URL} "Full Report"])
     [:ul
      (for [li   summary-info-list
            :let [[title kw] li
                  info (first (kw data))]]
        [:li [:b title] (str info)])]
     [:b "Keywords and Key Phrases:"]
     [:ul (for [kw (concat (read-string keywords) (read-string phrases))]
            [:li kw])]]))

(def random-entries (take 3 (repeatedly #(rand-int (count (:report-id DS_joined_compliance))))))

{::clerk/visibility {:result :show}}
(clerk/html (random-summary-page (first random-entries)))
(clerk/html (random-summary-page (second random-entries)))
(clerk/html (random-summary-page (last random-entries)))
