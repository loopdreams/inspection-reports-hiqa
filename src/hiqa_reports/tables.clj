(ns hiqa-reports.tables
  (:require
   [hiqa-reports.parsers-writers :as dat]
   [clojure.set :as set]
   [clojure.string :as str]
   [tablecloth.api :as tc]))

;; Drafts for analysis


;; Utilities
(defn keywordise-when-spaces [string]
  (if (string? string)
    (keyword (str/lower-case (str/replace string #" " "")))
    string))

;; Compliance groupings

(defn compliance-by-group [ds group type]
  (-> ds
      (tc/group-by group)
      (tc/aggregate #(float
                      (/ (reduce + (% type))
                         (count (% type)))))
      (tc/rename-columns {"summary" type})))

(def non-compliance-by-provider
  (compliance-by-group
   dat/DS_pdf_info_agg_compliance
   :name-of-provider
   :percent-noncompliant))

(def full-compliance-by-provider
  (compliance-by-group
   dat/DS_pdf_info_agg_compliance
   :name-of-provider
   :percent-fully-compliant))

(def non-compliance-by-area
  (compliance-by-group
   dat/DS_pdf_info_agg_compliance
   :address-of-centre
   :percent-noncompliant))

(def full-compliance-by-area
  (compliance-by-group
   dat/DS_pdf_info_agg_compliance
   :address-of-centre
   :percent-fully-compliant))
      

;; Compliance Levels per reg

(defn judgement-for-reg-no [entry reg-no]
  (:judgement (first (filter #(= reg-no (:reg-no %)) (:compliance-levels entry)))))

(defn compliance-levels-for-reg [entries reg-no]
  (reduce (fn [result entry]
            (let [id (:report-id entry)]
              (assoc result (keyword id) (judgement-for-reg-no entry reg-no))))
          {}
          entries))

(defn compliance-levels-table [entries area area-nos]
  (reduce (fn [tbl reg-no]
            (conj tbl
                  (let [area-name area
                        counts (set/rename-keys (frequencies (vals (compliance-levels-for-reg entries reg-no)))
                                                {nil :unchecked})]
                    (into counts
                          {:area area-name
                           :regulation (get (area-name dat/hiqa-regulations) reg-no)}))))
          []
          area-nos))


(defn compliance-levels-table-quality [entries]
  (compliance-levels-table entries :quality-and-safety dat/quality-and-safety-nos))

(defn compliance-levels-table-capacity [entries]
  (compliance-levels-table entries :capacity-and-capability dat/capacity-and-capability-nos))

(defn make-compliance-tc-table [entries data-fn]
  (let [data (data-fn entries)]
    (-> data
        (tc/dataset {:key-fn keywordise-when-spaces})
        (tc/reorder-columns [:area :regulation :compliant]))))


(defn aggregate-judgements [ds]
  (-> ds
      (tc/select-columns #":Regulation.*")
      (tc/aggregate-columns #(frequencies (remove nil? %)))
      (tc/rows :as-maps)
      first))

(defn aggregate-compliance-levels [ds]
  (let [ms (aggregate-judgements ds)]
    (for [k     (keys ms)
          :let  [n (name k)
                 num (re-find #"\d+" n)]
          :when num
          :let  [num (parse-long num)
                 judgement (re-find #"Compliant|Not compliant|Substantially compliant" n)
                 area (if ((:capacity-and-capability dat/hiqa-regulations) num)
                        :capacity-and-capability
                        :quality-and-safety)
                 full-name (str (first (str/split n #"-")) ": " ((area dat/hiqa-regulations) num))
                 reg-name ((area dat/hiqa-regulations) num)]]
      {:area      area
       :number    num
       :name      full-name
       :reg-name  reg-name
       :judgement judgement
       :value     (k ms)})))

(def DS_agg_compliance_per_reg
  (-> (tc/dataset (aggregate-compliance-levels dat/DS_pdf_info))
      (tc/pivot->wider :judgement :value)
      (tc/rename-columns {"Compliant" :compliant
                          "Substantially compliant" :substantiallycompliant
                          "Not compliant" :notcompliant})))





;; Compliance grouping
;;
(defn compliance-levels-by-frontmatter-area [entries frontmatter-area reg-no]
  (reduce (fn [result entry]
            (let [group      (get (:frontmatter entry) frontmatter-area)
                  compliance (compliance-levels-for-reg entry reg-no)]
              (update-in result [group compliance] (fnil + 0) 1)))
          {}
          entries))

;; TODO Validate this...
(defn make-regulation-table
  "Group is either :provider, :area or :id"
  [entries reg-no group]
  (let [extracts
        (reduce (fn [table entry]
                  (conj table
                        (let [fm        (:frontmatter entry)
                              judgement (judgement-for-reg-no entry reg-no)]
                          {:id                      (:centre-id entry)
                           :report-id               (:report-id entry)
                           :provider                (:name-of-provider fm)
                           :area                    (:address-of-centre fm)
                           :compliant               (if (= judgement "Compliant") 1 0)
                           :notcompliant           (if (= judgement "Not compliant") 1 0)
                           :substantiallycompliant (if (= judgement "Substantially compliant") 1 0)})))
                []
                entries)]
    (-> extracts
        tc/dataset
        (tc/group-by group)
        (tc/aggregate {:compliant #(reduce + (% :compliant))
                       :notcompliant #(reduce + (% :notcompliant))
                       :substantiallycompliant #(reduce + (% :substantiallycompliant))})
        (tc/rename-columns {:$group-name group}))))


(comment
  (->
   (make-regulation-table dat/pdf-info-DB-deprecated 23 :area)
   (tc/order-by [:not-compliant] [:desc])))


;; Frequencies

(defn observations-by-report-id [entries]
  (reduce #(assoc %1 (keyword (:report-id %2)) (:observations %2)) {} entries))

(def stopwords
  (str/split-lines
   (slurp "https://gist.githubusercontent.com/sebleier/554280/raw/7e0e4a1ce04c2bb7bd41089c9821dbcf6d0c786c/NLTK's%2520list%2520of%2520english%2520stopwords")))

;; (reverse
;;  (sort-by val
;;           (frequencies
;;            (remove (set stopwords)
;;                    (map str/lower-case
;;                         (str/split
;;                          (str/replace
;;                           (str/join (vals (observations-by-report-id test-group)))
;;                           #"\.|," "")
;;                          #"\s+"))))))

