(ns notebooks.04-datasets
  #:nextjournal.clerk{:visibility {:code :fold}, :toc :collapsed}
  (:require
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [hiqa-reports.parsers-writers :as dat]))

(clerk/table
 (-> dat/DS_pdf_info
     (tc/drop-columns :observations)
     (tc/info)
     (tc/select-columns
      [:col-name
       :n-missing
       :min
       :mean
       :mode
       :max])))



(clerk/table
 (-> dat/DS_pdf_info
     (tc/info)))



(clerk/table
 (-> dat/DS_pdf_info_agg_compliance
     (tc/select-columns [:total])
     (tc/info)))

(clerk/table
 (-> dat/DS_pdf_info
     (tc/select-columns [:number-of-residents-present])
     (tc/info)))
