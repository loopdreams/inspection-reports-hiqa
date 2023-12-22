(ns notebooks.04-datasets
  #:nextjournal.clerk{:visibility {:code :fold}, :toc :collapsed}
  (:require
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [hiqa-reports.parsers-writers :as dat]))

(clerk/table
 (-> dat/DS_pdf_info
     (tc/info :columns)))
