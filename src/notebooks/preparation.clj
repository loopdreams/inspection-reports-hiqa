(ns notebooks.preparation
  #:nextjournal.clerk{:visibility {:code :fold}, :toc true}
  (:require
   [clojure.string :as str]
   [hiqa-reports.hiqa-register :refer [hiqa-reg-tbl]]
   [hiqa-reports.pdf-scrape :refer [report-list-by-year]]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))

(comment
  (clerk/serve! {:browse? true :watch-paths ["src/notebooks"]}))

;; # Preparation Steps for Analysis of Hiqa Reports
;; **Date**: 29th November, 2023

;; Hiqa inspection reports are published in pdf format in their [website](https://www.hiqa.ie/reports-and-publications/inspection-reports).
;;
;; There are 5,512 reports listed at the time of writing. I was only interested in looking at the reports on disability centres (a smaller subset).
;;
;; The Hiqa website is disgned as a SPA, so it was difficult to find a way to automate filtering/searching based on the reports interface.
;; Luckily, Hiqa also have a [register](https://www.hiqa.ie/areas-we-work/disability-services) for disability providers which also includes URLs for each centre.
;; Within these pages ([example of a centre page](https://www.hiqa.ie/areas-we-work/find-a-centre/lakeshore-services)), they also list inspection reports related to the centre,
;; so I used these URLs to build the list of reports relating to disability services. I then added these on to the Hiqa register to refer back to them later when downloading the reports.
;; For reference, here are the first 10 lines of the Hiqa register with the additional report information added.

(clerk/table
 (-> hiqa-reg-tbl
     (tc/select-columns [:Centre_ID :County :Registration_Date :URL :reports])
     (tc/select-rows (range 11))))

;; Some additional info on the HIQA register (with added reports column):

(clerk/table
 (-> hiqa-reg-tbl (tc/info :columns)))



;; Not all centres have inspection reports, and some centres have multiple. In total, I found links to **3,153** inspection reports.
;;
;; Looking at the filenames for the reports, which contain the date, the following additional information was obtained from this list:

(clerk/table
 (for [year (keys report-list-by-year)
       :let [c (count (year report-list-by-year))]]
   {:year (name year)
    :number-of-reports c}))

