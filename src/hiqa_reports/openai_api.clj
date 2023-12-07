(ns hiqa-reports.openai-api
  (:require [wkok.openai-clojure.api :as api]
            [clojure.edn :as edn]
            [hiqa-reports.parsers-writers :refer [pdf-info-DB]]))

(def auth (:auth (edn/read-string (slurp "creds.edn"))))

(def prompt "Summarize the following text into 5 keywords reflecting the sentiment of the residents, and also assign an overall rating of 'positive', 'negative' or 'neutral' based on these sentiments. Finally, summarise the text in two sentences. Return the answer in edn format {:rating rating :keywords keywords :summary summary}. Here is the text:")


(defn request-keyword-summary [observations]
  (api/create-chat-completion {:model "gpt-3.5-turbo"
                               :messages [{:role "user" :content (str prompt observations)}]}
                              {:api-key auth}))


(comment
  (request-keyword-summary (:observations (first pdf-info-DB))))

;; Test-run:
;; Value: "{:rating \"positive\", :keywords [\"needs\", \"improvements\", \"comfortable\", \"clean\", \"engaging\"], :summary \"Residents were generally satisfied with the service provided, although improvements were needed in certain areas. The staff were observed to be attentive and the premises were well-maintained.\"}"
