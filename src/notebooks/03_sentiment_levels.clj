(ns notebooks.03-sentiment-levels
  (:require
   [clojure.edn :as edn]
   [tablecloth.api :as tc]))

(def DS (tc/dataset "resources/datasets/created/GPT_responses.csv" {:key-fn keyword}))


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
