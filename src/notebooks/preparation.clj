(ns notebooks.preparation
  #:nextjournal.clerk{:visibility {:code :fold}, :toc true}
  (:require
   [clojure.string :as str]
   [hiqa-reports.hiqa-register :refer [hiqa-reg-DB]]
   [hiqa-reports.pdf-get :refer [report-list-by-year]]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))

(comment
  (clerk/serve! {:browse? true :watch-paths ["src/notebooks"]}))

;; # Preparation Steps for Analysis of Hiqa Reports
;; **Date**: 29th November, 2023
;;
;; ## Key Questions
;; ### Main Questions
;; - What is the average experience/sentiment of people in the centres?
;; - What are the compliance levels in relation to the various regulations?
;; ### Related Questions
;; - Is there a relationship between number of residents and positivie sentiment/experience?
;; - Is there a relationship between number of residents and compliance levels?
;; - Is there a relationship between geography and comliance levels?
;; - Is there a relationship between provider and compliance levels/sentiment?
;;
;; ## Finding report URLs
;; Hiqa inspection reports are published in pdf format in their [website](https://www.hiqa.ie/reports-and-publications/inspection-reports).
;;
;; There are 5,512 reports listed at the time of writing. I was only interested in looking at the reports on disability centres (a smaller subset).
;;
;; The Hiqa website is disgned as a SPA, so it was difficult to find a way to automate filtering/searching based on the reports interface.
;; Luckily, Hiqa also have a [register](https://www.hiqa.ie/areas-we-work/disability-services) for disability providers which also includes URLs for each centre.
;; Within these pages ([example of a centre page](https://www.hiqa.ie/areas-we-work/find-a-centre/lakeshore-services)), they also list inspection reports related to the centre,
;; so I used these URLs to build the list of reports relating to disability services. I then added these on to the Hiqa register to refer back to them later when downloading the reports.
;; For reference, here are the first 10 lines of the Hiqa register with the additional report information added.
;;
;; The HIQA register with the additional report addresses is [located here :TODO: add link](link).

(clerk/table
 (-> hiqa-reg-DB
     (tc/select-columns [:Centre_ID :County :Registration_Date :URL :reports])
     (tc/select-rows (range 11))))

;; Some additional info on the HIQA register (with added reports column):

(clerk/table
 (-> hiqa-reg-DB (tc/info :columns)))



;; Not all centres have inspection reports, and some centres have multiple. In total, I found links to **3,153** inspection reports.
;;
;; Looking at the filenames for the reports, which contain the date, the following additional information was obtained from this list:

(clerk/table
 (for [year (keys report-list-by-year)
       :let [c (count (year report-list-by-year))]]
   {:year (name year)
    :number-of-reports c}))


;; After downloading the reports, **3,098** (98%) of the URLs suceeded.
;;
;; ## Inspection Reports Format
;;
;; The key enabling feature for this analysis is that the inspection reports are written in a __standard format__, making them quite easy to parse.
;; So, rather than using an additional library for extracting tables from pdfs, I simply used [pdfbox](https://pdfbox.apache.org/) (via clojure wrapper - [pdfboxing](https://github.com/dotemacs/pdfboxing)) to extract the text.
;;
;; Then, I looked for particular markers in the text, to extract the following information:
;;  1. 'Frontmatter' info: the table on the first page containing:
;;      - Name of centre
;;      - Name of provider
;;      - Address (e.g., Dublin 9)
;;      - Type of Inspection (e.g., 'unannounced')
;;      - Date of Inspection
;;      - Centre ID
;;      - Fieldwork ID
;;  2. Number of residents present on the date of inspection
;;  3. "What residents told us and what inspectors observed"
;;  4. Levels of compliance for regulations (in 'Appendix 1')
;;
;; The last two areas (3 and 4) were the main ones I was hoping to analyse. The 'Centre ID' in the frontmatter was also
;; central for linking this information to the HIQA register.
;;
;; Since multiple reports exist for each centre, I also created a custom 'report ID' which was just 'centreID-Date of inspection',
;; for example, '1234-20230101'.
;;
;; There is other information you could extract from the reports (for example, more detail behind the compliance judgements), but for
;; now I decided to restrict the focus to these 4 areas. One thing to look into in the future could be also recording the judgement detail
;; in cases where the centre is 'Not Compliant' for a regulation, in order to see what kinds of things drive this.
;;
;; ## Aggregation of compliance levels
;; TODO
;;
;; ## Aggregation of sentiment analysis
;;
;; I am not a data scientist, so I used ChatGPT via the Openai API ("gpt-3.5-turbo") to analyse the text relating to residents' feedback.
;; This version of ChatGPT is also the lowest cost version ($0.0010 / 1K input tokens at time of writing) , because I am also not a millionaire.
;; It would be interesting to see what kind of analysis GPT4 could produce.
;;
;; As a test, I input the following text into the GUI verison of ChatGPT and asked it for 5 keywords that reflect the sentiment of
;; the people living at the centre:
;;
;; > This was an unannounced risk-based inspection of the centre following up on the
;; previous risk-based inspections of the designated centre. Due to an inappropriate
;; emergency admission to the centre there had been a continuation of peer-to-peer
;; behavioural and alleged safeguarding incidents occurring in the residents' home.
;; Since the previous inspection the provider applied to vary the conditions of
;; registration for the centre and add an additional residential unit to the centre's
;; footprint. This was to support a resident, with complex needs, move to a more
;; suitable living arrangement. As a result of this action taken by the provider, there
;; had been a significant reduction of peer-to-peer incidents occurring in the centre.
;; Overall, these actions taken by the provider had resulted in positive outcomes for all
;; residents concerned and had led to a much improved lived experience for residents
;; in their home.
;;
;; > The inspector was provided the opportunity to meet with all six residents. Most
;; residents spoke in detail with the inspector and were able to relay their views about
;; their lived experience. Where residents were unable, or did not want to relay their
;; views, on-site observations, conversations with the person in charge and staff and a
;; review of documentation were used to inform a judgment on residents' experience
;; of living in the centre.
;;
;; > On the day of the inspection, the inspector visited the two premises within the
;; designated centre. The inspector was informed that the resident in the single
;; occupancy house might find it difficult for an unfamiliar person to be in their home
;; with them. To accommodate this, the inspector provided a morning's notice of their
;; visit to the residents home. In addition, a copy of the Health Information and
;; Quality Authority,(HIQA), ''nice to meet you'' document, to provide information
;; about the inspector and their reason for being there, was sent to the resident in
;; advance of the visit.
;;
;; > In the morning, on arrival at the house where five residents were living, the
;; inspector met with three residents who were in the kitchen. There was a calm,
;; relaxed and homely feel to the environment. Residents were sitting and chatting
;; amongst themselves and to staff. Residents were smiling, appearing content and
;; happy.
;;
;; > One of the residents showed the inspector their room and pointed out some of the
;; improvements to the décor that had taken place since the last inspection. The room
;; had been painted a colour that was of the resident's choosing. They resident said
;; they were really happy with the colour of their room. They told the inspector that
;; there were plans in place to purchase new curtains and a new chair was on the way.
;; The inspector noticed that some improvements were still needed to the room and in
;; particular, to storage the resident’s personal care items.
;;
;; > The resident told the inspector that they were now very happy living in the
;; designated centre. While they had previously said they wanted to leave the house,
;; this was no longer the case as they were now very happy to continue living in the
;; centre. The resident said that everyone was happy since there had been a change to
;; the number of residents living in the centre. They said it was a quieter, more
;; relaxed place.
;;
;; > The inspector spoke with another resident, who had previously told the inspector
;; that they were unhappy with who they were living with and at the time of relaying
;; this information, appeared nervous and uncomfortable. However, on the day of the
;; inspection, when speaking to the inspector about their lived experience in their
;; home, they were smiling, appeared content and relaxed and said they were very
;; happy now.
;;
;; > Later in the day, the inspector met with another resident who also informed the
;; inspector that they were happy living in the house. The resident was due to
;; celebrate a milestone birthday at the weekend and seemed happy about the plans in
;; place for a house birthday party. Some of the residents excitedly told the inspector
;; about the plans in place for the birthday, including the type of cake being organised.
;; In the afternoon, on arrival at the newly added premises, while the resident showed
;; no signs of upset at the inspector being in their home, the resident chose not to
;; engage with the inspector. The inspector observed the resident, supported by their
;; staff member, preparing their evening meal. The resident appeared content cutting
;; and chopping up the ingredients needed to make the dish. The inspector was
;; informed by staff and the person in charge, that the resident enjoyed preparing and
;; making their own meal each evening.
;;
;; > The inspector carried out a walk-about of the new premises and found that the
;; house was bright, spacious and homely. The resident was provided with their own
;; bedroom and bathroom. The kitchen was large and provided a good space for
;; preparing and cooking food which was in line with the resident likes and
;; preferences. The sitting room was a good size and provided a comfortable space for
;; the resident to relax. There was a garden out the front and back of the house.
;; There was a separate small building at the back of the house which was used as a
;; laundry and storage room.
;;
;; > The resident had been supported, at a pace that met their needs, to transition into
;; this house three weeks previous to the inspection. This was a temporary home for
;; the resident until their full-time one bedroom apartment was available to them in
;; August 2023. There was a transition plan in place for the resident which included
;; two phases, the first being the move to the temporary location and the second their
;; permanent location. It was evident from speaking with the person in charge, staff
;; and a review of documentation that the resident was very much part of, and had
;; been consulted about, the transition to each location.
;; In summary, the inspector found that there had been significant improvements to
;; the lived experience of all residents living in the designated centre. There had been
;; a reduction in behavioural and safeguarding incidents occurring in the centre which
;; resulted in positive outcomes for residents. The inspector found that overall, each
;; resident’s well-being and welfare was maintained to a good standard and that there
;; was a strong and visible person-centred culture within the designated centre.
;; Through speaking with residents and staff, through observations and a review of
;; documentation, it was evident that staff and the local management team were
;; striving to ensure that residents lived in a supportive and caring environment.
;; The next two sections of the report present the findings of this inspection in relation
;; to the governance and management arrangements in place in the centre and how
;; these arrangements impacted on the quality and
;;
;; ChatGPT provided the following keyword summary:
;;
;; 1. **Positive Outcomes:** The actions taken by the provider, including adding a residential unit and addressing residents' needs, resulted in a significant reduction of incidents and improved the lived experience.
;;
;; 2. **Contentment:** Residents expressed contentment and happiness with their living conditions, highlighting a calm, relaxed, and homely environment in the designated center.
;;
;; 3. **Improvements:** Residents noted improvements in their living spaces, such as personalized room decor, and expressed satisfaction with ongoing efforts to enhance their surroundings.
;;
;; 4. **Transition Support:** The transition of a resident to a new premises was well-managed, with a detailed plan in place, and the resident was consulted about and involved in the process.
;;
;; 5. **Person-Centered Culture:** The report indicates a strong and visible person-centered culture within the designated center, emphasizing staff and management's commitment to ensuring a supportive and caring environment for residents.
;;
;; I also asked for a summary of the text in two sentences:
;;
;; > The unannounced risk-based inspection of the designated center revealed significant
;; improvements in residents' lived experiences, with positive outcomes stemming from the
;; provider's actions, including the addition of a residential unit. Residents expressed contentment,
;; and a person-centered culture was evident, contributing to an overall positive environment in the center.
;;
;; Finally, I asked it to assign a rating of 'positive', 'negative' or 'neutral' to the centre:
;;
;; > Positive
;;
;; ## Outputs
;;
;; - A version of the HIQA register with links to inspection reports for the centre (csv)
;; - TODO An aggregation of the __compliance levels__ for each of the regulations (separate page in this notebook)
;; - TODO A sentiment analysis/keyword analysis or the report sections "What residents told us and what inspectors observed" using chatGPT
