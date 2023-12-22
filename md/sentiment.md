# Sentiment Levels

Aggregate 'sentiment' levels were obtained by passing the text contained under the section **What residents told us and what inspectors observed** to `GPT-3.5` for evaluation.

The following prompt was provided:

> "Summarize the following text into 5 keywords reflecting the sentiment of the residents. Do not include the word 'residents' as a keyword."

> "Also provide 3 key phrases reflecting the sentiment of the residents"

> "Also assign an overall rating of 'positive', 'negative' or 'neutral' based on these sentiments."

> "Finally, summarise the text in two sentences."

In other words, the following was asked for:
- **rating** (positive/negative/neutral)
- **keywords** (5)
- **key phrases** (3)
- **summary** (2 sentences)

There are a couple of major caveats here:

1. I used the least powerful version of GPT. The cost was around $6.69 for 3,733 requests (this included some trail and error requests at the outset). The next most powerful api (`GPT-4`) would have cost around 30x this.
2. I am not very familiar with the GPT model, especially questions around how best to formulate the prompt. There were probably better ways to formulate the requests. This exercise was primarily exploratory in nature, therefore a limited amount of time was spent engineering the prompt. 

## Rating

## Keywords

## Key Phrases

## Summaries
