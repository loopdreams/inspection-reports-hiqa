# HIQA Inspection Reports - Data Aggregation

## Context and Method

All of the data below was taken from the [HIQA Inspection Reports](https://www.hiqa.ie/reports-and-publications/inspection-reports) for Disability Centres, available on their website in `.pdf` format.

Report URLs are listed under pages for [centres](https://www.hiqa.ie/find-a-centre) where an inspection has occurred ([example](https://www.hiqa.ie/areas-we-work/find-a-centre/st-dominics-services)). Using the HIQA register for disability centres, I was able to get the URLs for the centre pages, and from these, the URLs for all inspection  reports.

Reports are written according to a **standard template**, making them relatively easy to parse. I extracted information from the following sections of the reports:

- 'Frontmatter' area on the first page, including details of the centre id, date of inspection, type of inspection, etc.
- Number of residents present at the time of inspection
- Levels of compliance for the regulations inspected ("Compliant", "Not compliant", "Substantially compliant")
- The narrative text from the section titled "What residents told us and what inspectors observed"

This is not an exhaustive list of information that could be extracted from the reports.

;; TODO Information on the 'success rate'

Below is some general information and insights gathered from the data contained in the reports, using information contained in the frontmatter and the 'number of residents' present field.

There are separate pages for :

- **Aggregate Compliance Levels** based on the levels of compliance listed in the reports
- **Sentiment Levels** based on the narrative 'observations' text sections of the reports, aggregated/analysed using OpanAI's `GPT-3.5`

## General Information

approximately
### Number of Inspections
- by year
- by month,

### Number of Centres

- total 
- per region
- number of inspections per centre 
- number of centres with reports vs. number registered (coverage)

### Number of Providers

- total 
- per region

## Number of Residents Present

### Average numbers of residents present

- frequencies/average
- vs. HIQA register capacity

### Congregated Settings
