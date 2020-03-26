# pa-covid-dataviz

Script that pulls data from several sources and outputs a spreadsheet (this will go into a Google spreadsheet). The script
will be run every night (around 4 AM CET?) in Jenkins. It creates less than 15 temporary files and runs for 5 minutes.

As much as possible, the data should be pulled from HDX sources.

* WB* WB indicators of interest: https://data.humdata.org/dataset/world-bank-indicators-of-interest-to-the-covid-19-outbreak
* INFORM* Inform risk index: https://data.humdata.org/dataset/country-risk-profiles-for-191-countries

Susceptibility to contagion:
1. Age and Population - Population ages 65 and above (% total) *WB*
2. Water & Sanitation - People with basic handwashing facilities (% total population) *WB*
3. Estimated number of people living with HIV - Adult (>15) rate *INFORM*
4. Total Uprooted people (percentage of the total population) *INFORM*

Capacity of country to cope:
1. Health - Hospital beds (per 1,000 people) *WB*
2. Health - Physicians (per 1,000 people) *WB*
3. Health - Nurses and midwives (per 1,000 people) *WB*
4. Health - Out-of-pocket expenditure per capita *WB* - we should compare this to a baseline (global average?)

The columns for this output spreadsheet should be as follows:
country_code, age_pop, water_san, hiv, uprooted, beds, physicians, nurses, health_expenditure
