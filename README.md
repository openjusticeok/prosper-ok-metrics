# Metrics

## November 2025
Aggregations: Total and by gender
### Jail
- Jail Bookings
- Jail Releases
- Tulsa County comparison to Oklahoma County
- Average Daily Population

### Prison
- Prison Sentences
- Prison Releases
- Population by Gender
- Oklahoma County comparison to Tulsa County vs all other counties


# Older Metrics / Details

These can be found in `/inst/older-metrics`

### Drivers' license restoration (and other DL-related reforms)
  * **Bill Description:**
    * [HB-1795](http://webserver1.lsb.state.ok.us/cf_pdf/2021-22%20int/hb/HB1795%20int.pdf) reduces the number of offenses for which a license can be automatically revoked, allowing fewer Oklahomans to lose their license for non-driving infractions. HB 1795 also increases access to provisionial licenses, reduces the monthly fee for those licenses and regulates against DPS suspension only for failure to pay.
  * **Metric Description:**
    * Open Justice Oklahoma performed a data analysis of the misdemenor drug offenses that required revocation prior to the passage of HB 1795. They then tracked the number of those cases since November 2021, the effective date for HB 1795. Because of a data descrepency in the court minutes they were unable to get a precise number of the reduction of failure to pay and failure to appear related revocations, but they're working to gather that data from DPS. They conservatively estimated a reduction of around 5,000 FTP revocations since November 1st based on 2018 numbers. FTA numbers vary widely thoughout the year. As such the OJO recomended waiting to get data from DPS before attempting to add those numbers. It's likely that this estimate could double once the analysts have access to the complete dat picture.
  * **Notes:**
    * The initial "data anlysis of the misdemeanor drug offenses that required revocation prior to the passage of HB 1795" was what Damion asked me to do back when he was still at OKPI. The original version of that is in the `andrew-misc` repo, under `/dl-revocation/revocation.R`, and the new verison is in this repo under `R/find-dl-revocation-cases.R`.
    * It's basically just searching statewide for all CM cases with ONLY drug charges in the given time period. The effective date of the reform was November 2021, so the time period is kind of pre- / post-that and also tracking the changes quarterly going forward. Right now, we're looking for Q1 of FY2023, so that's July / August / September of 2022. Gonna have to get that scraper issue fixed so we can include all September cases.
    * The "data discrepancy" mentioned is just referencing my initial attempt at using the court minutes to find revocations sent to DPS. The data is just really irregular and I ultimately decided it wasn't worth trying to wrangle it, but we are working on getting a data request to DPS.

---

### Drug Court / SQ780 Implementation
 * **Notes:**
   * Trying to find evidence on SQ780's effect on drug court participation
   * We were going to search through the minute data and attempt to see:
     * How many CF / CM cases had a `DCPF` drug court program fee minute in Oklahoma county pre- and post-780? What is that as a % of total CF / CM cases?
     * How many CF / CM cases had a minute indicating that they were removed from drug court? E.g. "MOTION TO REVOKE FROM DRUG COURT", "BENCH WARRANT - DRUG COURT VIOLATION", etc. Get total numbers and numbers as a % of all cases pre- and post-780.
