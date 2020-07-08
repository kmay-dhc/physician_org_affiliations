#!/usr/bin/env python
# coding: utf-8

# #### Calculate Scores


import pandas as pd 
from google.cloud import bigquery


print(datetime.now(), ' Calculating scores')


bq_conn = bigquery.Client()




sql_calc_score = """
-- score = log pct months + log months + log claims - log avg month lag + log comm claims + log mcr claims  
-- get the min and max score values, and divide by 10.
-- create buckets based on that cutoff. Every affiliation then gets a score between 1 and 10. 

CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_ORG_AFFILIATION_SCORES
AS 
SELECT *
    , LOG_PCT_MONTHS_MCR + LOG_PCT_MONTHS_COMM + LOG_MONTHS + LOG_CLAIMS - LOG_AVG_MONTH_LAG AS SCORE 
    , NULL AS SCORE_BUCKET
FROM (
      SELECT PHYSICIAN_NPI
      , DEFHC_ID
      , CASE WHEN MAX_DATE_MCR IS NULL THEN 0
             WHEN DATE_DIFF(MAX_DATE_MCR, MIN_DATE_MCR, MONTH)+1 = 1 THEN 0 
            ELSE LOG((MONTHS_MCR*1.0/(DATE_DIFF(MAX_DATE_MCR, MIN_DATE_MCR, MONTH)+1))*100, 10) END AS LOG_PCT_MONTHS_MCR
      , CASE WHEN MAX_DATE_COMM IS NULL THEN 0
             WHEN DATE_DIFF(MAX_DATE_COMM, MIN_DATE_COMM, MONTH)+1 = 1 THEN 0 
            ELSE LOG((MONTHS_COMM*1.0/(DATE_DIFF(MAX_DATE_COMM, MIN_DATE_COMM, MONTH)+1))*100, 10) END AS LOG_PCT_MONTHS_COMM
      , LOG(MONTHS, 10) AS LOG_MONTHS
      , LOG(COMM_CLAIMS+MCR_CLAIMS, 10) AS LOG_CLAIMS
      , CASE WHEN AVG_MONTH_LAG IS NULL THEN 0 ELSE LOG(AVG_MONTH_LAG, 10) END AS LOG_AVG_MONTH_LAG 
      FROM DS_WORK.PHYSICIAN_ORG_AFFILIATIONS 
    ) """

bq_conn.query(sql_calc_score).result()



sql_update_score_bucket = """
UPDATE DS_WORK.PHYSICIAN_ORG_AFFILIATION_SCORES 
SET SCORE_BUCKET = CAST(CEILING((SCORE-(SELECT MIN(SCORE) 
                                        FROM DS_WORK.PHYSICIAN_ORG_AFFILIATION_SCORES)) 
                                 / 
                                 (SELECT (MAX(SCORE) - MIN(SCORE)) / 10 AS BUCKET_SIZE 
                                  FROM DS_WORK.PHYSICIAN_ORG_AFFILIATION_SCORES) 
                                ) AS INT64)
WHERE 1=1"""

bq_conn.query(sql_update_score_bucket).result()

bq_conn.query("""UPDATE DS_WORK.PHYSICIAN_ORG_AFFILIATION_SCORES SET SCORE_BUCKET = 1 WHERE SCORE_BUCKET < 1""").result()


print(datetime.now(), ' Complete.')


