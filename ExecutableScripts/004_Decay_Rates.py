#!/usr/bin/env python
# coding: utf-8

# ## Decay Rate

# The rate will be dependent on: 
# - Commercial Claims 
# - Medicare Claims
# - Average lag between claims (# of months) 
# - Max lag between claims (# of months) 
# 
# A relationship will remain active (decay rate of 0) with 1 or more claims in a month 
# 
# Decay starts when the # of months since the last claim exceeds the average
# - Exponential rate of decay starting at the historical average, through the historical max
#  - If the lag period exceeds the historical max, it will be flagged as no longer current 
# 
# Analysis dates for commercial and Medicare claims to be systematically determined based on data availability


import math
import numpy as np
from google.cloud import bigquery

from datetime import datetime
import pyodbc
import pandas as pd


# #### All relationship decay rates


bq_conn = bigquery.Client()
p02_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DHCSQLP02;DATABASE=DS_WORK;Trusted_Connection=yes;')


print(datetime.now(), ' Calculating decay')

#get the commercial analysis date based on the latest available data 

dt_comm = bq_conn.query("""SELECT DATE_ADD(DATE_ADD(MAX(CLAIM_DATE), INTERVAL -3 MONTH), INTERVAL 1 DAY) AS MAX_COMM_DATE 
                 FROM CommercialHealthClaims.CLMS_COMMERCIAL_MAX_DATE 
                 WHERE ANALYSIS_TYPE_ID = 1""").result().to_dataframe().loc[0].values[0].strftime('%Y-%m-%d')



medicare_years = list()

for db in(list(pd.read_sql("""SELECT [name] AS DB FROM sys.databases WHERE [name] LIKE 'MEDICARE_SAF_20%' """, p02_conn)['DB'].values)):
    if db[-4:] > '2015':
        medicare_years.append(db[-4:])

dt_mcr = pd.read_sql("""SELECT DATEADD(DAY, 1, MAX(CLM_THRU_DT)) AS MAX_DATE_MCR FROM MEDICARE_SAF_{}.dbo.CLAIMS_FILE_QTRLY""".format(max(medicare_years)), p02_conn)['MAX_DATE_MCR'].values[0].strftime('%Y-%m-%d')




# calc decay 
sql_calc_decay = """
CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_ORG_AFFILIATION_DECAY 
AS 
SELECT  PHYSICIAN_NPI
      , DEFHC_ID 
      , CURRENT_MONTH_LAG
      , AVG_MONTH_LAG
      , MAX_MONTH_LAG 
      , DECAY_RATE 
      , CASE WHEN CURRENT_MONTH_LAG <= IFNULL(AVG_MONTH_LAG, 1) THEN 1 
          ELSE POWER((1+DECAY_RATE), CURRENT_MONTH_LAG)
        END AS CURRENT_DECAY_REMAINING_VAL
      , CURRENT_DATE() AS UPDATE_DATE 
FROM    (
        SELECT *
          , (POWER(0.1, (1/(IFNULL(MAX_MONTH_LAG,1)+1))))- 1 AS DECAY_RATE
          , CASE WHEN IFNULL(MAX_DATE_COMM,'1900-01-01') = MAX_DATE THEN DATE_DIFF('{dt_comm}', MAX_DATE, MONTH) 
                ELSE DATE_DIFF('{dt_mcr}', MAX_DATE, MONTH)
            END AS CURRENT_MONTH_LAG 
        FROM DS_WORK.PHYSICIAN_ORG_AFFILIATIONS 
        ) 
""".format(dt_comm=dt_comm, dt_mcr=dt_mcr)

bq_conn.query(sql_calc_decay).result()

print(datetime.now(), ' Complete.')