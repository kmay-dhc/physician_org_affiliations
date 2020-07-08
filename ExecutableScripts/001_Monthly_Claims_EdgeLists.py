#!/usr/bin/env python
# coding: utf-8

# ### Compile Edge Lists from Medicare and commercial claims: 
# ##### Monthly claims by physician & facility from all available data sources. 
# ##### _____________________________________________________________________________________

# Import relevant libraries

import pandas as pd 
import numpy as np 

from google.cloud import bigquery 
import pyodbc

from datetime import datetime 


# Establish connections to BigQuery and SSMS


bq_conn = bigquery.Client() 
ssms_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=Database5;DATABASE=DefinitiveHC;Trusted_Connection=yes;')
p02_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DHCSQLP02;DATABASE=DS_WORK;Trusted_Connection=yes;')




# #### COMMERCIAL CLAIMS

# ##### Rendering-Billing


comm_ren_bil_sql = """
CREATE  OR REPLACE TABLE DS_WORK.RENDERING_BILLING_AFFILIATIONS
AS
SELECT  RENDERING_NPI
    ,   CLAIM_MONTH
    ,   A.BILLING_NPI
    ,   A.CLAIMS 
    ,   A.MIN_SERVICE_TO_DATE
    ,   A.MAX_SERVICE_TO_DATE 
FROM    (
        SELECT  RENDERING_NPI
            ,   CLAIM_YEAR 
            ,   SAFE_CAST(CONCAT(CAST(CLAIM_YEAR AS STRING), '-', CAST(EXTRACT(MONTH FROM SERVICE_TO_DATE) AS STRING), '-', '01') AS DATE) AS CLAIM_MONTH
            ,   BILLING_NPI
            ,   COUNT(*) AS CLAIMS
            ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
            ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
        FROM   (
                SELECT DISTINCT CLAIM_YEAR
                    ,  SAFE_CAST(CONCAT(SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 1, 4), '-', SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 5, 2), '-', SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 7, 2)) AS DATE) AS SERVICE_TO_DATE
                    ,  B.NPI AS RENDERING_NPI
                    ,  C.NPI AS BILLING_NPI
                    ,  A.DPID 
                FROM   CommercialHealthClaims.CLMS_EntityData A
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID B
                ON     A.RENDERING_NPIID = B.NPIID 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C
                ON     A.BILLING_NPIID = C.NPIID
                WHERE  RENDERING_NPIID IS NOT NULL 
                AND    BILLING_NPIID IS NOT NULL 
                AND    RENDERING_NPIID <> BILLING_NPIID 
                AND    CLAIM_YEAR >= 2016
                ) 
        GROUP   BY RENDERING_NPI
            ,   CLAIM_YEAR 
            ,   SAFE_CAST(CONCAT(CAST(CLAIM_YEAR AS STRING), '-', CAST(EXTRACT(MONTH FROM SERVICE_TO_DATE) AS STRING), '-', '01') AS DATE)
            ,   BILLING_NPI
        ) A 
"""

bq_conn.query(comm_ren_bil_sql).result()


# ##### Rendering-Facility



comm_ren_fac_sql = """
CREATE  OR REPLACE TABLE DS_WORK.RENDERING_SERVICE_FACILITY_AFFILIATIONS
AS
SELECT  RENDERING_NPI
    ,   CLAIM_MONTH
    ,   A.SERVICE_FACILITY_NPI
    ,   A.CLAIMS 
    ,   A.MIN_SERVICE_TO_DATE
    ,   A.MAX_SERVICE_TO_DATE 
FROM    (
        SELECT  RENDERING_NPI
            ,   CLAIM_YEAR 
            ,   SAFE_CAST(CONCAT(CAST(CLAIM_YEAR AS STRING), '-', CAST(EXTRACT(MONTH FROM SERVICE_TO_DATE) AS STRING), '-', '01') AS DATE) AS CLAIM_MONTH
            ,   SERVICE_FACILITY_NPI
            ,   COUNT(*) AS CLAIMS
            ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
            ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
        FROM   (
                SELECT DISTINCT CLAIM_YEAR
                    ,  SAFE_CAST(CONCAT(SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 1, 4), '-', SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 5, 2), '-', SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 7, 2)) AS DATE) AS SERVICE_TO_DATE
                    ,  B.NPI AS RENDERING_NPI
                    ,  C.NPI AS SERVICE_FACILITY_NPI
                    ,  A.DPID
                FROM   CommercialHealthClaims.CLMS_EntityData A 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID B
                ON     A.RENDERING_NPIID = B.NPIID 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C
                ON     A.SERVICE_FACILITY_NPIID = C.NPIID
                WHERE  RENDERING_NPIID IS NOT NULL 
                AND    SERVICE_FACILITY_NPIID IS NOT NULL 
                AND    SERVICE_FACILITY_NPIID <> IFNULL(BILLING_NPIID, 0) 
                AND    CLAIM_YEAR >= 2016
                ) 
        GROUP   BY RENDERING_NPI
            ,   CLAIM_YEAR 
            ,   SAFE_CAST(CONCAT(CAST(CLAIM_YEAR AS STRING), '-', CAST(EXTRACT(MONTH FROM SERVICE_TO_DATE) AS STRING), '-', '01') AS DATE)
            ,   SERVICE_FACILITY_NPI
        ) A 
""" 

bq_conn.query(comm_ren_fac_sql).result()




# #### MEDICARE CLAIMS

# Institutional (Inpatient, Outpatient, SNF, HHA, Hospice)



medicare_years_df = pd.DataFrame()
medicare_years_qtrly_df = pd.DataFrame()

for db in(list(pd.read_sql("""SELECT [name] AS DB FROM sys.databases WHERE [name] LIKE 'MEDICARE_SAF_20%' """, p02_conn)['DB'].values)):
    medicare_years_df = medicare_years_df.append(pd.read_sql("""SELECT {yr} AS CLAIM_YEAR FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME ='CLAIMS_FILE' AND {yr} > 2015 """.format(db=db, yr=db[-4:]), p02_conn))
    
for db in(list(pd.read_sql("""SELECT [name] AS DB FROM sys.databases WHERE [name] LIKE 'MEDICARE_SAF_20%' """, p02_conn)['DB'].values)):
    medicare_years_qtrly_df = medicare_years_qtrly_df.append(pd.read_sql("""SELECT {yr} AS CLAIM_YEAR FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME ='CLAIMS_FILE_QTRLY' AND {yr} > 2015 """.format(db=db, yr=db[-4:]), p02_conn))
    


medicare_years = list(medicare_years_df['CLAIM_YEAR'].values)
medicare_years_qtrly = set(medicare_years_qtrly_df['CLAIM_YEAR'].values) - set(medicare_years)
medicare_years_qtrly = list(medicare_years_qtrly)


#truncate tables in BQ 


bq_conn.query("""DELETE FROM DS_WORK.MCR_ATTENDING_ORGANIZATION_AFFILIATIONS WHERE 1=1""").result()  
bq_conn.query("""DELETE FROM DS_WORK.MCR_OPERATING_ORGANIZATION_AFFILIATIONS WHERE 1=1""").result()  
bq_conn.query("""DELETE FROM DS_WORK.MCR_RENDERING_ORGANIZATION_AFFILIATIONS WHERE 1=1""").result() 


# ##### Attending-Organization


# populate table in BQ 


for yr in medicare_years: 
    mcr_sql = """
    SELECT  AT_PHYSN_NPI
        ,   CLAIM_MONTH
        ,   A.ORG_NPI_NUM
        ,   A.CLAIMS 
        ,   A.MIN_SERVICE_TO_DATE
        ,   A.MAX_SERVICE_TO_DATE 
    FROM    (
            SELECT  AT_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) AS CLAIM_MONTH 
                ,   ORG_NPI_NUM
                ,   COUNT(*) AS CLAIMS
                ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
                ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
            FROM   (
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    ) A
            GROUP   BY AT_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) 
                ,   ORG_NPI_NUM
            ) A 
    WHERE   AT_PHYSN_NPI <> 0 
    AND     ORG_NPI_NUM <> 0 
    """.format(yr=yr)
    
    print(datetime.now())
    print('Attending-organization data for {yr}'.format(yr=yr))
    df = pd.read_sql(mcr_sql, p02_conn)
    
    # push to BQ
    dataframe = df
    tablename = 'stable-healer-231019.DS_WORK.MCR_ATTENDING_ORGANIZATION_AFFILIATIONS'

    job = bq_conn.load_table_from_dataframe(dataframe, destination = tablename)
    job.result()
    print(datetime.now())
    

for yr in medicare_years_qtrly: 
    
    mcr_sql = """
    SELECT  AT_PHYSN_NPI
        ,   CLAIM_MONTH
        ,   A.ORG_NPI_NUM
        ,   A.CLAIMS 
        ,   A.MIN_SERVICE_TO_DATE
        ,   A.MAX_SERVICE_TO_DATE 
    FROM    (
            SELECT  AT_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) AS CLAIM_MONTH 
                ,   ORG_NPI_NUM
                ,   COUNT(*) AS CLAIMS
                ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
                ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
            FROM   (
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(AT_PHYSN_NPI  AS INT) AS AT_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(AT_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    AT_PHYSN_NPI <> ORG_NPI_NUM 
                    ) A
            GROUP   BY AT_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) 
                ,   ORG_NPI_NUM
            ) A 
    WHERE   AT_PHYSN_NPI <> 0 
    AND     ORG_NPI_NUM <> 0 
    """.format(yr=yr)
    
    
    print(datetime.now())
    print('Attending-organization data for {yr}'.format(yr=yr))
    df = pd.read_sql(mcr_sql, p02_conn)
    
    # push to BQ
    dataframe = df
    tablename = 'stable-healer-231019.DS_WORK.MCR_ATTENDING_ORGANIZATION_AFFILIATIONS'

    job = bq_conn.load_table_from_dataframe(dataframe, destination = tablename)
    job.result()
    print(datetime.now())



# ##### Operating-Organization



# populate table in BQ 



for yr in medicare_years: 

    mcr_sql2 = """SELECT  OP_PHYSN_NPI
        ,   CLAIM_MONTH
        ,   A.ORG_NPI_NUM
        ,   A.CLAIMS 
        ,   A.MIN_SERVICE_TO_DATE
        ,   A.MAX_SERVICE_TO_DATE 
    FROM    (
            SELECT  OP_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) AS CLAIM_MONTH 
                ,   ORG_NPI_NUM
                ,   COUNT(*) AS CLAIMS
                ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
                ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
            FROM   (
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    ) A
            GROUP   BY OP_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) 
                ,   ORG_NPI_NUM
            ) A """.format(yr=yr)
    
    print(datetime.now())
    print('Operating-org data for {yr}'.format(yr=yr))
    df = pd.read_sql(mcr_sql2, p02_conn)
    
    #push to BQ
    dataframe = df
    tablename = 'stable-healer-231019.DS_WORK.MCR_OPERATING_ORGANIZATION_AFFILIATIONS'

    job = bq_conn.load_table_from_dataframe(dataframe, destination = tablename)
    job.result()
    print(datetime.now())



for yr in medicare_years_qtrly: 
    
    mcr_sql2 = """SELECT  OP_PHYSN_NPI
        ,   CLAIM_MONTH
        ,   A.ORG_NPI_NUM
        ,   A.CLAIMS 
        ,   A.MIN_SERVICE_TO_DATE
        ,   A.MAX_SERVICE_TO_DATE 
    FROM    (
            SELECT  OP_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) AS CLAIM_MONTH 
                ,   ORG_NPI_NUM
                ,   COUNT(*) AS CLAIMS
                ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
                ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
            FROM   (
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(OP_PHYSN_NPI  AS INT) AS OP_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(OP_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    OP_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    OP_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    ) A
            GROUP   BY OP_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) 
                ,   ORG_NPI_NUM
            ) A """.format(yr=yr)
    
    print(datetime.now())
    print('Operating-org data for {yr}'.format(yr=yr))
    df = pd.read_sql(mcr_sql2, p02_conn)
    
    #push to BQ
    dataframe = df
    tablename = 'stable-healer-231019.DS_WORK.MCR_OPERATING_ORGANIZATION_AFFILIATIONS'

    job = bq_conn.load_table_from_dataframe(dataframe, destination = tablename)
    job.result()
    print(datetime.now())



# ##### Rendering-Organization


# populate table in BQ


for yr in medicare_years: 
    
    mcr_sql3 = """SELECT  RNDRNG_PHYSN_NPI AS RENDERING_NPI
        ,   CLAIM_MONTH
        ,   A.ORG_NPI_NUM
        ,   A.CLAIMS 
        ,   A.MIN_SERVICE_TO_DATE
        ,   A.MAX_SERVICE_TO_DATE 
    FROM    (
            SELECT  RNDRNG_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) AS CLAIM_MONTH 
                ,   ORG_NPI_NUM
                ,   COUNT(*) AS CLAIMS
                ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
                ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
            FROM   (
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    ) A
            GROUP   BY RNDRNG_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) 
                ,   ORG_NPI_NUM
            ) A  """.format(yr=yr)
    
    print(datetime.now())
    print('Rendering-org data for {yr}'.format(yr=yr))
    df = pd.read_sql(mcr_sql3, p02_conn)
    
    #push to BQ
    dataframe = df
    tablename = 'stable-healer-231019.DS_WORK.MCR_RENDERING_ORGANIZATION_AFFILIATIONS'

    job = bq_conn.load_table_from_dataframe(dataframe, destination = tablename)
    job.result()
    print(datetime.now())



for yr in medicare_years_qtrly: 
    
    mcr_sql3 = """SELECT  RNDRNG_PHYSN_NPI AS RENDERING_NPI
        ,   CLAIM_MONTH
        ,   A.ORG_NPI_NUM
        ,   A.CLAIMS 
        ,   A.MIN_SERVICE_TO_DATE
        ,   A.MAX_SERVICE_TO_DATE 
    FROM    (
            SELECT  RNDRNG_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) AS CLAIM_MONTH 
                ,   ORG_NPI_NUM
                ,   COUNT(*) AS CLAIMS
                ,   MIN(SERVICE_TO_DATE) AS MIN_SERVICE_TO_DATE
                ,   MAX(SERVICE_TO_DATE) AS MAX_SERVICE_TO_DATE
            FROM   (
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    UNION ALL 
                    SELECT DISTINCT {yr} AS CLAIM_YEAR
                        ,  CAST(CLM_THRU_DT AS DATE) AS SERVICE_TO_DATE
                        ,  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) AS RNDRNG_PHYSN_NPI
                        ,  TRY_CAST(ORG_NPI_NUM AS INT) AS ORG_NPI_NUM
                        ,  DESY_SORT_KEY
                    FROM   MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}_q
                    WHERE  TRY_CAST(RNDRNG_PHYSN_NPI  AS INT) IS NOT NULL 
                    AND    TRY_CAST(ORG_NPI_NUM AS INT) IS NOT NULL 
                    AND    RNDRNG_PHYSN_NPI <> ORG_NPI_NUM 
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(AT_PHYSN_NPI,0)
                    AND    RNDRNG_PHYSN_NPI <> ISNULL(OP_PHYSN_NPI,0)
                    ) A
            GROUP   BY RNDRNG_PHYSN_NPI
                ,   CLAIM_YEAR 
                ,   CAST(CAST(YEAR(SERVICE_TO_DATE) AS VARCHAR) + '-' + CAST(MONTH(SERVICE_TO_DATE) AS VARCHAR) + '-' + '01' AS DATE) 
                ,   ORG_NPI_NUM
            ) A  """.format(yr=yr)
    
    print(datetime.now())
    print('Rendering-org data for {yr}'.format(yr=yr))
    df = pd.read_sql(mcr_sql3, p02_conn)
    
    #push to BQ
    dataframe = df
    tablename = 'stable-healer-231019.DS_WORK.MCR_RENDERING_ORGANIZATION_AFFILIATIONS'

    job = bq_conn.load_table_from_dataframe(dataframe, destination = tablename)
    job.result()
    print(datetime.now())




