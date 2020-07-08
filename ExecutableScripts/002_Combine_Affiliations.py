#!/usr/bin/env python
# coding: utf-8

# #### Combine all commercial and Medicare affiliation lists into a table summarized by affiliation.

from google.cloud import bigquery
import pyodbc
import pandas as pd
from datetime import datetime


bq_conn = bigquery.Client()
ssms_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DHCSQLP02;DATABASE=DS_WORK;Trusted_Connection=yes;')


combined_sql = """
CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_ORG_AFFILIATIONS
AS
SELECT  NPI AS PHYSICIAN_NPI
    ,   DEFHC_ID
    ,   AC.HOSPITAL_NAME
    ,   AC.FIRM_TYPE 
    ,   MIN(MIN_DATE) AS MIN_DATE
    ,   MAX(MAX_DATE) AS MAX_DATE
    ,   MIN(MIN_DATE_COMM) AS MIN_DATE_COMM
    ,   MAX(MAX_DATE_COMM) AS MAX_DATE_COMM
    ,   MIN(MIN_DATE_MCR) AS MIN_DATE_MCR
    ,   MAX(MAX_DATE_MCR) AS MAX_DATE_MCR
    ,   AVG(MONTHS_BETWEEN) AS AVG_MONTH_LAG 
    ,   MIN(MONTHS_BETWEEN) AS MIN_MONTH_LAG
    ,   MAX(MONTHS_BETWEEN) AS MAX_MONTH_LAG 
    ,   COUNT(DISTINCT CLAIM_MONTH) AS MONTHS
    ,   COUNT(DISTINCT CASE WHEN IFNULL(MEDICARE_CLAIMS,0)>0 THEN CLAIM_MONTH END) AS MONTHS_MCR
    ,   COUNT(DISTINCT CASE WHEN IFNULL(COMMERCIAL_CLAIMS,0)>0 THEN CLAIM_MONTH END) AS MONTHS_COMM
    ,   DATE_DIFF(MAX(CLAIM_MONTH), MIN(CLAIM_MONTH), MONTH)+1 AS MONTHS_DENOM
    ,   SUM(COMMERCIAL_CLAIMS) AS COMM_CLAIMS
    ,   SUM(MEDICARE_CLAIMS) AS MCR_CLAIMS
FROM    (
        SELECT  NPI 
            ,   DEFHC_ID
            ,   CLAIM_MONTH
            ,   MIN(MIN_SERVICE_TO_DATE) AS MIN_DATE
            ,   MAX(MAX_SERVICE_TO_DATE) AS MAX_DATE
            ,   MIN(MIN_DATE_COMM) AS MIN_DATE_COMM
            ,   MAX(MAX_DATE_COMM) AS MAX_DATE_COMM
            ,   MIN(MIN_DATE_MCR) AS MIN_DATE_MCR
            ,   MAX(MAX_DATE_MCR) AS MAX_DATE_MCR
            ,   SUM(COMMERCIAL_CLAIMS) AS COMMERCIAL_CLAIMS
            ,   SUM(MEDICARE_CLAIMS) AS MEDICARE_CLAIMS
            ,   LAG(CLAIM_MONTH) OVER(PARTITION BY NPI, DEFHC_ID ORDER BY CLAIM_MONTH) LAST_MTH
            ,   DATE_DIFF(CLAIM_MONTH, LAG(CLAIM_MONTH) OVER(PARTITION BY NPI, DEFHC_ID ORDER BY CLAIM_MONTH), MONTH) AS MONTHS_BETWEEN
        FROM    (
                SELECT RENDERING_NPI AS NPI 
                    ,  C.DEFHC_ID
                    ,  CLAIM_MONTH
                    ,  MIN_SERVICE_TO_DATE
                    ,  MAX_SERVICE_TO_DATE
                    ,  CLAIMS AS COMMERCIAL_CLAIMS 
                    ,  0 AS MEDICARE_CLAIMS
                    ,  MIN_SERVICE_TO_DATE AS MIN_DATE_COMM
                    ,  NULL AS MIN_DATE_MCR
                    ,  MAX_SERVICE_TO_DATE AS MAX_DATE_COMM
                    ,  NULL AS MAX_DATE_MCR 
                FROM   DS_WORK.RENDERING_BILLING_AFFILIATIONS A 
                JOIN   CommercialHealthClaims.PhysicianLookup B
                ON     A.RENDERING_NPI = B.NPI 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C 
                ON     A.BILLING_NPI = C.NPI 
                UNION ALL 
                SELECT RENDERING_NPI AS NPI 
                    ,  C.DEFHC_ID
                    ,  CLAIM_MONTH
                    ,  MIN_SERVICE_TO_DATE
                    ,  MAX_SERVICE_TO_DATE
                    ,  CLAIMS AS COMMERCIAL_CLAIMS
                    ,  0 AS MEDICARE_CLAIMS
                    ,  MIN_SERVICE_TO_DATE AS MIN_DATE_COMM
                    ,  NULL AS MIN_DATE_MCR
                    ,  MAX_SERVICE_TO_DATE AS MAX_DATE_COMM
                    ,  NULL AS MAX_DATE_MCR 
                FROM   DS_WORK.RENDERING_SERVICE_FACILITY_AFFILIATIONS A 
                JOIN   CommercialHealthClaims.PhysicianLookup B
                ON     A.RENDERING_NPI = B.NPI 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C 
                ON     A.SERVICE_FACILITY_NPI = C.NPI 
                UNION ALL 
                SELECT A.AT_PHYSN_NPI AS NPI 
                    ,  C.DEFHC_ID  
                    ,  CLAIM_MONTH
                    ,  MIN_SERVICE_TO_DATE
                    ,  MAX_SERVICE_TO_DATE
                    ,  0 AS COMMERCIAL_CLAIMS
                    ,  CLAIMS AS MEDICARE_CLAIMS
                    ,  NULL AS MIN_DATE_COMM
                    ,  MIN_SERVICE_TO_DATE AS MIN_DATE_MCR
                    ,  NULL AS MAX_DATE_COMM
                    ,  MAX_SERVICE_TO_DATE AS MAX_DATE_MCR 
                FROM   DS_WORK.MCR_ATTENDING_ORGANIZATION_AFFILIATIONS A 
                JOIN   CommercialHealthClaims.PhysicianLookup B
                ON     A.AT_PHYSN_NPI = B.NPI 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C 
                ON     A.ORG_NPI_NUM = C.NPI
                UNION ALL 
                SELECT A.OP_PHYSN_NPI AS NPI
                    ,  C.DEFHC_ID 
                    ,  CLAIM_MONTH
                    ,  MIN_SERVICE_TO_DATE
                    ,  MAX_SERVICE_TO_DATE
                    ,  0 AS COMMERCIAL_CLAIMS 
                    ,  CLAIMS AS MEDICARE_CLAIMS
                    ,  NULL AS MIN_DATE_COMM
                    ,  MIN_SERVICE_TO_DATE AS MIN_DATE_MCR
                    ,  NULL AS MAX_DATE_COMM
                    ,  MAX_SERVICE_TO_DATE AS MAX_DATE_MCR 
                FROM   DS_WORK.MCR_OPERATING_ORGANIZATION_AFFILIATIONS  A 
                JOIN   CommercialHealthClaims.PhysicianLookup B
                ON     A.OP_PHYSN_NPI = B.NPI 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C 
                ON     A.ORG_NPI_NUM = C.NPI
                UNION ALL 
                SELECT A.RENDERING_NPI AS NPI
                    ,  C.DEFHC_ID 
                    ,  CLAIM_MONTH
                    ,  MIN_SERVICE_TO_DATE
                    ,  MAX_SERVICE_TO_DATE
                    ,  0 AS COMMERCIAL_CLAIMS 
                    ,  CLAIMS AS MEDICARE_CLAIMS
                    ,  NULL AS MIN_DATE_COMM
                    ,  MIN_SERVICE_TO_DATE AS MIN_DATE_MCR
                    ,  NULL AS MAX_DATE_COMM
                    ,  MAX_SERVICE_TO_DATE AS MAX_DATE_MCR 
                FROM   DS_WORK.MCR_RENDERING_ORGANIZATION_AFFILIATIONS  A 
                JOIN   CommercialHealthClaims.PhysicianLookup B
                ON     A.RENDERING_NPI = B.NPI 
                JOIN   CommercialHealthClaims.LOOKUP_DEFHC_ID C 
                ON     A.ORG_NPI_NUM = C.NPI
                ) 
        WHERE   CLAIM_MONTH >= '2016-01-01'
        AND     CLAIM_MONTH <= (SELECT MAX(CLAIM_DATE) FROM CommercialHealthClaims.CLMS_COMMERCIAL_MAX_DATE)
        GROUP   BY NPI 
            ,   DEFHC_ID
            ,   CLAIM_MONTH
        ) A
JOIN   PS_WORK.ALL_COMPANIES AC
ON     A.DEFHC_ID = AC.HOSPITAL_ID
GROUP  BY NPI
    ,   DEFHC_ID
    ,   AC.HOSPITAL_NAME
    ,   AC.FIRM_TYPE 
"""

print(datetime.now(), ' Step 1: Replacing combined table DS_WORK.PHYSICIAN_ORG_AFFILIATIONS')
bq_conn.query(combined_sql).result()
print(datetime.now(), ' Step 1 Complete') 



# Calculate total claims by physician by month to use as denominator when calculating percentages. 


# PHYSICIAN TOTALS - COMMERCIAL 

sql_phys_total_comm = """
CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_CLAIMS_COMMERCIAL
AS
SELECT RENDERING_NPI
    ,  CLAIM_MONTH
    ,  COUNT(*) AS PHYSICIAN_TOTAL_CLAIMS_COMM
FROM (
        SELECT DISTINCT B.NPI AS RENDERING_NPI
            , SERVICE_TO_DATE
            , DPID 
            ,  SAFE_CAST(CONCAT(CAST(CLAIM_YEAR AS STRING), '-', SUBSTR(SAFE_CAST(SERVICE_TO_DATE AS STRING), 5, 2), '-', '01') AS DATE) AS CLAIM_MONTH
        FROM CommercialHealthClaims.CLMS_EntityData A 
        JOIN CommercialHealthClaims.LOOKUP_DEFHC_ID B 
        ON A.RENDERING_NPIID = B.NPIID 
        WHERE CLAIM_YEAR >= 2016
        AND COALESCE(A.BILLING_NPIID, A.SERVICE_FACILITY_NPIID) IS NOT NULL 
        AND COALESCE(A.BILLING_NPIID, A.SERVICE_FACILITY_NPIID) <> A.RENDERING_NPIID 
    ) 
GROUP BY RENDERING_NPI
    , CLAIM_MONTH
"""


print(datetime.now(), ' Step 2: Calculating physician total claims - commercial')
bq_conn.query(sql_phys_total_comm).result()
print(datetime.now(), ' Step 2 Complete') 


# PHYSICIAN TOTALS - MEDICARE 

sql_phys_total_mcr = """
CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_CLAIMS_MEDICARE
AS
SELECT  PHYSICIAN_NPI
    ,   CLAIM_MONTH
    ,   SUM(CLAIMS) AS PHYSICIAN_TOTAL_CLAIMS_MCR
FROM    (
        SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, CLAIM_MONTH, SUM(CLAIMS) AS CLAIMS FROM DS_WORK.MCR_ATTENDING_ORGANIZATION_AFFILIATIONS GROUP BY AT_PHYSN_NPI, CLAIM_MONTH
        UNION ALL
        SELECT OP_PHYSN_NPI, CLAIM_MONTH, SUM(CLAIMS) FROM DS_WORK.MCR_OPERATING_ORGANIZATION_AFFILIATIONS GROUP BY OP_PHYSN_NPI, CLAIM_MONTH
        UNION ALL
        SELECT RENDERING_NPI, CLAIM_MONTH, SUM(CLAIMS) FROM DS_WORK.MCR_RENDERING_ORGANIZATION_AFFILIATIONS GROUP BY RENDERING_NPI, CLAIM_MONTH
        ) 
GROUP   BY PHYSICIAN_NPI
    ,   CLAIM_MONTH 
"""


print(datetime.now(), ' Step 3: Calculating physician total claims - Medicare')
bq_conn.query(sql_phys_total_mcr).result()
print(datetime.now(), ' Step 3 Complete') 




# Calculate percentages of physicians' practice over the active claims period for each


sql_phys_aff_practice_pct = """
CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_ORG_AFFILIATIONS_CLAIMS_PCT
AS 
SELECT PHYSICIAN_NPI
    ,  DEFHC_ID
    ,  IFNULL(PCT_CLAIMS, 0) AS PCT_CLAIMS
    ,  IFNULL(PCT_CLAIMS_MCR, 0) AS PCT_CLAIMS_MCR
    ,  IFNULL(PCT_CLAIMS_COMM, 0) AS PCT_CLAIMS_COMM
FROM   (
        SELECT A.PHYSICIAN_NPI
            ,  A.DEFHC_ID
            ,  A.HOSPITAL_NAME
            ,  A.FIRM_TYPE
            ,  A.MIN_DATE
            ,  A.MAX_DATE
            ,  A.MIN_DATE_COMM
            ,  A.MAX_DATE_COMM
            ,  A.MIN_DATE_MCR
            ,  A.MAX_DATE_MCR 
            ,  COMM_CLAIMS
            ,  SUM(B.PHYSICIAN_TOTAL_CLAIMS_COMM) AS PHYS_CLAIMS_COMM
            ,  COMM_CLAIMS*1.0 / NULLIF(SUM(B.PHYSICIAN_TOTAL_CLAIMS_COMM),0) AS PCT_CLAIMS_COMM
            ,  MCR_CLAIMS
            ,  SUM(C.PHYSICIAN_TOTAL_CLAIMS_MCR) AS PHYS_CLAIMS_MCR
            ,  MCR_CLAIMS*1.0 / NULLIF(SUM(C.PHYSICIAN_TOTAL_CLAIMS_MCR),0) AS PCT_CLAIMS_MCR
            ,  COMM_CLAIMS+MCR_CLAIMS AS CLAIMS 
            ,  (COMM_CLAIMS+MCR_CLAIMS)*1.0 / NULLIF(SUM(IFNULL(C.PHYSICIAN_TOTAL_CLAIMS_MCR,0)+IFNULL(B.PHYSICIAN_TOTAL_CLAIMS_COMM,0)),0) AS PCT_CLAIMS
        FROM   DS_WORK.PHYSICIAN_ORG_AFFILIATIONS A 
        JOIN   (
                SELECT RENDERING_NPI AS PHYSICIAN_NPI, CLAIM_MONTH FROM DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_CLAIMS_COMMERCIAL
                UNION DISTINCT 
                SELECT PHYSICIAN_NPI, CLAIM_MONTH FROM DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_CLAIMS_MEDICARE
               ) X 
        ON     A.PHYSICIAN_NPI = X.PHYSICIAN_NPI 
        LEFT   JOIN DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_CLAIMS_COMMERCIAL B 
        ON     A.PHYSICIAN_NPI = B.RENDERING_NPI 
        AND    X.CLAIM_MONTH = B.CLAIM_MONTH
        AND    B.CLAIM_MONTH BETWEEN CAST(CONCAT(EXTRACT(YEAR FROM A.MIN_DATE_COMM), '-', EXTRACT(MONTH FROM A.MIN_DATE_COMM), '-01') AS DATE) AND CAST(CONCAT(EXTRACT(YEAR FROM A.MAX_DATE_COMM), '-', EXTRACT(MONTH FROM A.MAX_DATE_COMM), '-01') AS DATE)
        LEFT   JOIN DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_CLAIMS_MEDICARE C 
        ON     A.PHYSICIAN_NPI = C.PHYSICIAN_NPI 
        AND    X.CLAIM_MONTH = C.CLAIM_MONTH 
        AND    C.CLAIM_MONTH BETWEEN CAST(CONCAT(EXTRACT(YEAR FROM A.MIN_DATE_MCR), '-', EXTRACT(MONTH FROM A.MIN_DATE_MCR), '-01') AS DATE) AND CAST(CONCAT(EXTRACT(YEAR FROM A.MAX_DATE_MCR), '-', EXTRACT(MONTH FROM A.MAX_DATE_MCR), '-01') AS DATE)
        GROUP  BY A.PHYSICIAN_NPI
            , A.DEFHC_ID
            , A.HOSPITAL_NAME
            , A.FIRM_TYPE
            , A.MIN_DATE
            , A.MAX_DATE
            ,  A.MIN_DATE_COMM
            ,  A.MAX_DATE_COMM
            ,  A.MIN_DATE_MCR
            ,  A.MAX_DATE_MCR 
            , COMM_CLAIMS
            , MCR_CLAIMS
        ) 
"""


print(datetime.now(), ' Step 4: Calculating claims pct')
print(bq_conn.query(sql_phys_aff_practice_pct).result())
print(datetime.now(), ' Step 4 complete.')






# Calculate total unique patients for each relationship 



# COMMERCIAL 

sql_patients_comm = """
CREATE OR REPLACE TABLE DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_PATIENTS_COMMERCIAL
AS
SELECT PHYSICIAN_NPI
    ,  DEFHC_ID
    ,  COUNT(*) AS PHYSICIAN_TOTAL_PATIENTS_COMM
FROM (
        SELECT B.NPI AS PHYSICIAN_NPI
            , C.DEFHC_ID
            , DPID 
        FROM CommercialHealthClaims.CLMS_EntityData A 
        JOIN CommercialHealthClaims.LOOKUP_DEFHC_ID B 
        ON A.RENDERING_NPIID = B.NPIID
        JOIN CommercialHealthClaims.LOOKUP_DEFHC_ID C 
        ON A.BILLING_NPIID = C.NPIID
        WHERE CLAIM_YEAR >= 2016
        AND C.DEFHC_ID IS NOT NULL 
        AND A.BILLING_NPIID <> A.RENDERING_NPIID 
        UNION DISTINCT 
        SELECT B.NPI AS RENDERING_NPI
            , C.DEFHC_ID
            , DPID 
        FROM CommercialHealthClaims.CLMS_EntityData A 
        JOIN CommercialHealthClaims.LOOKUP_DEFHC_ID B 
        ON A.RENDERING_NPIID = B.NPIID 
        JOIN CommercialHealthClaims.LOOKUP_DEFHC_ID C 
        ON A.SERVICE_FACILITY_NPIID = C.NPIID
        WHERE CLAIM_YEAR >= 2016
        AND C.DEFHC_ID IS NOT NULL 
        AND A.BILLING_NPIID <> A.RENDERING_NPIID 
    ) 
GROUP BY PHYSICIAN_NPI
    , DEFHC_ID
"""

print(datetime.now(), ' Step 5: Calculating unique patients - commercial') 
bq_conn.query(sql_patients_comm).result()
print(datetime.now(), ' Step 5 complete') 



# MEDICARE 

medicare_years_df = pd.DataFrame()
medicare_years_qtrly_df = pd.DataFrame()

for db in(list(pd.read_sql("""SELECT [name] AS DB FROM sys.databases WHERE [name] LIKE 'MEDICARE_SAF_20%' """, ssms_conn)['DB'].values)):
    medicare_years_df = medicare_years_df.append(pd.read_sql("""SELECT {yr} AS CLAIM_YEAR FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME ='INPATIENT_CLM_PRCDR' AND {yr} > 2015 """.format(db=db, yr=db[-4:]), ssms_conn))
    
for db in(list(pd.read_sql("""SELECT [name] AS DB FROM sys.databases WHERE [name] LIKE 'MEDICARE_SAF_20%' """, ssms_conn)['DB'].values)):
    medicare_years_qtrly_df = medicare_years_qtrly_df.append(pd.read_sql("""SELECT {yr} AS CLAIM_YEAR FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME ='INPATIENT_CLM_PRCDR_QTRLY' AND {yr} > 2015 """.format(db=db, yr=db[-4:]), ssms_conn))
    
medicare_years = list(medicare_years_df['CLAIM_YEAR'].values)
medicare_years_qtrly = list(set(medicare_years_qtrly_df['CLAIM_YEAR'].values) - set(medicare_years))


print('Medicare full years: ', medicare_years)
print('Medicare quarterly years: ', medicare_years_qtrly)




# update DS_WORK.dbo.LOOKUP_DEFHC_ID
df_lookup_defhc = bq_conn.query('SELECT NPI, IFNULL(DEFHC_ID,0) AS DEFHC_ID FROM CommercialHealthClaims.LOOKUP_DEFHC_ID').result().to_dataframe()



print(datetime.now(), ' Step 6: Updating DHCSQLP02.DS_WORK.dbo.LOOKUP_DEFHC_ID')

cursor = ssms_conn.cursor() 
cursor.execute('TRUNCATE TABLE DS_WORK.dbo.LOOKUP_DEFHC_ID')

for i, row in df_lookup_defhc.iterrows():
    cursor.execute('INSERT INTO DS_WORK.dbo.LOOKUP_DEFHC_ID VALUES (?, ?)', int(row['NPI']), int(row['DEFHC_ID']))
    ssms_conn.commit() 

cursor.close()

print(datetime.now(), ' Step 6 complete.')



sql_patients_mcr = ''

for yr in [medicare_years[0]]: 
    sql_patients_mcr += """
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
    UNION 
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
    UNION 
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
    UNION 
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
    UNION 
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
    UNION 
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
    UNION 
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
    UNION 
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
    UNION 
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
    UNION 
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
    UNION 
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
    UNION 
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
    UNION 
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
    UNION 
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
    """.format(yr=yr)
    
for yr in medicare_years[1:]: 
    sql_patients_mcr += """
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
    UNION 
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
    UNION 
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}
    """.format(yr=yr)
    
for yr in medicare_years_qtrly: 
    sql_patients_mcr += """
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}_q
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}_q
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.inp_claimsk_lds_100_{yr}_q
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}_q
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}_q
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.out_claimsk_lds_100_{yr}_q
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}_q
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}_q
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.snf_claimsk_lds_100_{yr}_q
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}_q
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}_q
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hha_claimsk_lds_100_{yr}_q
    UNION  
    SELECT AT_PHYSN_NPI AS PHYSICIAN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}_q
    UNION  
    SELECT OP_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}_q
    UNION  
    SELECT RNDRNG_PHYSN_NPI, ORG_NPI_NUM, DESY_SORT_KEY  
    FROM MEDICARE_SAF_{yr}.dbo.hosp_claimsk_lds_100_{yr}_q""".format(yr=yr)
    

sql_patients_mcr = """SELECT PHYSICIAN_NPI, DEFHC_ID, COUNT(DISTINCT DESY_SORT_KEY) AS PHYSICIAN_TOTAL_PATIENTS_MCR
                        FROM ({sql}) A 
                        JOIN DS_WORK.dbo.LOOKUP_DEFHC_ID B 
                        ON A.ORG_NPI_NUM = CAST(B.NPI AS VARCHAR) 
                        GROUP BY PHYSICIAN_NPI, DEFHC_ID""".format(sql=sql_patients_mcr)




print(datetime.now(), ' Step 7: calculating unique patients - Medicare ')

df_patients_mcr = pd.read_sql(sql_patients_mcr, ssms_conn)


#TO BQ 
bq_conn.query('DELETE FROM DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_PATIENTS_MEDICARE WHERE 1=1').result()
bq_conn.load_table_from_dataframe(df_patients_mcr, 'DS_WORK.PHYSICIAN_AFFILIATION_TOTAL_PATIENTS_MEDICARE').result()


print(datetime.now(), ' Step 7 complete.')

