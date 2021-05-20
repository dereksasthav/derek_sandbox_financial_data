SELECT *
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE COMPANY = '20.02-SESC'
;


SELECT *
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE COMPANY = '20.02-SESC'
;

-- check dates 
select distinct datemonthend
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
ORDER BY DATEMONTHEND;

with gcprod as (
  select distinct datemonthend
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
), zap as (
    
  select distinct datemonthend
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
)

select gcprod.datemonthend as gcprod, zap.datemonthend as zap
from gcprod full outer join zap on gcprod.datemonthend = zap.datemonthend
order by gcprod.datemonthend desc


with gcprod as (

    SELECT 
        COMPANY
        , DATEMONTHEND
        , COUNT(*) as numRecords_GCPROD
        , SUM("AR MONTHEND BALANCE") as balance_gcprod
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
    GROUP BY COMPANY, DATEMONTHEND

), zap as (

    SELECT  
        COMPANY
        , DATEMONTHEND
        , COUNT(*) as numRecords_zap
        , SUM("AR MONTHEND BALANCE") as balance_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    GROUP BY COMPANY, DATEMONTHEND

)

select 
    gcprod.company as company_gcprod
    , zap.company as company_zap
    , gcprod.DATEMONTHEND as DATEMONTHEND_gcprod
    , zap.DATEMONTHEND as DATEMONTHEND_zap
    , numRecords_GCPROD
    , numRecords_zap
    , numRecords_zap - numRecords_GCPROD as numRecords_ZapMinusGCPROD
    , balance_gcprod
    , balance_zap
    , balance_zap - balance_gcprod as balance_ZapMinusGCPROD
FROM 
    gcprod FULL OUTER JOIN  zap on gcprod.company = zap.company AND gcprod.DATEMONTHEND = zap.DATEMONTHEND
WHERE 1=1
    AND (gcprod.company IS NULL or zap.company IS NULL)

/** check document counts **/
with gcprod as (

    SELECT 
        COMPANY
        , DATEMONTHEND
        , "DOCUMENT NO." as document_no
        , COUNT(*) as numRecords_GCPROD
        , SUM("AR MONTHEND BALANCE") as balance_gcprod
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC'
    GROUP BY COMPANY
        , DATEMONTHEND
        , "DOCUMENT NO."

), zap as (

    SELECT  
        COMPANY
        , DATEMONTHEND
        , "DOCUMENT NO." as document_no
        , COUNT(*) as numRecords_zap
        , SUM("AR MONTHEND BALANCE") as balance_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC'
    GROUP BY COMPANY, DATEMONTHEND
        , "DOCUMENT NO."

)

select 
    gcprod.company as company_gcprod
    , gcprod.document_no as document_no_gcprod
    , zap.company as company_zap
    , zap.document_no as document_no_zap
    , gcprod.DATEMONTHEND as DATEMONTHEND_gcprod
    , zap.DATEMONTHEND as DATEMONTHEND_zap
    , numRecords_GCPROD
    , numRecords_zap
    , balance_gcprod
    , balance_zap
    , IFNULL(balance_zap,0) - IFNULL(balance_gcprod,0) as balance_ZapMinusGCPROD
    , IFNULL(numRecords_zap,0) - IFNULL(numRecords_GCPROD,0) as numRecords_ZapMinusGCPROD
FROM 
    gcprod FULL OUTER JOIN  zap on gcprod.company = zap.company AND gcprod.document_no = zap.document_no
WHERE 
    gcprod.document_no IS NULL or zap.document_no IS NULL
    ABS(IFNULL(balance_zap,0) - IFNULL(balance_gcprod,0)) > 1
    OR ABS(IFNULL(numRecords_zap,0) - IFNULL(numRecords_GCPROD,0)) > 0
    

/** investigate a specific company to find variance in records **/


with gcprod as (

    SELECT  
        SUMMARYKEY
        , COMPANY
        , "DOCUMENT NO." as document_no
        , "AR MONTHEND BALANCE" as ar_monthend_balance
        , "DOCUMENT DATE" as document_date
        , "POSTING DATE" as posting_date
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC'

), zap as (

    SELECT  
        SUMMARYKEY
        , COMPANY
        , "DOCUMENT NO." as document_no
        , "AR MONTHEND BALANCE" as ar_monthend_balance
        , "DOCUMENT DATE" as document_date
        , "POSTING DATE" as posting_date
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company ='20.02-SESC'

)

select 
    gcprod.SUMMARYKEY as summarykey_gcprod
    , gcprod.company as company_gcprod
    , gcprod.document_no as document_no_gcprod
    , gcprod.ar_monthend_balance as ar_monthend_balance_gcprod
    , zap.SUMMARYKEY as summarykey_zap
    , zap.company as company_zap
    , zap.document_no as document_no_zap
    , zap.ar_monthend_balance as ar_monthend_balance_zap
FROM 
    gcprod FULL OUTER JOIN  zap on gcprod.SUMMARYKEY = zap.SUMMARYKEY
WHERE zap.SUMMARYKEY is null or gcprod.SUMMARYKEY is null


/** investigate specific document records **/

SELECT DATEMONTHEND, replace(company,'_','.') as company, "CUSTOMER NO.", "DOCUMENT NO.", "DOCUMENT DATE", "AR MONTHEND BALANCE", "POSTING DATE", "DUE AGED DAYS", "INVOICE AGED DAYS"
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE company = '20.02-SESC' AND "DOCUMENT NO." = 'PROVDISCCM-30'
UNION
SELECT DATEMONTHEND, replace(company,'_','.') as company, "CUSTOMER NO.", "DOCUMENT NO.", "DOCUMENT DATE", "AR MONTHEND BALANCE", "POSTING DATE", "DUE AGED DAYS", "INVOICE AGED DAYS"
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE company = '20.02-SESC' AND "DOCUMENT NO." = 'PROVDISCCM-30'
ORDER BY source 

/** Testing joins with due date **/
with tmp as (
    SELECT *
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC' AND "DOCUMENT NO." = 'PROVDISCCM-30'
    UNION
    SELECT *
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC' AND "DOCUMENT NO." = 'PROVDISCCM-30'
)
select 
    fnl.*
    , due_age."BUCKET SHORT"
from tmp fnl 
LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY due_age
    on TRIM(replace(fnl."COMPANY",'_','.')) =  TRIM(replace(due_age.COMPANY,'_','.')  ) 
    and fnl."DUE AGED DAYS" >= due_age."FROM"
    and fnl."DUE AGED DAYS" <  due_age."TO"
