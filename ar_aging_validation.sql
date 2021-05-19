SELECT *
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE COMPANY = '20.02-SESC'
;


SELECT *
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE COMPANY = '20.02-SESC'
;

with gcprod as (

    SELECT 
        COMPANY
        , COUNT("DOCUMENT NO.") as numDocuments_GCPROD
        , SUM("AR MONTHEND BALANCE") as balance_gcprod
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY
    GROUP BY COMPANY

), zap as (

    SELECT  
        COMPANY
        , COUNT("DOCUMENT NO.") as numDocuments_zap
        , SUM("AR MONTHEND BALANCE") as balance_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    GROUP BY COMPANY

)

select 
    gcprod.company as company_gcprod
    , zap.company as company_zap
    , numDocuments_GCPROD
    , numDocuments_zap
    , numDocuments_zap - numDocuments_GCPROD as numDocuments_ZapMinusGCPROD
    , balance_gcprod
    , balance_zap
    , balance_zap - balance_gcprod as balance_ZapMinusGCPROD
FROM 
    gcprod FULL OUTER JOIN  zap on gcprod.company = zap.company

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
