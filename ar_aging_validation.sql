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
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY

), zap as (

    SELECT  
        COMPANY
        , COUNT("DOCUMENT NO.") as numDocuments_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY

)

select 
    gcprod.company as company_gcprod
    , zap.company as company_zap
    , numDocuments_GCPROD
    , numDocuments_zap
FROM 
    gcprod FULL OUTER JOIN  zap on gcprod.company = zap.company


