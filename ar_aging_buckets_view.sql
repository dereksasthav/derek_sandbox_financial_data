WITH raw_buckets AS (

    SELECT * FROM DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY

), ar_aging AS (

    SELECT * FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY

), all_companies AS (

    SELECT DISTINCT COMPANY
    FROM ar_aging

), buckets_companies AS (

    SELECT DISTINCT COMPANY
    FROM raw_buckets

), missing_companies AS (

    SELECT COMPANY 
    FROM all_companies
    EXCEPT
    SELECT COMPANY 
    FROM buckets_companies

), default_buckets AS (

    SELECT *
    FROM raw_buckets
    WHERE COMPANY = '10.01-GCI'


), missing_buckets AS (

    SELECT 
        abbc_in."BUCKET ID"
        , abbc_in."BUCKET TYPE"
        , mc.COMPANY
        , abbc_in."BUCKET SHORT"
        , abbc_in."BUCKET DESC FOR DUE"
        , abbc_in."BUCKET DESC FOR INVOICE"
        , abbc_in."FROM"
        , abbc_in."TO"
        , abbc_in."DIRECTION FOR DUE"
        , abbc_in."DIRECTION FOR INVOICE"
    FROM missing_companies mc 
    CROSS JOIN default_buckets abbc_in 


), unioned_data AS (

    SELECT 
         "BUCKET ID"
        , "BUCKET TYPE"
        , COMPANY
        , "BUCKET SHORT"
        , "BUCKET DESC FOR DUE"
        , "BUCKET DESC FOR INVOICE"
        , "FROM"
        , "TO"
        , "DIRECTION FOR DUE"
        , "DIRECTION FOR INVOICE"
    FROM missing_buckets
    
    UNION 

    SELECT 
         "BUCKET ID"
        , "BUCKET TYPE"
        , COMPANY
        , "BUCKET SHORT"
        , "BUCKET DESC FOR DUE"
        , "BUCKET DESC FOR INVOICE"
        , "FROM"
        , "TO"
        , "DIRECTION FOR DUE"
        , "DIRECTION FOR INVOICE"
    FROM raw_buckets


)

SELECT * 
FROM unioned_data