SELECT *
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST
WHERE COMPANY = '20.02-SESC'
;


SELECT *
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE COMPANY = '20.02-SESC'
;

-- check dates 
select distinct datemonthend
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST
ORDER BY DATEMONTHEND;

select distinct datemonthend
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
ORDER BY DATEMONTHEND;

-- compare dates
with gcprod as (

    select distinct datemonthend
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST

), zap as (

    select distinct datemonthend
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
)

select gcprod.datemonthend as gcprod, zap.datemonthend as zap
from gcprod full outer join zap on gcprod.datemonthend = zap.datemonthend
order by gcprod.datemonthend desc

/** check by company by month **/

with gcprod as (

    SELECT 
        REPLACE(COMPANY,'_','.') as COMPANY
        , DATEMONTHEND
        , COUNT(*) as numRecords_GCPROD
        , SUM("AR MONTHEND BALANCE") as balance_gcprod
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST2
    WHERE 1=1
        AND REPLACE(COMPANY,'_','.') = '20.02-SESC'
    GROUP BY REPLACE(COMPANY,'_','.'), DATEMONTHEND

), zap as (

    SELECT  
        COMPANY
        , DATEMONTHEND
        , COUNT(*) as numRecords_zap
        , SUM("AR MONTHEND BALANCE") as balance_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE 1=1
        AND company = '20.02-SESC'
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
    , ROUND(IFNULL(balance_zap,0) - IFNULL(balance_gcprod,0)) as balance_ZapMinusGCPROD
FROM 
    gcprod FULL OUTER JOIN  zap on gcprod.company = zap.company AND gcprod.DATEMONTHEND = zap.DATEMONTHEND
WHERE 1=1
    AND (gcprod.company IS NULL or zap.company IS NULL)
    AND coalesce(gcprod.datemonthend, zap.datemonthend) <= '2021-04-30'
ORDER BY company_zap, DATEMONTHEND_zap

/** check document counts **/
with gcprod as (

    SELECT 
        COMPANY
        , DATEMONTHEND
        , "DOCUMENT NO." as document_no
        , COUNT(*) as numRecords_GCPROD
        , SUM("AR MONTHEND BALANCE") as balance_gcprod
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST
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
    gcprod FULL OUTER JOIN  zap 
    on gcprod.company = zap.company 
    AND gcprod.document_no = zap.document_no
    AND gcprod.DATEMONTHEND = zap.datemonthend
WHERE 1=1
    AND gcprod.document_no IS NULL or zap.document_no IS NULL
    --ABS(IFNULL(balance_zap,0) - IFNULL(balance_gcprod,0)) > 1
    --OR ABS(IFNULL(numRecords_zap,0) - IFNULL(numRecords_GCPROD,0)) > 0
    

/** check specific row counts and balances by customer**/
with gcprod as (

    SELECT 
        REPLACE(COMPANY,'_','.') as COMPANY
        , DATEMONTHEND
        , "CUSTOMER NO." as customer_no
        , COUNT(*) as numRecords_GCPROD
        , SUM("AR MONTHEND BALANCE") as balance_gcprod
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST2
    WHERE REPLACE(COMPANY,'_','.') = '20.02-SESC'
    GROUP BY REPLACE(COMPANY,'_','.')
        , DATEMONTHEND
        , "CUSTOMER NO."

), zap as (

    SELECT  
        COMPANY
        , DATEMONTHEND
        , "CUSTOMER NO." as customer_no
        , COUNT(*) as numRecords_zap
        , SUM("AR MONTHEND BALANCE") as balance_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC'
    GROUP BY COMPANY, DATEMONTHEND
        , "CUSTOMER NO."

)

select 
    gcprod.company as company_gcprod
    , gcprod.customer_no as customer_no_gcprod
    , zap.company as company_zap
    , zap.customer_no as customer_no_zap
    , gcprod.DATEMONTHEND as DATEMONTHEND_gcprod
    , zap.DATEMONTHEND as DATEMONTHEND_zap
    , numRecords_GCPROD
    , numRecords_zap
    , balance_gcprod
    , balance_zap
    , ROUND(IFNULL(balance_zap,0) - IFNULL(balance_gcprod,0)) as balance_ZapMinusGCPROD
    , ROUND(IFNULL(numRecords_zap,0) - IFNULL(numRecords_GCPROD,0)) as numRecords_ZapMinusGCPROD
FROM 
    gcprod FULL OUTER JOIN  zap 
    on gcprod.company = zap.company 
    AND gcprod.customer_no = zap.customer_no
    AND gcprod.DATEMONTHEND = zap.datemonthend
WHERE 1=1
    AND (gcprod.customer_no IS NULL or zap.customer_no IS NULL)
    AND coalesce(zap.datemonthend, gcprod.datemonthend) = '2021-04-30'
    --ABS(IFNULL(balance_zap,0) - IFNULL(balance_gcprod,0)) > 1
    --OR ABS(IFNULL(numRecords_zap,0) - IFNULL(numRecords_GCPROD,0)) > 0
    


/** investigate a specific company to find variance in records **/


with gcprod as (

    SELECT  
        SUMMARYKEY
        , COMPANY
        , "DOCUMENT NO." as document_no
        , "AR MONTHEND BALANCE" as ar_monthend_balance
        , "DOCUMENT DATE" as document_date
        , "POSTING DATE" as posting_date
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST
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
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST
WHERE company = '20.02-SESC' AND "DOCUMENT NO." = 'PROVDISCCM-30'
UNION
SELECT DATEMONTHEND, replace(company,'_','.') as company, "CUSTOMER NO.", "DOCUMENT NO.", "DOCUMENT DATE", "AR MONTHEND BALANCE", "POSTING DATE", "DUE AGED DAYS", "INVOICE AGED DAYS"
FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
WHERE company = '20.02-SESC' AND "DOCUMENT NO." = 'PROVDISCCM-30'
ORDER BY source 

/** Testing joins with due date **/
with tmp as (
    SELECT *
    FROM DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST
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















/** recreating Abraham's query **/

with customer_ledger as (

    select
        coalesce(  dcle."COMPANYCODE" , cle."COMPANYCODE") "COMPANY"
        , coalesce(  dcle."CUSTOMER NO_" , cle."CUSTOMER NO_")  "CUSTOMER NO."
        , coalesce(  dcle."DOCUMENT NO_" , cle."DOCUMENT NO_" )  "DOCUMENT NO."
        , coalesce(  dcle."CURRENCY CODE", cle."CURRENCY CODE") "CURRENCY CODE"
        , cle."DOCUMENT DATE" "DOCUMENT DATE"
        , coalesce(  dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE" )  "INITIAL ENTRY DUE DATE"
        , dcle."AMOUNT (LCY)"  "AR MONTHEND BALANCE (LCY)"
        , dcle."AMOUNT" "AR MONTHEND BALANCE"
        , current_date()"ZAP_TIMESTAMP"
        , current_date()"ZAP_CREATEDTIME"
        , coalesce(  dcle."CUST_ LEDGER ENTRY NO_", cle."ENTRY NO_") "CUST. LEDGER ENTRY NO."
        , dcle."DEBIT AMOUNT (LCY)" "AR INVOICE AMOUNT (LCY)"
        , dcle."DEBIT AMOUNT"      "AR INVOICE AMOUNT"
        , cle."CLOSED AT DATE"
        , cle."CLOSED BY AMOUNT"
        , cle."CLOSED BY AMOUNT (LCY)"
        , cle."POSITIVE"
        , cle."OPEN" 
        --"DUE AGED DAYS"
        --"INVOICE AGED DAYS"
        --, "BUCKET ID BY DUE"
        --, "BUCKET ID BY INVOICE"
        , coalesce(  dcle."CURRENCY CODE", cle."CURRENCY CODE" ) "CURRENCY CODE LCY"
        , coalesce(dcle."POSTING DATE", cle."POSTING DATE") "POSTING DATE"
        , current_date() INGEST_DATETIME
        from DATAWAREHOUSE.NAVCOMPANION.STAGE_DETAILED_CUSTOMER_LEDGER_ENTRY dcle
        left join DATAWAREHOUSE.NAVCOMPANION.STAGE_CUSTOMER_LEDGER_ENTRY  cle
            on   cle."COMPANYCODE"  = dcle."COMPANYCODE"
            and  cle."ENTRY NO_"    = dcle."CUST_ LEDGER ENTRY NO_" 
        where 1=1
            and not exists ( 
                select
                    sub_dcle."COMPANYCODE"
                            , sub_dcle."DOCUMENT NO_"
                            , sub_dcle."CUST_ LEDGER ENTRY NO_"
                        from DATAWAREHOUSE.NAVCOMPANION.STAGE_DETAILED_CUSTOMER_LEDGER_ENTRY sub_dcle
                        where 1=1
                            and sub_dcle."ENTRY TYPE"         = 2
                            and dcle."COMPANYCODE"  = sub_dcle."COMPANYCODE"
                            and dcle."DOCUMENT NO_" = sub_dcle."DOCUMENT NO_"
                            and dcle."CUST_ LEDGER ENTRY NO_" = sub_dcle."CUST_ LEDGER ENTRY NO_"
                        )  
            AND dcle."COMPANYCODE" = '20_02-SESC'

), base_ar as (

    select
        cust_ledgr."CUST. LEDGER ENTRY NO."
        , cust_ledgr."COMPANY"
        , cust_ledgr."CUSTOMER NO."
        , cust_ledgr."DOCUMENT NO."
        , cust_ledgr."CURRENCY CODE"
        , cust_ledgr."POSTING DATE"
        , cust_ledgr."CURRENCY CODE LCY"
        , cust_ledgr."DOCUMENT DATE"
        , cust_ledgr."INITIAL ENTRY DUE DATE"
        , cust_ledgr."OPEN"
        , cust_ledgr."CLOSED AT DATE"
        , substr(cust_ledgr."CLOSED AT DATE",1,10)      CLOSED_SUBSTR
        , sum(cust_ledgr."AR INVOICE AMOUNT (LCY)")     "AR INVOICE AMOUNT (LCY)"
        , sum(cust_ledgr."AR INVOICE AMOUNT")           "AR INVOICE AMOUNT"
        , sum(cust_ledgr."AR MONTHEND BALANCE (LCY)")   "AR MONTHEND BALANCE (LCY)"
        , sum(cust_ledgr."AR MONTHEND BALANCE")         "AR MONTHEND BALANCE"
    from customer_ledger cust_ledgr
    group by 
        cust_ledgr."CUST. LEDGER ENTRY NO."
        , cust_ledgr."COMPANY"
        , cust_ledgr."CUSTOMER NO."
        , cust_ledgr."DOCUMENT NO."
        , cust_ledgr."CURRENCY CODE"
        , cust_ledgr."POSTING DATE"
        , cust_ledgr."CURRENCY CODE LCY"
        , cust_ledgr."DOCUMENT DATE"
        , cust_ledgr."INITIAL ENTRY DUE DATE"
        , cust_ledgr."OPEN"
        , cust_ledgr."CLOSED AT DATE"
        , substr(cust_ledgr."CLOSED AT DATE",1,10) 


), mon_dt as (

    select
        dt.MONTH_YEAR_ABREV
        , max(dt.date_key)         AS DATE_MONTH_END
    from DATAWAREHOUSE.PUBLIC.LU_STATIC_RAW_CALENDAR dt
    where 1=1
        and dt.date_key >= to_date('2017-01-01')
        and dt.date_key <  current_date()
    group by 
        dt.MONTH_YEAR_ABREV


), fnl as (

    select
        mon_dt.DATE_MONTH_END                 as DATE_MONTH_END
        , base_ar."CUST. LEDGER ENTRY NO."    as "CUST. LEDGER ENTRY NO."
        , replace(base_ar."COMPANY",'_','.')  as "COMPANY"
        , base_ar."CUSTOMER NO."              as "CUSTOMER NO."
        , base_ar."DOCUMENT NO."              as "DOCUMENT NO."
        , base_ar."CURRENCY CODE"             as "CURRENCY CODE"
        , base_ar."POSTING DATE"              as "POSTING DATE"
        , base_ar."CURRENCY CODE LCY"         as "CURRENCY CODE LCY"
        , base_ar."DOCUMENT DATE"             as "DOCUMENT DATE"
        , base_ar."INITIAL ENTRY DUE DATE"    as "INITIAL ENTRY DUE DATE"
        , base_ar."OPEN"                      as "OPEN"
        , base_ar."AR INVOICE AMOUNT (LCY)"   as "AR INVOICE AMOUNT (LCY)"
        , base_ar."AR INVOICE AMOUNT"         as "AR INVOICE AMOUNT"
        , base_ar."AR MONTHEND BALANCE (LCY)" as "AR MONTHEND BALANCE (LCY)"
        , base_ar."AR MONTHEND BALANCE"       as "AR MONTHEND BALANCE"
        , datediff(day, mon_dt.DATE_MONTH_END, to_date(base_ar."INITIAL ENTRY DUE DATE")) as  "DUE AGED DAYS"
        , datediff(day, mon_dt.DATE_MONTH_END, to_date(base_ar."DOCUMENT DATE")) as  "INVOICE AGED DAYS"
        , base_ar.CLOSED_SUBSTR  
    FROM mon_dt CROSS JOIN base_ar
    WHERE 1=1
        /** possible scenarios
        1. The document is closed: need to filter to POSTDATE <= MONTHEND and CLOSEDATE > MONTHEND
        2. The document is open: need to filter to POSTDATE <= MONTHEND and (OPEN=1 OR CLOSEDDATE = 1753) 
        **/
        AND to_date(base_ar."POSTING DATE") <= to_date(mon_dt.DATE_MONTH_END)
        -- AND to_date(mon_dt.DATE_MONTH_END) <> to_date(last_day(current_date())) -- WHAT IS THIS????
        AND (
            -- document is open 
            (base_ar."OPEN" = 1 OR  base_ar.CLOSED_SUBSTR = '1753-01-01')
            OR
            -- document is closed
            to_date(base_ar."CLOSED AT DATE") >= to_date(mon_dt.DATE_MONTH_END)

        )

), zap as (

    SELECT *
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE COMPANY = '20.02-SESC' AND datemonthend = '2021-04-30'

)






SELECT fnl."CUST. LEDGER ENTRY NO." as fnl, zap."CUST. LEDGER ENTRY NO." as zap
FROM fnl
FULL OUTER JOIN zap ON fnl."CUST. LEDGER ENTRY NO." = zap."CUST. LEDGER ENTRY NO."
WHERE (fnl.DATE_MONTH_END = '2021-04-30' OR zap.DATEMONTHEND = '2021-04-30')
AND (fnl."CUST. LEDGER ENTRY NO." IS NULL OR zap."CUST. LEDGER ENTRY NO." IS NULL)
ORDER BY fnl."CUST. LEDGER ENTRY NO."


/**
SELECT *
FROM fnl 
WHERE DATE_MONTH_END = '2021-04-30'
AND "DOCUMENT NO." IN (
    SELECT "DOCUMENT NO." FROM fnl EXCEPT SELECT "DOCUMENT NO." FROM zap
)

**/
/** ABRAHAM original logic 
and (
                         (    
                             to_date(base_ar."POSTING DATE") <= to_date(mon_dt.DATE_MONTH_END)
                           AND 
                             to_date(mon_dt.DATE_MONTH_END) <> to_date(last_day(current_date()))  --- why is this needed? why wouldn't you want records 
                           AND   
                             to_date(base_ar."CLOSED AT DATE") >= to_date(mon_dt.DATE_MONTH_END)  --- should this not be greater than? if the document was closed in that month would you want that showing up?
                         )
                        OR (
                             to_date(base_ar."POSTING DATE") <= to_date(mon_dt.DATE_MONTH_END)
                           AND 
                             to_date(mon_dt.DATE_MONTH_END)  <> last_day(current_date())  -- why are we removing the last month
                           AND
                             base_ar.CLOSED_SUBSTR = '1753-01-01'  ---- what does this mean?
                           )
                        OR (
                              to_date(mon_dt.DATE_MONTH_END) = last_day(current_date())  --- why would we only include open ones in the latest month? They could have been posted in prior months?
                            AND
                              base_ar."OPEN" = 1
                           )
                      )

**/


/** Notes
ZAP includes records that are not included in GCPROD, mostly due to filter on Entry Type = 2
Should not be removing paid off documents, should keep them until they are paid off, this can happen in the cross join
Not sure the value of this exercise without just seeing how ZAP does it

**/








/** LATEST CODE as of morning of 5/21 **/

with base_ar as ( -- base customer ledger entry data
                              select
                                  coalesce(  dcle."COMPANYCODE" 
                                           , cle."COMPANYCODE"
                                          )                         "COMPANY"
                                , coalesce(  dcle."CUSTOMER NO_" 
                                          , cle."CUSTOMER NO_"
                                          )                         "CUSTOMER NO."
                                , coalesce(  dcle."DOCUMENT NO_" 
                                           , cle."DOCUMENT NO_"
                                          )                         "DOCUMENT NO."
                                , coalesce(  dcle."CUST_ LEDGER ENTRY NO_"
                                           , cle."ENTRY NO_"
                                          )                         "CUST. LEDGER ENTRY NO."
                               , coalesce(dcle."POSTING DATE" 
                                           , cle."POSTING DATE"
                                          )                         "POSTING DATE"
                               , cle."OPEN" 
                               , cle."CLOSED AT DATE"
                               , cle."CLOSED BY AMOUNT"
                               , dcle."ENTRY TYPE"
                               , coalesce(  dcle."CURRENCY CODE"
                                          , cle."CURRENCY CODE"
                                         )                         "CURRENCY CODE"
                               , cle."DOCUMENT DATE"               "DOCUMENT DATE"
                               , coalesce(  dcle."INITIAL ENTRY DUE DATE"
                                          , cle."DUE DATE"
                                         )                         "INITIAL ENTRY DUE DATE"
                               , dcle."AMOUNT (LCY)"               "AR MONTHEND BALANCE (LCY)"
                               , dcle."AMOUNT"                     "AR MONTHEND BALANCE"
                               , case 
                                   when dcle."ENTRY TYPE" = 1 
                                     then  dcle."DEBIT AMOUNT"
                                 end                               POSTED_INVOICE
                               , case 
                                   when dcle."ENTRY TYPE" = 2 
                                     then  dcle."CREDIT AMOUNT"
                                 end                               POSTED_PAYMENT
                               , current_date()                    "ZAP_TIMESTAMP"
                               , current_date()                    "ZAP_CREATEDTIME"
                              , dcle."DEBIT AMOUNT (LCY)"          "AR INVOICE AMOUNT (LCY)"
                              , dcle."DEBIT AMOUNT"                "AR INVOICE AMOUNT"
                              , cle."CLOSED BY AMOUNT (LCY)"
                              , cle."POSITIVE"
                              , dcle."SOURCE CODE"

                              --"DUE AGED DAYS"
                              --"INVOICE AGED DAYS"
                              --, "BUCKET ID BY DUE"
                              --, "BUCKET ID BY INVOICE"
                              , coalesce(  dcle."CURRENCY CODE"
                                          , cle."CURRENCY CODE"
                                         )                         "CURRENCY CODE LCY"
                              , current_date()                     INGEST_DATETIME
                              from DATAWAREHOUSE.NAVCOMPANION.STAGE_DETAILED_CUSTOMER_LEDGER_ENTRY dcle
                              left join DATAWAREHOUSE.NAVCOMPANION.STAGE_CUSTOMER_LEDGER_ENTRY  cle
                                on   cle."COMPANYCODE"            = dcle."COMPANYCODE"
                                and  cle."ENTRY NO_"              = dcle."CUST_ LEDGER ENTRY NO_" 
                              where 1=1
                                -- and dcle."ENTRY TYPE" = 1 -- get all invoiced
                                -- and dcle."ENTRY TYPE" = 2 -- get all payments
                                and coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE" )   = '20_02-SESC'
                                AND coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_") = 'S-0000736-C'
                                AND coalesce(  dcle."CUST_ LEDGER ENTRY NO_" , cle."ENTRY NO_") IN (107303)
                                                         
                             
          ) , ar_inv as       (
                                select  -- get all invoices
                                   base_ar."COMPANY"
                                 , base_ar."CUSTOMER NO."
                                 , base_ar."CUST. LEDGER ENTRY NO."
                                 , base_ar."ENTRY TYPE"
                                 , base_ar."POSTING DATE"
                                 , base_ar."DOCUMENT NO."
                                 , base_ar."OPEN"
                                 , base_ar."CURRENCY CODE"
                                 , base_ar."CLOSED AT DATE"
                                 , base_ar."CLOSED BY AMOUNT" 
                                 , base_ar."DOCUMENT DATE"
                                 , base_ar."INITIAL ENTRY DUE DATE"
                                 , base_ar."AR MONTHEND BALANCE (LCY)"
                                 , base_ar."AR MONTHEND BALANCE"
                                 , base_ar."AR INVOICE AMOUNT (LCY)"
                                 , base_ar."AR INVOICE AMOUNT"
                                 , base_ar."OPEN" 
                                 -- ,  ROW_NUMBER() OVER (
                                 --                        PARTITION BY  base_ar."COMPANY"
                                 --                                    , base_ar."CUSTOMER NO."
                                 --                        ORDER BY      base_ar."COMPANY"
                                 --                                    , base_ar."CUSTOMER NO."
                                 --                                    , base_ar."CUST. LEDGER ENTRY NO."
                                 --                      ) as rownum   
                                from base_ar
                                where 1=1
                                  and "ENTRY TYPE" = 1
                             ) 
           , ar_pmt as      (
                                select  -- get all payments
                                   base_ar."COMPANY"
                                 , base_ar."CUSTOMER NO."
                                 , base_ar."CUST. LEDGER ENTRY NO."
                                 , base_ar."ENTRY TYPE"
                                 , base_ar."POSTING DATE"
                                 , base_ar."DOCUMENT NO."
                                 , base_ar."OPEN"
                                 , base_ar."CLOSED AT DATE"
                                 , base_ar."CLOSED BY AMOUNT" 
                                 , base_ar."DOCUMENT DATE"
                                 , base_ar."INITIAL ENTRY DUE DATE"
                                 , base_ar."AR MONTHEND BALANCE (LCY)"
                                 , base_ar."AR MONTHEND BALANCE"
                                 , base_ar."AR INVOICE AMOUNT (LCY)"
                                 , base_ar."AR INVOICE AMOUNT"
                                 , base_ar."OPEN" 
                                 -- ,  ROW_NUMBER() OVER (
                                 --                        PARTITION BY  base_ar."COMPANY"
                                 --                                    , base_ar."CUSTOMER NO."
                                 --                        ORDER BY      base_ar."COMPANY"
                                 --                                    , base_ar."CUSTOMER NO."
                                 --                                    , base_ar."CUST. LEDGER ENTRY NO."
                                 --                      ) as rownum   
                                from base_ar
                                where 1=1
                                  and "ENTRY TYPE" = 2
                             )

           , ar_full as     (
                                select  -- get moving balance
                                   base_ar."COMPANY"
                                 , base_ar."CUSTOMER NO."
                                 , base_ar."CUST. LEDGER ENTRY NO."  
                                 , base_ar."ENTRY TYPE"
                                 , base_ar."POSTING DATE"
                                 , base_ar."DOCUMENT NO."
                                 , base_ar."OPEN"
                                 , base_ar."CLOSED AT DATE"
                                 , base_ar."CLOSED BY AMOUNT" 
                                 , base_ar."DOCUMENT DATE"
                                 , base_ar."INITIAL ENTRY DUE DATE"
                                 , base_ar."OPEN" 
                                 , base_ar."CLOSED AT DATE"
                                 , base_ar."CLOSED BY AMOUNT" 
                                 , sum(base_ar."AR MONTHEND BALANCE (LCY)")   over (
                                                                     partition by base_ar."COMPANY"
                                                                                , base_ar."CUSTOMER NO."
                                                                     order by     base_ar."COMPANY"
                                                                                , base_ar."CUSTOMER NO."
                                                                                , base_ar."CUST. LEDGER ENTRY NO."
                                                                                , base_ar."ENTRY TYPE"
                                                                      rows between unbounded preceding and current row
                                                                   )   CUMM_AR_MONTHEND_BALANCE_LCY
                                 , sum(base_ar."AR MONTHEND BALANCE")   over (
                                                                     partition by base_ar."COMPANY"
                                                                                , base_ar."CUSTOMER NO."
                                                                     order by     base_ar."COMPANY"
                                                                                , base_ar."CUSTOMER NO."
                                                                                , base_ar."CUST. LEDGER ENTRY NO."
                                                                                , base_ar."ENTRY TYPE"
                                                                      rows between unbounded preceding and current row
                                                                   )   CUMM_AR_MONTHEND_BALANCE
                                 ,  ROW_NUMBER() OVER (
                                                        PARTITION BY  base_ar."COMPANY"
                                                                    , base_ar."CUSTOMER NO."
                                                        ORDER BY      base_ar."COMPANY"
                                                                    , base_ar."CUSTOMER NO."
                                                                    , base_ar."CUST. LEDGER ENTRY NO."
                                                      ) as rownum   
                                from  base_ar
                                where 1=1  
                            )
          select
              ROW_NUMBER() OVER (
                                  ORDER BY  fnl.DATE_MONTH_END
                                          , fnl."COMPANY"
                                          , fnl."CUST. LEDGER ENTRY NO."
                                )                as SUMMARYKEY
            , fnl.DATE_MONTH_END                 as DATEMONTHEND
            , fnl."CUST. LEDGER ENTRY NO." 
            , fnl."COMPANY"
            , fnl."CUSTOMER NO."
            , fnl."DOCUMENT NO."
            , fnl."CURRENCY CODE"
            , fnl."POSTING DATE"
            , fnl."CURRENCY CODE"                as  "CURRENCY CODE LCY"
            -- , fnl."CLOSED BY AMOUNT" 
            , fnl."DOCUMENT DATE"
            , fnl."INITIAL ENTRY DUE DATE" 
            , fnl."AR MONTHEND BALANCE (LCY)"
            , fnl."AR MONTHEND BALANCE"
            , fnl."AR INVOICE AMOUNT (LCY)"
            , fnl."AR INVOICE AMOUNT"
            , fnl."DUE AGED DAYS"
            , fnl."INVOICE AGED DAYS"
            , due_age."BUCKET ID"                 as "BUCKET ID BY DUE"
            , inv_age."BUCKET ID"                 as "BUCKET ID BY INVOICE"
            -- , fnl.CUMM_AR_MONTHEND_BALANCE
            -- , fnl.CLOSE_POSTING_DATE
            , current_date()                      as INGEST_DATETIME
          from (
                  select -- cartesian out data 
                       mon_dt.DATE_MONTH_END
                     , replace(sub."COMPANY",'_','.')  as "COMPANY"
                     , sub."CUSTOMER NO."
                     , sub."CUST. LEDGER ENTRY NO." 
                     , sub."DOCUMENT NO."
                     , sub."CURRENCY CODE"
                     , sub."POSTING DATE"
                     , sub."CLOSED BY AMOUNT" 
                     , sub."DOCUMENT DATE"
                     , sub."INITIAL ENTRY DUE DATE" 
                     , sub."AR MONTHEND BALANCE (LCY)"
                     , sub."AR MONTHEND BALANCE"
                     , sub."AR INVOICE AMOUNT (LCY)"
                     , sub."AR INVOICE AMOUNT"
                     , sub.CUMM_AR_MONTHEND_BALANCE
                     , sub.CLOSE_POSTING_DATE
                     , datediff(day, mon_dt.DATE_MONTH_END 
                                , sub."INITIAL ENTRY DUE DATE"
                               )                               as  "DUE AGED DAYS"
                     , datediff(day, mon_dt.DATE_MONTH_END
                                , sub."DOCUMENT DATE"
                               )                               as  "INVOICE AGED DAYS"
                     ,  ROW_NUMBER() OVER (
                                            PARTITION BY  sub."COMPANY"
                                                        , sub."CUSTOMER NO."
                                                        , sub."CUST. LEDGER ENTRY NO."
                                                        , sub."DOCUMENT NO."
                                                        , mon_dt.DATE_MONTH_END
                                            ORDER BY      sub."COMPANY"
                                                        , sub."CUSTOMER NO."
                                                        , sub."CUST. LEDGER ENTRY NO."
                                                        , sub."DOCUMENT NO."
                                                        , mon_dt.DATE_MONTH_END
                                                        , datediff(day, mon_dt.DATE_MONTH_END
                                                                      , sub."INITIAL ENTRY DUE DATE"
                                                                  )
                                          ) as rownum  
                    
                  from ( -- get last day of the month from 2017-01-01 to today
                        select
                           dt.MONTH_YEAR_ABREV
                         , max(dt.date_key)         AS DATE_MONTH_END
                        from DATAWAREHOUSE.PUBLIC.LU_STATIC_RAW_CALENDAR dt
                        where 1=1
                          and dt.date_key >= to_date('2017-01-01')
                          and dt.date_key <  current_date()
                        group by 
                           dt.MONTH_YEAR_ABREV
                       ) mon_dt
                  cross join (
                                select  -- flatten data and evaluate for attribution and bucket association
                                   ar_inv."COMPANY"
                                 , ar_inv."CUSTOMER NO."
                                 , ar_inv."CUST. LEDGER ENTRY NO." 
                                 , ar_inv."DOCUMENT NO."
                                 , ar_inv."CURRENCY CODE"
                                 , to_date(ar_inv."POSTING DATE")            "POSTING DATE"
                                 , to_date(ar_inv."DOCUMENT DATE")           "DOCUMENT DATE"
                                 , to_date(ar_inv."INITIAL ENTRY DUE DATE")  "INITIAL ENTRY DUE DATE"

                                 -- , ar_pmt."POSTING DATE"     CLOSE_POSTING_DATE
                                 -- , ar_pmt."CLOSED AT DATE"
                                 , ar_inv."AR MONTHEND BALANCE (LCY)"
                                 , ar_inv."AR MONTHEND BALANCE"
                                 , ar_inv."AR INVOICE AMOUNT (LCY)"
                                 , ar_inv."AR INVOICE AMOUNT"
                                 , ar_pmt."CLOSED BY AMOUNT" 
                                 , ar_full.CUMM_AR_MONTHEND_BALANCE
                                 , case 
                                     when substr(ar_pmt."CLOSED AT DATE",1,10) = '1753-01-01'
                                       then to_date(ar_pmt."POSTING DATE")
                                       else to_date(ar_pmt."CLOSED AT DATE")
                                   end                      CLOSE_POSTING_DATE
                                from ar_inv
                                join ar_pmt
                                  on  ar_pmt."COMPANY"      = ar_inv."COMPANY"
                                  and ar_pmt."CUSTOMER NO." = ar_inv."CUSTOMER NO."
                                  and ar_pmt."CUST. LEDGER ENTRY NO." = ar_inv."CUST. LEDGER ENTRY NO."
                                join ar_full
                                  on  ar_full."COMPANY"      = ar_inv."COMPANY"
                                  and ar_full."CUSTOMER NO." = ar_inv."CUSTOMER NO."
                                  and ar_full."CUST. LEDGER ENTRY NO." = ar_inv."CUST. LEDGER ENTRY NO."
                                  and ar_full."COMPANY"      = ar_pmt."COMPANY"
                                  and ar_full."CUSTOMER NO." = ar_pmt."CUSTOMER NO."
                                  and ar_full."CUST. LEDGER ENTRY NO." = ar_pmt."CUST. LEDGER ENTRY NO."
                             ) sub
                  where  sub.CUMM_AR_MONTHEND_BALANCE != 0
                    and  datediff(month, sub."POSTING DATE" , sub.CLOSE_POSTING_DATE) != 0
                    and  sub."POSTING DATE"     <= mon_dt.DATE_MONTH_END 
                    and  sub.CLOSE_POSTING_DATE >= mon_dt.DATE_MONTH_END  
               ) fnl
          join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY due_age
            on  fnl."COMPANY"           =  due_age.COMPANY
            and fnl."DUE AGED DAYS"     >= due_age."FROM"
            and fnl."DUE AGED DAYS"     <= due_age."TO"
          join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY inv_age
            on  fnl."COMPANY"           =  inv_age.COMPANY
            and fnl."INVOICE AGED DAYS" >= inv_age."FROM"
            and fnl."INVOICE AGED DAYS" <= inv_age."TO" 
          where 1=1
            and fnl.rownum = 1
            -AND fnl.DATE_MONTH_END  = '2021-03-31'



/** DEREK ATTEMPT **/
with base_ar as (
    
    select
        coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE") "COMPANY"
        , coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_") "CUSTOMER NO."
        , coalesce(  dcle."DOCUMENT NO_"  , cle."DOCUMENT NO_") "DOCUMENT NO."
        , coalesce(  dcle."CUST_ LEDGER ENTRY NO_", cle."ENTRY NO_")    "CUST. LEDGER ENTRY NO."
        , coalesce(dcle."POSTING DATE" , cle."POSTING DATE"  )   "POSTING DATE"
        , dcle.amount 
        , cle."OPEN" 
        , cle."CLOSED AT DATE"
        , cle."CLOSED BY AMOUNT"
        , dcle."ENTRY TYPE"
        , coalesce(  dcle."CURRENCY CODE" , cle."CURRENCY CODE")  "CURRENCY CODE"
        , cle."DOCUMENT DATE"  "DOCUMENT DATE"
        , coalesce(  dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE" )  "INITIAL ENTRY DUE DATE"
        , dcle."AMOUNT (LCY)"  
        , case 
            when dcle."ENTRY TYPE" = 1 then  dcle."DEBIT AMOUNT"
            end   POSTED_INVOICE
        , case 
            when dcle."ENTRY TYPE" = 2 then  dcle."CREDIT AMOUNT"
            end POSTED_PAYMENT
        , current_date()                    "ZAP_TIMESTAMP"
        , current_date()                    "ZAP_CREATEDTIME"
        , dcle."DEBIT AMOUNT (LCY)"          "AR INVOICE AMOUNT (LCY)"
        , dcle."DEBIT AMOUNT"                "AR INVOICE AMOUNT"
        , cle."CLOSED BY AMOUNT (LCY)"
        , cle."POSITIVE"
        , dcle."SOURCE CODE"

        --"DUE AGED DAYS"
        --"INVOICE AGED DAYS"
        --, "BUCKET ID BY DUE"
        --, "BUCKET ID BY INVOICE"
        , coalesce(  dcle."CURRENCY CODE" , cle."CURRENCY CODE" )    "CURRENCY CODE LCY"
        , current_date()                     INGEST_DATETIME
    from DATAWAREHOUSE.NAVCOMPANION.STAGE_DETAILED_CUSTOMER_LEDGER_ENTRY dcle
    left join DATAWAREHOUSE.NAVCOMPANION.STAGE_CUSTOMER_LEDGER_ENTRY  cle
        on   cle."COMPANYCODE"  = dcle."COMPANYCODE"
        and  cle."ENTRY NO_"  = dcle."CUST_ LEDGER ENTRY NO_" 
    where 1=1
        -- and dcle."ENTRY TYPE" = 1 -- get all invoiced
        -- and dcle."ENTRY TYPE" = 2 -- get all payments
        and coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE" )   = '20_02-SESC'
        AND coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_") = 'S-0000736-C'
        AND coalesce(  dcle."CUST_ LEDGER ENTRY NO_" , cle."ENTRY NO_") IN (107303)
                                                       
), mon_dt as (

    select
        dt.MONTH_YEAR_ABREV
        , max(dt.date_key)         AS DATE_MONTH_END
    from DATAWAREHOUSE.PUBLIC.LU_STATIC_RAW_CALENDAR dt
    where 1=1
        and dt.date_key >= to_date('2017-01-01')
        and dt.date_key <  current_date()
    group by 
        dt.MONTH_YEAR_ABREV

), grouped_ar as (

    select 
        "COMPANY"
        , "CUSTOMER NO."
        , "DOCUMENT NO."
        , "CUST. LEDGER ENTRY NO."
        , "POSTING DATE"
        , SUM(amount) as amount 
        , "OPEN" 
        , "CLOSED AT DATE"
        , "CLOSED BY AMOUNT"
        --, dcle."ENTRY TYPE"
        , "CURRENCY CODE"
        , "DOCUMENT DATE"
        , "INITIAL ENTRY DUE DATE"
        , SUM("AMOUNT (LCY)") as amount_lcy
        , current_date()                    "ZAP_TIMESTAMP"
        , current_date()                    "ZAP_CREATEDTIME"
        --, dcle."DEBIT AMOUNT (LCY)"          "AR INVOICE AMOUNT (LCY)"
        --, dcle."DEBIT AMOUNT"                "AR INVOICE AMOUNT"
        , "CLOSED BY AMOUNT (LCY)"
        , "POSITIVE"
        --, dcle."SOURCE CODE"

        --"DUE AGED DAYS"
        --"INVOICE AGED DAYS"
        --, "BUCKET ID BY DUE"
        --, "BUCKET ID BY INVOICE"
        , "CURRENCY CODE LCY"
        , current_date()                     INGEST_DATETIME
    from base_ar
    group by 
        "COMPANY"
        , "CUSTOMER NO."
        , "DOCUMENT NO."
        , "CUST. LEDGER ENTRY NO."
        , "POSTING DATE"
        , "OPEN" 
        , "CLOSED AT DATE"
        , "CLOSED BY AMOUNT"
        --, dcle."ENTRY TYPE"
        , "CURRENCY CODE"
        , "DOCUMENT DATE"
        , "INITIAL ENTRY DUE DATE"
        , current_date()                  
        , current_date()                    
        --, dcle."DEBIT AMOUNT (LCY)"          
        --, dcle."DEBIT AMOUNT"               
        , "CLOSED BY AMOUNT (LCY)"
        , "POSITIVE"
        --, dcle."SOURCE CODE"
        --"DUE AGED DAYS"
        --"INVOICE AGED DAYS"
        --, "BUCKET ID BY DUE"
        --, "BUCKET ID BY INVOICE"
        , "CURRENCY CODE LCY"
        , current_date()                     


)
  
    select 
        mon_dt.DATE_MONTH_END
        , sum(amount) OVER(PARTITION BY "COMPANY", "CUSTOMER NO.","CUST. LEDGER ENTRY NO." ORDER BY "POSTING DATE" )
        , grouped_ar.*
    from grouped_ar
    cross join mon_dt 
    where 1=1
        and  "POSTING DATE"     <= mon_dt.DATE_MONTH_END 
        --and  sub.CLOSE_POSTING_DATE >= mon_dt.DATE_MONTH_END 
        and "CLOSED AT DATE" > mon_dt.DATE_MONTH_END
    ORDER BY COMPANY, "CUSTOMER NO.",mon_dt.DATE_MONTH_END