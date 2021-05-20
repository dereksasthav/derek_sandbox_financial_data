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
    WHERE company = '20.02-SESC'
    GROUP BY COMPANY, DATEMONTHEND

), zap as (

    SELECT  
        COMPANY
        , DATEMONTHEND
        , COUNT(*) as numRecords_zap
        , SUM("AR MONTHEND BALANCE") as balance_zap
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE company = '20.02-SESC'
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
    gcprod FULL OUTER JOIN  zap 
    on gcprod.company = zap.company 
    AND gcprod.document_no = zap.document_no
    AND gcprod.DATEMONTHEND = zap.datemonthend
WHERE 1=1
    AND gcprod.document_no IS NULL or zap.document_no IS NULL
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

)


SELECT *
FROM fnl
WHERE DATE_MONTH_END = '2021-04-30'



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