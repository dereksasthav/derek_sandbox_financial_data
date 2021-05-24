TRUNCATE TABLE DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST2 ;
MERGE INTO DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNTS_RECEIVABLE_MONTHLY_TEST2 as TARGET 
USING(
    with mon_dt as (
        select
            dt.MONTH_YEAR_ABREV
            , max(dt.date_key)         AS DATE_MONTH_END
        from DATAWAREHOUSE.PUBLIC.LU_STATIC_RAW_CALENDAR dt
        where 1=1
            and dt.date_key >= to_date('2017-01-01')
            and dt.date_key < current_date()
        group by 
            dt.MONTH_YEAR_ABREV
    ), base_ar as (
        select
             coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE") "COMPANY"
            , coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_") "CUSTOMER NO."
            , coalesce(  dcle."DOCUMENT NO_"  , cle."DOCUMENT NO_") "DOCUMENT NO."
            , coalesce(  dcle."CUST_ LEDGER ENTRY NO_", cle."ENTRY NO_")    "CUST. LEDGER ENTRY NO."
            --, coalesce(dcle."POSTING DATE" , cle."POSTING DATE"  )   "POSTING DATE"
            , dcle."POSTING DATE" as "DETAIL POSTING DATE"
            , cle."POSTING DATE" as "HEADER POSTING DATE"
            , SUM(dcle.amount) "AR MONTHEND BALANCE"
            , cle."OPEN" 
            , cle."CLOSED AT DATE"
            , cle."CLOSED BY AMOUNT"
            --, dcle."ENTRY TYPE"
            , coalesce( sih."CURRENCY CODE", dcle."CURRENCY CODE", cle."CURRENCY CODE", gls."LCY CODE" )  "CURRENCY CODE"
            , cle."DOCUMENT DATE"  "DOCUMENT DATE"
            , coalesce( sih."DUE DATE", dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE")  "INITIAL ENTRY DUE DATE"
            , SUM(dcle."AMOUNT (LCY)") as "AR MONTHEND BALANCE (LCY)" 
            , current_date()                    "ZAP_TIMESTAMP"
            , current_date()                    "ZAP_CREATEDTIME"
            , SUM(dcle."DEBIT AMOUNT (LCY)") as "AR INVOICE AMOUNT (LCY)"
            , SUM(dcle."DEBIT AMOUNT") as "AR INVOICE AMOUNT"
            --, cle."CLOSED BY AMOUNT (LCY)"
            , cle."POSITIVE"
            , coalesce( sih."CURRENCY CODE", dcle."CURRENCY CODE", cle."CURRENCY CODE", gls."LCY CODE" )   "CURRENCY CODE LCY"
            , current_date()                     INGEST_DATETIME
        from DATAWAREHOUSE.NAVCOMPANION.STAGE_DETAILED_CUSTOMER_LEDGER_ENTRY dcle
        left join DATAWAREHOUSE.NAVCOMPANION.STAGE_CUSTOMER_LEDGER_ENTRY  cle
            on   cle."COMPANYCODE"  = dcle."COMPANYCODE"
            and  cle."ENTRY NO_"  = dcle."CUST_ LEDGER ENTRY NO_" 
        left join DATAWAREHOUSE.GC_PROD_WH.RAW_GENERAL_LEDGER_SETUP gls
            on   cle."COMPANYCODE"  = gls.COMPANYCODE 
        left join DATAWAREHOUSE.GC_PROD_WH.RAW_SALES_INVOICE_HEADER sih
            on   cle."COMPANYCODE"  = sih.ZAP_COMPANY
            and  cle."ENTRY NO_"    = sih."NO."
        where 1=1
            --and coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE" )   = '20_02-SESC'
            --AND coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_") = 'S-0000736-C'
            --AND coalesce(  dcle."CUST_ LEDGER ENTRY NO_" , cle."ENTRY NO_") IN (107303)
        GROUP BY
            coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE") 
            , coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_")
            , coalesce(  dcle."DOCUMENT NO_"  , cle."DOCUMENT NO_") 
            , coalesce(  dcle."CUST_ LEDGER ENTRY NO_", cle."ENTRY NO_") 
            --, coalesce(dcle."POSTING DATE" , cle."POSTING DATE"  )  
            , dcle."POSTING DATE" 
            , cle."POSTING DATE" 
            , cle."OPEN" 
            , cle."CLOSED AT DATE"
            , cle."CLOSED BY AMOUNT"
            , coalesce( sih."CURRENCY CODE", dcle."CURRENCY CODE", cle."CURRENCY CODE", gls."LCY CODE" ) 
            , cle."DOCUMENT DATE"  
            , coalesce(  sih."DUE DATE", dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE" ) 
            , cle."POSITIVE"
        HAVING SUM(dcle.amount) <> 0
    ), cartesian as (
        select 
            m.DATE_MONTH_END as DATEMONTHEND
            , base_ar."CUST. LEDGER ENTRY NO."
            , base_ar.COMPANY
            , base_ar."CUSTOMER NO."
            , base_ar."DOCUMENT NO."
            , base_ar."CURRENCY CODE"
            , base_ar."DETAIL POSTING DATE"
            , base_ar."HEADER POSTING DATE"
            , base_ar."CURRENCY CODE LCY"
            , base_ar."DOCUMENT DATE"
            , base_ar."INITIAL ENTRY DUE DATE"
            , base_ar."AR INVOICE AMOUNT (LCY)"
            , base_ar."AR INVOICE AMOUNT"
            , base_ar."AR MONTHEND BALANCE (LCY)"
            , base_ar."AR MONTHEND BALANCE"
            , DATEDIFF(d, m.DATE_MONTH_END, base_ar."INITIAL ENTRY DUE DATE") as "DUE AGED DAYS"
            , DATEDIFF(d, m.DATE_MONTH_END, base_ar."DOCUMENT DATE") as "INVOICE AGED DAYS"
        from base_ar
        CROSS JOIN mon_dt m
        WHERE 
            (base_ar."DETAIL POSTING DATE" <= m.DATE_MONTH_END
            --AND m.DATE_MONTH_END <> CAST(FLOOR(CAST(GETDATE() AS FLOAT)) AS DATETIME) -- is this needed?
            AND (base_ar."CLOSED AT DATE" >= m.DATE_MONTH_END OR base_ar."CLOSED AT DATE" = CAST('1753-01-01' AS DATETIME))) --OR [Closed at Date] IS NULL))
            OR  
            (m.DATE_MONTH_END = GETDATE() AND base_ar."OPEN" = 1)
    ), final as (
        /** add one more grouping to regroup to header record and remove detail, remove records with header open amount of 0 **/
        select 
            ROW_NUMBER() OVER(ORDER BY  ct.DATEMONTHEND, ct."COMPANY", ct."CUST. LEDGER ENTRY NO.") as SUMMARYKEY
            , ct.DATEMONTHEND
            , ct."CUST. LEDGER ENTRY NO."
            , ct.COMPANY
            , ct."CUSTOMER NO."
            , ct."DOCUMENT NO."
            , ct."CURRENCY CODE"
            --, ct."DETAIL POSTING DATE"
            , ct."HEADER POSTING DATE" as "POSTING DATE"
            , ct."CURRENCY CODE LCY"
            , ct."DOCUMENT DATE"
            , ct."INITIAL ENTRY DUE DATE"
            , SUM(ct."AR INVOICE AMOUNT (LCY)") as "AR INVOICE AMOUNT (LCY)"
            , SUM(ct."AR INVOICE AMOUNT") as "AR INVOICE AMOUNT"
            , SUM(ct."AR MONTHEND BALANCE (LCY)") as "AR MONTHEND BALANCE (LCY)"
            , SUM(ct."AR MONTHEND BALANCE") as "AR MONTHEND BALANCE"
            , ct."DUE AGED DAYS"
            , ct."INVOICE AGED DAYS"
            , due_age."BUCKET ID" as "BUCKET ID BY DUE"
            , inv_age."BUCKET ID" as "BUCKET ID BY INVOICE"
            , current_date() as INGEST_DATETIME
        from cartesian ct
        LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY due_age
            on  replace(ct."COMPANY",'_','.') =  replace(due_age.COMPANY,'_','.')
            and ct."DUE AGED DAYS"     >= due_age."FROM"
            and ct."DUE AGED DAYS"     <= due_age."TO"
        LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY inv_age
            on replace(ct."COMPANY",'_','.') =  replace(inv_age.COMPANY,'_','.')
            and ct."INVOICE AGED DAYS" >= inv_age."FROM"
            and ct."INVOICE AGED DAYS" <= inv_age."TO" 
        GROUP BY 
           ct.DATEMONTHEND
            , ct."CUST. LEDGER ENTRY NO."
            , ct.COMPANY
            , ct."CUSTOMER NO."
            , ct."DOCUMENT NO."
            , ct."CURRENCY CODE"
            --, ct."DETAIL POSTING DATE"
            , ct."HEADER POSTING DATE"
            , ct."CURRENCY CODE LCY"
            , ct."DOCUMENT DATE"
            , ct."INITIAL ENTRY DUE DATE"
            , ct."DUE AGED DAYS"
            , ct."INVOICE AGED DAYS"
            , due_age."BUCKET ID" 
            , inv_age."BUCKET ID"
            , current_date()
        HAVING SUM(ct."AR MONTHEND BALANCE") <> 0
    )
        SELECT *
        FROM final 
) as src
ON target.summarykey = src.summarykey
    when matched 
        then update 
            set "INGEST_DATETIME"                  = src."INGEST_DATETIME"
                , DATEMONTHEND                     = src.DATEMONTHEND
                , "CUST. LEDGER ENTRY NO."         = src."CUST. LEDGER ENTRY NO."
                , "COMPANY"                        = src."COMPANY"
                , "CUSTOMER NO."                   = src."CUSTOMER NO."
                , "DOCUMENT NO."                   = src."DOCUMENT NO."
                , "CURRENCY CODE"                  = src."CURRENCY CODE"
                , "POSTING DATE"                   = src."POSTING DATE"
                , "CURRENCY CODE LCY"              = src."CURRENCY CODE LCY"
                , "DOCUMENT DATE"                  = src."DOCUMENT DATE"
                , "INITIAL ENTRY DUE DATE"         = src."INITIAL ENTRY DUE DATE"
                , "AR INVOICE AMOUNT (LCY)"        = src."AR INVOICE AMOUNT (LCY)"
                , "AR INVOICE AMOUNT"              = src."AR INVOICE AMOUNT"
                , "AR MONTHEND BALANCE (LCY)"      = src."AR MONTHEND BALANCE (LCY)"
                , "AR MONTHEND BALANCE"            = src."AR MONTHEND BALANCE"
                , "DUE AGED DAYS"                  = src."DUE AGED DAYS"
                , "INVOICE AGED DAYS"              = src."INVOICE AGED DAYS"
                , "BUCKET ID BY DUE"               = src."BUCKET ID BY DUE"
                , "BUCKET ID BY INVOICE"           = src."BUCKET ID BY INVOICE"
    when not matched 
     then insert 
     (
              SUMMARYKEY
            , DATEMONTHEND
            , "CUST. LEDGER ENTRY NO."
            , "COMPANY"
            , "CUSTOMER NO."
            , "DOCUMENT NO."
            , "CURRENCY CODE"
            , "POSTING DATE"
            , "CURRENCY CODE LCY"
            , "DOCUMENT DATE"
            , "INITIAL ENTRY DUE DATE"
            , "AR INVOICE AMOUNT (LCY)"
            , "AR INVOICE AMOUNT"
            , "AR MONTHEND BALANCE (LCY)"
            , "AR MONTHEND BALANCE"
            , "DUE AGED DAYS"
            , "INVOICE AGED DAYS"
            , "BUCKET ID BY DUE"
            , "BUCKET ID BY INVOICE"
            , INGEST_DATETIME
     )
     values 
     (  
              src.SUMMARYKEY
            , src.DATEMONTHEND
            , src."CUST. LEDGER ENTRY NO."
            , src."COMPANY"
            , src."CUSTOMER NO."
            , src."DOCUMENT NO."
            , src."CURRENCY CODE"
            , src."POSTING DATE"
            , src."CURRENCY CODE LCY"
            , src."DOCUMENT DATE"
            , src."INITIAL ENTRY DUE DATE"
            , src."AR INVOICE AMOUNT (LCY)"
            , src."AR INVOICE AMOUNT"
            , src."AR MONTHEND BALANCE (LCY)"
            , src."AR MONTHEND BALANCE"
            , src."DUE AGED DAYS"
            , src."INVOICE AGED DAYS"
            , src."BUCKET ID BY DUE"
            , src."BUCKET ID BY INVOICE"
            , src.INGEST_DATETIME
     )
; 