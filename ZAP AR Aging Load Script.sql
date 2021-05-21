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
            and dt.date_key <  current_date()
        group by 
            dt.MONTH_YEAR_ABREV

    ), base_ar as (

        select
            coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE") "COMPANY"
            , coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_") "CUSTOMER NO."
            , coalesce(  dcle."DOCUMENT NO_"  , cle."DOCUMENT NO_") "DOCUMENT NO."
            , coalesce(  dcle."CUST_ LEDGER ENTRY NO_", cle."ENTRY NO_")    "CUST. LEDGER ENTRY NO."
            , coalesce(dcle."POSTING DATE" , cle."POSTING DATE"  )   "POSTING DATE"
            , SUM(dcle.amount) "AR MONTHEND BALANCE"
            , cle."OPEN" 
            , cle."CLOSED AT DATE"
            , cle."CLOSED BY AMOUNT"
            --, dcle."ENTRY TYPE"
            , coalesce(  dcle."CURRENCY CODE" , cle."CURRENCY CODE")  "CURRENCY CODE"
            , cle."DOCUMENT DATE"  "DOCUMENT DATE"
            , coalesce(  dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE" )  "INITIAL ENTRY DUE DATE"
            , SUM(dcle."AMOUNT (LCY)") as "AR MONTHEND BALANCE (LCY)" 
            --, case 
            --  when dcle."ENTRY TYPE" = 1 then  dcle."DEBIT AMOUNT"
                --end   POSTED_INVOICE
            --, case 
            --  when dcle."ENTRY TYPE" = 2 then  dcle."CREDIT AMOUNT"
            --  end POSTED_PAYMENT
            , current_date()                    "ZAP_TIMESTAMP"
            , current_date()                    "ZAP_CREATEDTIME"
            , SUM(dcle."DEBIT AMOUNT (LCY)") as "AR INVOICE AMOUNT (LCY)"
            , SUM(dcle."DEBIT AMOUNT") as "AR INVOICE AMOUNT"
            --, cle."CLOSED BY AMOUNT (LCY)"
            , cle."POSITIVE"
            --, dcle."SOURCE CODE"

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
        GROUP BY
            coalesce(  dcle."COMPANYCODE"  , cle."COMPANYCODE") 
            , coalesce(  dcle."CUSTOMER NO_"  , cle."CUSTOMER NO_")
            , coalesce(  dcle."DOCUMENT NO_"  , cle."DOCUMENT NO_") 
            , coalesce(  dcle."CUST_ LEDGER ENTRY NO_", cle."ENTRY NO_") 
            , coalesce(dcle."POSTING DATE" , cle."POSTING DATE"  )  
            , cle."OPEN" 
            , cle."CLOSED AT DATE"
            , cle."CLOSED BY AMOUNT"
            , coalesce(  dcle."CURRENCY CODE" , cle."CURRENCY CODE")
            , cle."DOCUMENT DATE"  
            , coalesce(  dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE" ) 
            , cle."POSITIVE"

    ), cartesian as (

        select 
            ROW_NUMBER() OVER(ORDER BY  DATE_MONTH_END, "COMPANY", "CUST. LEDGER ENTRY NO.") as SUMMARYKEY
            , m.DATE_MONTH_END as DATEMONTHEND
            , base_ar."CUST. LEDGER ENTRY NO."
            , base_ar.COMPANY
            , base_ar."CUSTOMER NO."
            , base_ar."DOCUMENT NO."
            , base_ar."CURRENCY CODE"
            , base_ar."POSTING DATE"
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
            (base_ar."POSTING DATE" <= m.DATE_MONTH_END
            --AND m.DATE_MONTH_END <> CAST(FLOOR(CAST(GETDATE() AS FLOAT)) AS DATETIME) -- is this needed?
            AND (base_ar."CLOSED AT DATE" >= m.DATE_MONTH_END OR base_ar."CLOSED AT DATE" = CAST('1753-01-01' AS DATETIME))) --OR [Closed at Date] IS NULL))
            OR 	
            (m.DATE_MONTH_END = GETDATE() AND base_ar."OPEN" = 1)
        ORDER BY m.DATE_MONTH_END
    ), final as (

        select ct.*
            , due_age."BUCKET ID" as "BUCKET ID BY DUE"
            , inv_age."BUCKET ID" as "BUCKET ID BY INVOICE"
            , current_date() as INGEST_DATETIME
        from cartesian ct
        LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY due_age
            on  ct."COMPANY"           =  due_age.COMPANY
            and ct."DUE AGED DAYS"     >= due_age."FROM"
            and ct."DUE AGED DAYS"     <= due_age."TO"
        LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_AGING_BUCKET_BY_COMPANY inv_age
            on ct."COMPANY"           =  inv_age.COMPANY
            and ct."INVOICE AGED DAYS" >= inv_age."FROM"
            and ct."INVOICE AGED DAYS" <= inv_age."TO" 


)
