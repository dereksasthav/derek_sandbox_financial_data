SELECT 
    (ROW_NUMBER() OVER (ORDER BY DateMonthEnd, [Company], [Cust. Ledger Entry No.])) AS SummaryKey
    , AR_MONTHLY.*
    , (SELECT TOP 1 [Bucket ID] FROM dbo.[Aging Bucket by Company] A1 
        WHERE A1.[Bucket Type] = 1 AND A1.[Company] = AR_MONTHLY.[Company] AND AR_MONTHLY.[Due Aged Days] BETWEEN A1.[From] AND A1.[To]
        ) AS [Bucket ID by Due]
    , (SELECT TOP 1 [Bucket ID] FROM dbo.[Aging Bucket by Company] B1 
        WHERE B1.[Bucket Type] = 1 AND B1.[Company] = AR_MONTHLY.[Company] AND AR_MONTHLY.[Invoice Aged Days] BETWEEN B1.[From] AND B1.[To]
        ) AS [Bucket ID by Invoice]
, CAST('2010-01-01' AS DATETIME) AS Zap_Timestamp, CAST('2010-01-01' AS DATETIME) AS Zap_CreatedTime 
FROM
    (SELECT m.DateMonthEnd, DCLE.*
        , datediff(d, m.DateMonthEnd, [Initial Entry Due Date]) AS [Due Aged Days]
        , datediff(d, m.DateMonthEnd, [Document Date]) AS [Invoice Aged Days]
    FROM 
        (SELECT [Calendar YQMD Month], MAX([Date]) AS DateMonthEnd
        FROM $Date$ WHERE [Date] BETWEEN '#APARHistoricalStart#' AND GETDATE()
        GROUP BY [Calendar YQMD Month]) m 

    CROSS APPLY 
    
    (SELECT 
        [Cust. Ledger Entry No.]
        , a.[Company]
        , [Customer No.]
        , [CustomerLedgerEntry - Document No.] AS [Document No.]
        , g.[LCY Code] AS [Currency Code LCY]
        , [CustomerLedgerEntry - Posting Date] AS [Posting Date]
        , CASE WHEN [Currency Code] = '' THEN g.[LCY Code] ELSE [Currency Code] END AS [Currency Code]
        , [Document Date], 
        (CASE WHEN [Initial Entry Due Date] = CAST('1753-01-01' AS DATETIME) --OR [Initial Entry Due Date] IS NULL 
            THEN (SELECT TOP 1 [Due Date] 
                    FROM $SalesInvoiceHeader$ H 
                    WHERE H.[Zap_Company] = a.[Company] 
                    AND H.[No.] = a.[CustomerLedgerEntry - Document No.]) 
            ELSE [Initial Entry Due Date] END) AS [Initial Entry Due Date]
        , SUM([Debit Amnt]) AS [AR Invoice Amount (LCY)], SUM([Debit Amount]) AS [AR Invoice Amount]
        , SUM([AR Amnt]) AS [AR Monthend Balance (LCY)], SUM([Amount]) AS [AR Monthend Balance] 
    FROM $AccountsReceivable$ a LEFT OUTER JOIN $GLSetup$ g ON a.[Company] = g.[Company]
    WHERE (
    	([Posting Date] <= m.DateMonthEnd 
    	AND m.DateMonthEnd <> CAST(FLOOR(CAST(GETDATE() AS FLOAT)) AS DATETIME) 
    	AND ([Closed at Date] >= m.DateMonthEnd OR [Closed at Date] = CAST('1753-01-01' AS DATETIME))) --OR [Closed at Date] IS NULL))
    	OR 	
    	(m.DateMonthEnd = CAST(FLOOR(CAST(GETDATE() AS FLOAT)) AS DATETIME) AND [Open] = 1)
    )
    GROUP BY [Cust. Ledger Entry No.], a.[Company], [Customer No.], [CustomerLedgerEntry - Document No.]
    , [Currency Code], [CustomerLedgerEntry - Posting Date]
    , [Document Date], [Initial Entry Due Date], g.[LCY Code]
    HAVING SUM(Amount) <> 0) DCLE


;



/** Derek attempt to recreate ZAP AR Aging**/
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
        , SUM(dcle.amount) amount 
        , cle."OPEN" 
        , cle."CLOSED AT DATE"
        , cle."CLOSED BY AMOUNT"
        --, dcle."ENTRY TYPE"
        , coalesce(  dcle."CURRENCY CODE" , cle."CURRENCY CODE")  "CURRENCY CODE"
        , cle."DOCUMENT DATE"  "DOCUMENT DATE"
        , coalesce(  dcle."INITIAL ENTRY DUE DATE" , cle."DUE DATE" )  "INITIAL ENTRY DUE DATE"
        , SUM(dcle."AMOUNT (LCY)") as "AMOUNT (LCY)" 
        --, case 
          --  when dcle."ENTRY TYPE" = 1 then  dcle."DEBIT AMOUNT"
            --end   POSTED_INVOICE
        --, case 
          --  when dcle."ENTRY TYPE" = 2 then  dcle."CREDIT AMOUNT"
          --  end POSTED_PAYMENT
        , current_date()                    "ZAP_TIMESTAMP"
        , current_date()                    "ZAP_CREATEDTIME"
        --, dcle."DEBIT AMOUNT (LCY)"          "AR INVOICE AMOUNT (LCY)"
        --, dcle."DEBIT AMOUNT"                "AR INVOICE AMOUNT"
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

), final as (

    select m.DATE_MONTH_END, base_ar.*
    from base_ar
    CROSS JOIN mon_dt m
    WHERE 
        (base_ar."POSTING DATE" <= m.DATE_MONTH_END
        --AND m.DATE_MONTH_END <> CAST(FLOOR(CAST(GETDATE() AS FLOAT)) AS DATETIME) -- is this needed?
        AND (base_ar."CLOSED AT DATE" >= m.DATE_MONTH_END OR base_ar."CLOSED AT DATE" = CAST('1753-01-01' AS DATETIME))) --OR [Closed at Date] IS NULL))
        OR 	
        (m.DATE_MONTH_END = GETDATE() AND base_ar."OPEN" = 1)
    ORDER BY m.DATE_MONTH_END

), zap as (

    SELECT *
    FROM DATAWAREHOUSE.ZAP_BIZ_CENTRAL.STAGE_ACCOUNTS_RECEIVABLE_MONTHLY
    WHERE 1=1 
        AND COMPANY = '20.02-SESC'
        AND "CUSTOMER NO." = 'S-0000736-C'
        AND "CUST. LEDGER ENTRY NO." IN (107303)

), zap_grouped as (

    SELECT 
        DATEMONTHEND as date_month_end
        , COMPANY
        , "CUSTOMER NO."
        , SUM("AR MONTHEND BALANCE") as "AMOUNT_ZAP" 
    FROM zap  
    GROUP BY
        DATE_MONTH_END
        , COMPANY
        , "CUSTOMER NO."

), final_grouped as (

    select 
        date_month_end
        , replace(company,'_','.') as company 
        , "CUSTOMER NO."
        --, "CUST. LEDGER ENTRY NO."
        , SUM(amount) as AMOUNT_GCPROD
     from final 
     group by 
        date_month_end
        , company
        , "CUSTOMER NO."
        --, "CUST. LEDGER ENTRY NO."
    
)

    SELECT 
        final_grouped.date_month_end as date_month_end_gcprod
        , final_grouped.company as company_gcprod
        , final_grouped."CUSTOMER NO." as customer_no_gcprod 
        , zap_grouped.date_month_end as date_month_end_zap
        , zap_grouped.company as company_zap
        , zap_grouped."CUSTOMER NO." as customer_no_zap 
        , final_grouped.AMOUNT_GCPROD
        , zap_grouped.AMOUNT_ZAP
        , ROUND(IFNULL(final_grouped.AMOUNT_GCPROD,0) - IFNULL(zap_grouped.AMOUNT_ZAP,0)) as variance 
    FROM final_grouped FULL OUTER JOIN zap_grouped
        ON final_grouped.DATE_MONTH_END = zap_grouped.DATE_MONTH_END
        AND final_grouped.company  = zap_grouped.Company
        AND final_grouped."CUSTOMER NO." = zap_grouped."CUSTOMER NO."
    WHERE 1=1 
        AND coalesce(final_grouped.date_month_end, zap_grouped.date_month_end) <= '2021-04-30'
        AND ROUND(IFNULL(final_grouped.AMOUNT_GCPROD,0) - IFNULL(zap_grouped.AMOUNT_ZAP,0))>1
    ORDER BY zap_grouped.date_month_end

