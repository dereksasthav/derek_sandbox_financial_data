SELECT (ROW_NUMBER() OVER (ORDER BY DateMonthEnd, [Company], [Cust. Ledger Entry No.])) AS SummaryKey, AR_MONTHLY.*
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
    (SELECT [Cust. Ledger Entry No.], a.[Company], [Customer No.], [CustomerLedgerEntry - Document No.] AS [Document No.]
    , g.[LCY Code] AS [Currency Code LCY], [CustomerLedgerEntry - Posting Date] AS [Posting Date]
    , CASE WHEN [Currency Code] = '' THEN g.[LCY Code] ELSE [Currency Code] END AS [Currency Code]
    , [Document Date], 
    (CASE WHEN [Initial Entry Due Date] = CAST('1753-01-01' AS DATETIME) --OR [Initial Entry Due Date] IS NULL 
    THEN (SELECT TOP 1 [Due Date] FROM $SalesInvoiceHeader$ H 
          WHERE H.[Zap_Company] = a.[Company] AND H.[No.] = a.[CustomerLedgerEntry - Document No.]) 
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