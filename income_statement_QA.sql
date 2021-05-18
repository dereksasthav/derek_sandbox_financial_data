/** Purpose of this query is to take the DATAWAREHOUSE.GC_PROD_WH.RAW_API_CHART_OF_ACCOUNTS_REPORT
table which is created by Abraham Tom and is a daily archive, and convert this into a balance by company by account
that is usable for comparison to NAV and other reports **/

SELECT *
FROM DATAWAREHOUSE.GC_PROD_WH.RAW_API_CHART_OF_ACCOUNTS_REPORT
WHERE 1=1
    AND payloaddata_income_balance = 'Income Statement'
    AND payloaddata_account_type = 'Posting' 
LIMIT 10