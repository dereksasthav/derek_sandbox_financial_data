WITH adaptive_src AS (
    
    SELECT * FROM "DATAWAREHOUSE"."ADAPTIVE_INSIGHTS"."RAW_LU_ADAPTIVE_INSIGHTS_ACCOUNT"

), bs_src AS (

    SELECT * FROM "DATAWAREHOUSE"."FPA_TO_ADAPTIVE"."VRAW_NAV_BALANCEREPORTSOURCE"

), is_src AS (

    SELECT * FROM "DATAWAREHOUSE"."FPA_TO_ADAPTIVE"."VRAW_NAV_INCOMEREPORTSOURCE"

), actual_accounts AS (

    SELECT DISTINCT companycode, account, periodenddate
    FROM bs_src

    UNION

    SELECT DISTINCT companycode, account, periodenddate
    FROM is_src 

), adaptive AS (

    SELECT sheet
        , code
        , description
        , name
        , account_code
        , account_name
        , account_child1_name
        , account_child2_name
        , account_child3_name
        , account_child4_name
        , account_child5_name
        , account_child6_name 
        , COALESCE(
            account_child6_name,
            account_child5_name,
            account_child4_name,
            account_child3_name,
            account_child2_name,
            account_child1_name,
            account_name
        ) AS adaptive_account_name
        , TRIM(SPLIT_PART(COALESCE(
            account_child6_name,
            account_child5_name,
            account_child4_name,
            account_child3_name,
            account_child2_name,
            account_child1_name,
            account_name
        ),' - ',0)) AS adaptive_account_code 
    FROM adaptive_src
    WHERE 1=1
        
)

SELECT DISTINCT
    actual_accounts.companycode
    , actual_accounts.account 
    , LAST_DAY(actual_accounts.periodenddate,'month') as periodenddate 
FROM actual_accounts
LEFT JOIN adaptive 
    ON actual_accounts.account = adaptive_account_code
WHERE 1=1
    AND adaptive.adaptive_account_code IS NULL
    AND periodenddate = '2021-07-31'
ORDER BY periodenddate DESC, actual_accounts.companycode, actual_accounts.account 