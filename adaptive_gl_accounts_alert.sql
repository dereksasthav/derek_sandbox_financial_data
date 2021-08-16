WITH adaptive_src AS (
    
    SELECT * FROM "DATAWAREHOUSE"."ADAPTIVE_INSIGHTS"."STAGE_LU_ADAPTIVE_INSIGHTS_ACCOUNT"

), bc_src AS (

    SELECT * FROM 

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
        , COALESCE(
            account_child5_name,
            account_child4_name,
            account_child3_name,
            account_child2_name,
            account_child1_name,
            account_name
        ) AS adaptive_account_name 
    FROM adaptive_src
    WHERE 1=1
        AND sheet like 'Balance Sheet'
        
)

SELECT *
FROM adaptive 

