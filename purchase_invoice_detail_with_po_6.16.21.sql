WITH purchase_invoice_detail AS (

    select
        pih."ZAP_COMPANY"                  ZAP_COMPANY
        , ci.NAME                            COMPANY_NAME
        , pih."NO."                          INVOICE_NUMBER
        , hidt.DOC_TYPE_LABEL                INVOICE_DOCUMENT_TYPE
        , pih."POSTING DATE"                 POSTING_DATE
        , pih."PAY-TO VENDOR NO."            PAY_TO_VENDOR_NO
        , vnd.NAME                           VENDOR_NAME
        , pih."VENDOR INVOICE NO_"           VENDOR_INVOICE_NUMBER
        , pih."DUE DATE"                     VENDOR_DUE_DATE
        , pih."ORDER NO_"                    PO_NBR
        , pih."POSTING DESCRIPTION"          INVOICE_HEADER_DESCRIPTION
        , pil."PURCHASE INVOICES"            PURHASE_INVOICE_LINE_NO
        , pil."QUANTITY"                     PURHASE_INVOICE_LINE_QUANTITY
        , pil."AMOUNT"                       PURHASE_INVOICE_LINE_AMOUNT
        , pil."NO."                          GL_ACCOUNT_CODE
        , acct.NAME                          GL_ACCOUNT_NAME
        , pil."SHORTCUT DIMENSION 1 CODE"    ENTITY_CODE
        , plent.LEGAL_ENTITY_NAME            LEGAL_ENTITY_NAME
        , pil."SHORTCUT DIMENSION 2 CODE"    ASSET_PROJECT_CODE
        , lups.SITE_NAME                     SITE_NAME
        , lups.ASSET_TYPE                    ASSET_TYPE
        , lups.PROJECT_STATUS                PROJECT_STATUS
        , pil."DESCRIPTION"                  INVOICE_LINE_DESCRIPTION
        , pil."ORDER NO_"                    ORIG_PO_NBR
        , pil."ORDER LINE NO_"               ORIG_PO_LINE_NBR
    from DATAWAREHOUSE.GC_PROD_WH.RAW_PURCHASE_INVOICE_HEADER  pih
    join DATAWAREHOUSE.GC_PROD_WH.RAW_PURCHASE_INVOICE_LINES  pil
        on  pih."ZAP_COMPANY"     = pil."COMPANY"
        and pih."NO."             = pil."DOCUMENT NO"
    join DATAWAREHOUSE.GC_PROD_WH.VW_LU_PURCHASE_HEADER_INVOICE_DOC_TYPE hidt
        on pih."APPLIES-TO DOC_ TYPE" = hidt.DOC_TYPE
    join DATAWAREHOUSE.GC_PROD_WH.RAW_COMPANY_INFORMATON ci
        on  pih."ZAP_COMPANY"         = ci.ZAP_COMPANY  -- 427
    LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_VENDOR vnd
        on  pih."PAY-TO VENDOR NO."   = vnd.VENDOR 
        and pih."ZAP_COMPANY"         = vnd.COMPANY       
    LEFT join DATAWAREHOUSE.GC_PROD_WH.RAW_ACCOUNT acct
        on  pil."NO."                 = acct.ACCOUNT 
        and pih."ZAP_COMPANY"         = acct.ZAP_COMPANY        
    LEFT join DATAWAREHOUSE.PUBLIC.LU_PROJECT_TO_SITE lups
        on  pil."SHORTCUT DIMENSION 2 CODE"  = lups.PROJECT_CODE   
    LEFT JOIN (
                select 
                sfdcpc.GC_PROJECT_CODE__C    JOIN_PROJECT_CODE
                , sfdcspv.NAME                 LEGAL_ENTITY_NAME
                from DATAWAREHOUSE.SALESFORCE.RAW_SALESFORCE_GC_PROJECT_CODE__C  sfdcpc
                left join DATAWAREHOUSE.SALESFORCE.RAW_SALESFORCE_GC_SPV__C sfdcspv
                on sfdcpc.GC_LEGAL_ENTITY__C = sfdcspv.ID 
            )  plent
        on pil."SHORTCUT DIMENSION 2 CODE"  = plent.JOIN_PROJECT_CODE
    where 1=1 
        --and pih."PAY-TO VENDOR NO." not like 'EMP%'
        --and pih."NO." not like 'PCM%'
        --and pih."ORDER NO_" not like 'PCM%'

), grouped_by_invoice AS (

    SELECT 
        ZAP_COMPANY as COMPANYCODE
        , LAST_DAY(POSTING_DATE, 'month') as POSTING_MONTH
        , INVOICE_NUMBER 
        , PO_NBR
        , SUM(PURHASE_INVOICE_LINE_AMOUNT) AS AMOUNT 
    FROM purchase_invoice_detail
    GROUP BY 1,2,3,4

), totals AS (

    SELECT 
       COMPANYCODE
        , POSTING_MONTH
        , COUNT(INVOICE_NUMBER) as total_number_of_invoices
        , SUM(CASE WHEN PO_NBR IS NOT NULL THEN 1 ELSE 0 END) as number_of_invoices_with_po
        , SUM(AMOUNT) as total_amount
        , SUM(CASE WHEN PO_NBR IS NOT NULL THEN AMOUNT ELSE 0 END) as amount_with_po
    FROM grouped_by_invoice
    GROUP BY 1,2


), final AS (

    SELECT *
        , DIV0(number_of_invoices_with_po, total_number_of_invoices) as percentage_of_invoices_with_po
        , DIV0(amount_with_po, total_amount) as percentage_of_amount_with_po
    FROM totals 

)

SELECT *
FROM final 
ORDER BY COMPANYCODE, POSTING_MONTH

