/** 
Author: Derek Sasthav
Purpose: recreate Louis's weekly waste ops dashboard, specifically the feedstock visuals, to help with performance of the report
**/

WITH src AS (

    SELECT * FROM "DATAWAREHOUSE"."LOUIS_SANDBOX"."vw_wtv_equipment_pivot_v2"

), liquid_waste_volume_received_t AS (

    SELECT 
        'Liquid Waste Volume Received (T)' as FEEDSTOCK_METRIC
        , SITE_NAME
        , TO_DATE(RECORD_DATE_LOCAL) AS RECORD_DATE_LOCAL
        , SUM(CASE WHEN EQUIPMENT_GROUP = 'Feedstock Reception Liquid Volume' THEN UNIT_VALUE ELSE 0 END) - SUM(CASE WHEN EQUIPMENT_GROUP = 'Manure Volume' THEN UNIT_VALUE ELSE 0 END) AS UNIT_VALUE
    FROM src 
    WHERE 1=1
        AND EQUIPMENT_GROUP IN ('Feedstock Reception Liquid Volume', 'Manure Volume')
    GROUP BY 1,2,3

), fog_waste_volume_received_t AS (

    SELECT 
        'FOG Waste Volume Received (T)' as FEEDSTOCK_METRIC
        , SITE_NAME
        , TO_DATE(RECORD_DATE_LOCAL) AS RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE) AS UNIT_VALUE
    FROM src 
    WHERE 1=1
        AND EQUIPMENT_GROUP IN ('Feedstock Reception FOG')
    GROUP BY 1,2,3


), solid_waste_volume_received_t AS (

    SELECT 
        'Solid Waste Volume Received (T)' as FEEDSTOCK_METRIC
        , SITE_NAME
        , TO_DATE(RECORD_DATE_LOCAL) AS RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE) AS UNIT_VALUE
    FROM src 
    WHERE 1=1
        AND EQUIPMENT_GROUP IN ('Feedstock Reception Solid')
    GROUP BY 1,2,3


), sso_waste_volume_received_t AS (

    SELECT 
        'SSO Waste Volume Received (T)' as FEEDSTOCK_METRIC
        , SITE_NAME
        , TO_DATE(RECORD_DATE_LOCAL) AS RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE) AS UNIT_VALUE
    FROM src 
    WHERE 1=1
        AND CONVERSION_COLUMN = 'SSO_VALUE'
    GROUP BY 1,2,3


), packaged_waste_volume_received_t AS (

    SELECT 
        'Packaged Waste Volume Received (T)' as FEEDSTOCK_METRIC
        , SITE_NAME
        , TO_DATE(RECORD_DATE_LOCAL) AS RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE) AS UNIT_VALUE
    FROM src 
    WHERE 1=1
        AND CONVERSION_COLUMN = 'PACKAGED_VALUE'
    GROUP BY 1,2,3


), manure_volume_received_t AS (

    SELECT 
        'Manure Volume Received (T)' as FEEDSTOCK_METRIC
        , SITE_NAME
        , TO_DATE(RECORD_DATE_LOCAL) AS RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE) AS UNIT_VALUE
    FROM src 
    WHERE 1=1
        AND EQUIPMENT_GROUP IN ('Feedstock Reception Manure')
    GROUP BY 1,2,3


), unioned_data AS (

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM liquid_waste_volume_received_t
    
    UNION ALL 

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM fog_waste_volume_received_t

    UNION ALL 

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM solid_waste_volume_received_t

    UNION ALL 

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM sso_waste_volume_received_t

    UNION ALL 
    
    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM packaged_waste_volume_received_t

    UNION ALL 

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM manure_volume_received_t


), feedstock_reception_g AS (

    SELECT 
        'Feedstock Reception (Gallons)' AS FEEDSTOCK_METRIC
        , SITE_NAME 
        , RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE)*240 AS UNIT_VALUE
    FROM unioned_data
    GROUP BY 1,2,3


), feedstock_reception_t AS (

    SELECT 
        'Feedstock Reception (Tons)' AS FEEDSTOCK_METRIC
        , SITE_NAME 
        , RECORD_DATE_LOCAL
        , SUM(UNIT_VALUE) as UNIT_VALUE
    FROM unioned_data
    GROUP BY 1,2,3

), final_volume AS (

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM unioned_data

    UNION ALL 

    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM feedstock_reception_g

    UNION ALL 
    
    SELECT FEEDSTOCK_METRIC, SITE_NAME, RECORD_DATE_LOCAL, UNIT_VALUE
    FROM feedstock_reception_t

)


SELECT * 
FROM final_volume