--CA Data
INSERT INTO MON.SPARK_FT_BUSINESS_ACTIVITY_DD
(
    SELECT 
        'DD_PMO_CA_DATA',  b.zone_lib, sum(a.MAIN_COST) main_cost, 
        'FT_GPRS_SITE_REVENU_DAILY', CURRENT_TIMESTAMP, EVENT_DATE
    FROM (  
        SELECT * FROM MON.SPARK_FT_GPRS_SITE_REVENU_DAILY
        WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###'
    ) a
    LEFT JOIN (select distinct zone_lib, site from DIM.DT_SITE_ZONE) b 
    ON a.site_name = b.site
    GROUP BY a.EVENT_DATE,b.zone_lib
);