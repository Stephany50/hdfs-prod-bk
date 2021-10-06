-- CA_Voix_Promo
INSERT INTO MON.SPARK_FT_BUSINESS_ACTIVITY_DD
(
SELECT 
    'DD_PMO_CA_VOIX_PROMO', b.zone_lib, sum(a.promo_rated_amount) promo, 
    'FT_GSM_SITE_REVENU_DAILY' , CURRENT_TIMESTAMP, transaction_date 
FROM ( 
    SELECT * FROM MON.SPARK_FT_GSM_SITE_REVENU_DAILY 
    WHERE TO_DATE(transaction_date) = '###SLICE_VALUE###'
)a
LEFT JOIN (select distinct zone_lib, site from DIM.DT_SITE_ZONE) b 
ON a.site_name = b.site
GROUP BY a.transaction_date,b.zone_lib
);