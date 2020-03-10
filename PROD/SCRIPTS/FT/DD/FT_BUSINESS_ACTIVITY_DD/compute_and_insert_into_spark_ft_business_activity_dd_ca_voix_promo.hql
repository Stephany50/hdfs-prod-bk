-- CA_Voix_Promo
INSERT INTO FT_BUSINESS_ACTIVITY_DD
    SELECT 'DD_PMO_CA_VOIX_PROMO', b.zone_lib, sum(a.promo_rated_amount) promo, 
        'FT_GSM_SITE_REVENU_DAILY' , CURRENT_TIMESTAMP, transaction_date 
    FROM MONDV.FT_GSM_SITE_REVENU_DAILY a
    LEFT JOIN (select distinct zone_lib, site from mondv.dt_site_zone) b on a.site_name = b.site
    WHERE transaction_date IN 
    ( 	
        SELECT datecode 
        FROM 
        (SELECT DISTINCT datecode FROM DIM.DT_DATES 
            WHERE datecode BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
            AND TRUNC(CURRENT_DATE- dashboard_kpi_day_bef)) c
        LEFT JOIN 
        (SELECT DISTINCT transaction_date FROM FT_BUSINESS_ACTIVITY_DD 
        WHERE USAGE_CODE='DD_PMO_CA_VOIX_PROMO' AND TRANSACTION_DATE 
        BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) d
        ON c.datecode = d.transaction_date
        WHERE d.transaction_date IS NULL
    )
    GROUP BY a.transaction_date,b.zone_lib;