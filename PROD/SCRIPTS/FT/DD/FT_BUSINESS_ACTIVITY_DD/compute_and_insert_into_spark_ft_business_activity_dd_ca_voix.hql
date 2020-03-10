--CA Voix
INSERT INTO FT_BUSINESS_ACTIVITY_DD
    SELECT 
        'DD_PMO_CA_VOIX_MAIN', b.zone_lib, sum(a.main_rated_amount) main_rated_amount, 
        'FT_GSM_SITE_REVENU_DAILY' , CURRENT_TIMESTAMP , transaction_date
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
            WHERE USAGE_CODE='DD_PMO_CA_VOIX_MAIN' AND TRANSACTION_DATE 
            BETWEEN trunc(CURRENT_DATE-dashboard_kpi_day_from) 
            AND trunc(CURRENT_DATE-dashboard_kpi_day_bef)) d
        ON c.datecode = d.transaction_date
        WHERE d.transaction_date IS NULL
    )
    GROUP BY a.transaction_date,b.zone_lib;