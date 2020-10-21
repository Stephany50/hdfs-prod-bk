INSERT INTO ft_business_activity_dd
    SELECT 
        'DD_PMO_PDM_INTERCO_MTN',zone_lib, sum(mtn)/sum(mtn+ocm_to_mtn), 
        'FT_PDM_SITE_DAILY', CURRENT_TIMESTAMP, event_date
    FROM MONDV.FT_PDM_SITE_DAILY a
    LEFT JOIN (select distinct zone_lib, site from mondv.dt_site_zone) b on a.site_name = b.site
    WHERE event_date IN 
    (
        SELECT datecode 
        FROM 
            (SELECT DISTINCT datecode FROM DIM.DT_DATES 
            WHERE datecode BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
            AND TRUNC(CURRENT_DATE- dashboard_kpi_day_bef)) c
        LEFT JOIN   
            (SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY_DD 
            WHERE USAGE_CODE='DD_PMO_PDM_INTERCO_MTN'
            AND TRANSACTION_DATE BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
            AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) d
        ON c.datecode = d.transaction_date
        WHERE d.transaction_date IS NULL
    )
    GROUP BY a.event_date,b.zone_lib;