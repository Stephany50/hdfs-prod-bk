--Mobilité SIM
INSERT INTO FT_BUSINESS_ACTIVITY_DD
    SELECT 
        'DD_PMO_MOB_SIM', zone_lib, mob_sim ,
        'ACTIV_IDENTIF_DAILY|FT_IDENTIFICATION_DAILY', 
        CURRENT_TIMESTAMP, event_date
    FROM 
        (
            SELECT 
                'DD_PMO_MOB_SIM', zone_lib, 
                count(msisdn_identificateur) mob_sim ,
                'ACTIV_IDENTIF_DAILY|FT_IDENTIFICATION_DAILY', event_date
            FROM
                (
                    (select event_date, msisdn_identificateur
                    from mondv.activ_identif_daily where event_date = d_start_date and nbre_actives >0) a
                    left join 
                        (select msisdn, site_name from ft_client_last_site_day where event_date = d_start_date) b
                    on a.msisdn_identificateur = b.msisdn
                    left join 
                        (select distinct site, zone_lib from mondv.dt_site_zone) t 
                    on b.site_name = t.site  
                ) 
            GROUP BY event_date, zone_lib
        )
    WHERE 
        event_date IN 
        ( 
            SELECT datecode 
            FROM 
                (SELECT DISTINCT datecode FROM DIM.DT_DATES 
                WHERE datecode BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
                AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) c
            LEFT JOIN 
                (SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY_DD 
                WHERE USAGE_CODE='DD_PMO_MOB_SIM'
                AND TRANSACTION_DATE BETWEEN trunc(CURRENT_DATE-dashboard_kpi_day_from) 
                AND trunc(CURRENT_DATE-dashboard_kpi_day_bef)) d
            ON c.datecode = d.transaction_date
            WHERE 
                d.transaction_date IS NULL
        );