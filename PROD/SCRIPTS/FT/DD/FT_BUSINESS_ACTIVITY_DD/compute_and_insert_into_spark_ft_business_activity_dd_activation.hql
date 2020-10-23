--Activation
INSERT INTO FT_BUSINESS_ACTIVITY_DD
    SELECT 'DD_PMO_ACTIV',nom_zone,sum(1) count_activation, 'FT_ACTIVATIONS_DAILY_DRAFT', CURRENT_TIMESTAMP,transaction_date
    FROM MON.SPARK_FT_ACTIVATION_DAILY_DRAFT a
    LEFT JOIN VW_CI_SITE_ZONE_PMO b on a.ci = b.ci
    WHERE a.TRANSACTION_DATE IN 
    ( 
    SELECT datecode 
    FROM (
            SELECT datecode 
            FROM 
                (SELECT DISTINCT datecode FROM DIM.DT_DATES 
                WHERE datecode BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
                AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) e
            LEFT JOIN 
                (SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY_DD 
                WHERE USAGE_CODE='DD_PMO_ACTIV'
                AND TRANSACTION_DATE BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
                AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) f
            ON e.datecode = f.transaction_date
            WHERE f.transaction_date IS NULL
        ) c
        INNER JOIN
        (
            SELECT EVENT_DATE
            FROM LOG_ACTIVATION_DAILY
            WHERE EVENT_DATE BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)
            AND IS_COMPLETED = 1
            AND SOURCE_TYPE IN ('DATA','VOICE-SMS','BUNDLE')
            GROUP BY EVENT_DATE
            HAVING COUNT(*) = 3
        ) d
        ON c.datecode = d.event_date
    )
    GROUP BY transaction_date,nom_zone;