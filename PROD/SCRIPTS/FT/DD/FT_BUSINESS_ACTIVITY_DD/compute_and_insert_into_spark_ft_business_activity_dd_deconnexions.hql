--Deconnexions
insert into MON.SPARK_FT_BUSINESS_ACTIVITY_DD
    SELECT 'DD_PMO_DECON' usage_code,b.zone_lib,sum(effectif) parc,'FT_PARCS_SITE_DAY', CURRENT_TIMESTAMP,a.event_date
    FROM (
    	SELECT 
           EVENT_DATE, PARC_TYPE, PROFILE, 
           STATUT, EFFECTIF, SITE_NAME, 
           CURRENT_TIMESTAMP INSERT_DATE, CONTRACT_TYPE, 
           OPERATOR_CODE
        FROM MON.SPARK_FT_PARCS_SITE_DAY
        WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###' 
        AND PARC_TYPE = 'PARC_DECONNEXION' 
        AND STATUT = 'INACT'
    ) a
    LEFT JOIN (
    	SELECT DISTINCT zone_lib, site FROM DIM.DT_SITE_ZONE
    ) b 
    ON a.site_name = b.site
    --WHERE STATUT = 'ACTIF'
    GROUP BY a.event_date,b.zone_lib;