--Deconnexions
insert into ft_business_activity_dd
    SELECT 'DD_PMO_DECON' usage_code,b.zone_lib,sum(effectif) parc,'FT_PARCS_SITE_DAY', CURRENT_TIMESTAMP,a.event_date
    FROM TMP.TT_PARCS_SITE_DAY a
    LEFT JOIN (select distinct zone_lib, site from mondv.dt_site_zone) b on a.site_name = b.site
    WHERE parc_type = 'PARC_DECONNEXION' AND STATUT = 'ACTIF' AND  a.event_date IN 
    	(
    	 SELECT c.datecode 
    	 FROM 
    	 	(SELECT DISTINCT datecode 
				FROM DIM.DT_DATES 
				WHERE datecode 
				BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
				AND TRUNC(CURRENT_DATE- dashboard_kpi_day_bef)) c
           LEFT JOIN  
	      	(SELECT DISTINCT transaction_date 
				FROM FT_BUSINESS_ACTIVITY_DD
				WHERE USAGE_CODE='DD_PMO_DECON' 
				AND TRANSACTION_DATE 
				BETWEEN trunc(CURRENT_DATE-dashboard_kpi_day_from) 
				AND trunc(CURRENT_DATE-dashboard_kpi_day_bef)) d
      	   ON c.datecode = d.transaction_date
      	 WHERE d.transaction_date IS NULL
      	)
    GROUP BY a.event_date,b.zone_lib;