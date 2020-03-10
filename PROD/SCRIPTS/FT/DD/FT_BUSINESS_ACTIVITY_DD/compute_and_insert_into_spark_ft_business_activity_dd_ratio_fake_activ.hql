--Ratio Fakes/Activations
INSERT INTO FT_BUSINESS_ACTIVITY_DD
SELECT 
	'DD_PMO_RATIO_FAKE',b.zone_lib,sum(fake_act)*100 / sum(subscriber_count) ratio, 
	'FT_PARC_ACTIVATIONS_SITE', CURRENT_TIMESTAMP, a.event_date
FROM MONDV.FT_PARC_ACTIVATIONS_SITE a
LEFT JOIN (select distinct zone_lib, site from mondv.dt_site_zone) b on a.first_site_name = b.site
WHERE a.event_date IN 
	(
		SELECT datecode 
		FROM 
		    (
		    	SELECT DISTINCT datecode FROM DIM.DT_DATES 
		         WHERE datecode BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) AND TRUNC(CURRENT_DATE- dashboard_kpi_day_bef)
		    ) c
		  LEFT JOIN 
		    (
		    	SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY_DD 
		        WHERE USAGE_CODE='DD_PMO_RATIO_FAKE'
		        AND TRANSACTION_DATE BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) and TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)
		    ) d
		  ON c.datecode = d.transaction_date
		WHERE d.transaction_date IS NULL
	)
GROUP BY a.event_date,b.zone_lib;