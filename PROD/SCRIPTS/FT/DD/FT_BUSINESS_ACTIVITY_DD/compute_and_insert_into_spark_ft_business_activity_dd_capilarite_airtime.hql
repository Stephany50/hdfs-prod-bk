--Capilarité Airtime
INSERT INTO FT_BUSINESS_ACTIVITY_DD
SELECT 
	'DD_PMO_CAP_AT' usage_code, zone_lib, cap_at,
	'FT_REFILL' source_table, CURRENT_TIMESTAMP , refill_date
FROM 
	(
		SELECT a.refill_date, zone_lib, count(distinct sender_msisdn) cap_at 
		FROM
			(select refill_date, sender_msisdn 
			 from mon.ft_refill 
			 where refill_mean = 'C2S' 
			 and termination_ind = 200 
			 and refill_date = d_start_date) a 
		LEFT JOIN 
			(select msisdn, site_name
			 from ft_client_last_site_day 
			 where event_date = d_start_date) b 
		ON a.sender_msisdn = b.msisdn
		LEFT JOIN 
			(select distinct  site,zone_lib 
			 from mondv.dt_site_zone) c
	 	ON c.site = b.site_name
		GROUP BY refill_date, zone_lib
	)
WHERE 
	refill_date IN 
	(
	  SELECT datecode 
      FROM 
			(SELECT DISTINCT datecode FROM DIM.DT_DATES 
			WHERE datecode 
			BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
			AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) e
        LEFT JOIN 
			(SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY_DD 
			WHERE USAGE_CODE='DD_PMO_CAP_AT'
			AND TRANSACTION_DATE 
			BETWEEN TRUNC(CURRENT_DATE-dashboard_kpi_day_from) 
			AND TRUNC(CURRENT_DATE-dashboard_kpi_day_bef)) f
        ON e.datecode = f.transaction_date
      WHERE
      	f.transaction_date IS NULL
    );