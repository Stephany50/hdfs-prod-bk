 --Capilarité SIM
INSERT INTO ft_business_activity_dd
SELECT 
	'DD_PMO_CAP_SIM', zone_lib, nb_cap_sim ,
	'ACTIV_IDENTIF_DAILY' source_table, 
	CURRENT_TIMESTAMP,d_start_date
FROM
(
   SELECT  
   		coalesce(zone_pmo,zone_lib) zone_lib, sum(coalesce(a.nb_cap_sim,0) + coalesce(b.nb_cap_sim,0)) nb_cap_sim
   FROM
    (
        SELECT 
        	coalesce(zone_pmo,zone_lib) zone_pmo, 
        	sum(coalesce(a.nb_cap_sim,0) + coalesce(b.nb_cap_sim,0)) nb_cap_sim
        FROM(
            select b.zone_pmo, count(msisdn_identificateur) nb_cap_sim
            from 
               (select event_date, msisdn_identificateur,zone_commerciale
                from mondv.activ_identif_daily a
                left join dim.dt_base_identificateur b on a.MSISDN_IDENTIFICATEUR = b.MSISDN
                where event_date = d_start_date and nbre_actives >0
                and zone_commerciale not like '%NOMAD%') a
            left join 
            	(select distinct zone_commerciale ,zone_pmo from mondv.dt_zone_commerciale) b 
            on trim(a.zone_commerciale) = trim(b.zone_commerciale)
            group by b.zone_pmo
        ) a 
        FULL JOIN (
            select t.zone_lib, count(msisdn_identificateur) nb_cap_sim
            from 
            	(select event_date, msisdn_identificateur,zone_commerciale
                from mondv.activ_identif_daily a
                join dim.dt_base_identificateur b on a.MSISDN_IDENTIFICATEUR = b.MSISDN
                where event_date = d_start_date and nbre_actives >0
                 and zone_commerciale like '%NOMAD%') a
            left join 
            	(select msisdn, site_name from ft_client_last_site_day where event_date = d_start_date) b 
            on a.msisdn_identificateur = b.msisdn
            left join 
            	(select distinct site, zone_lib from mondv.dt_site_zone) t 
            on b.site_name = t.site
            group by t.zone_lib
        ) b on a.zone_pmo = b.zone_lib
        GROUP BY coalesce(zone_pmo,zone_lib)
    ) a
    FULL JOIN 
    (
        select t.zone_lib, count(msisdn_identificateur) nb_cap_sim
        from
        	(select event_date, msisdn_identificateur,zone_commerciale
            from mondv.activ_identif_daily a
            left join dim.dt_base_identificateur b on a.MSISDN_IDENTIFICATEUR = b.MSISDN
            where event_date = d_start_date and nbre_actives >0
            and zone_commerciale is null) a
        left join 
        	(select msisdn, site_name from ft_client_last_site_day where event_date = d_start_date) b 
        on a.msisdn_identificateur = b.msisdn
        left join 
        	(select distinct site, zone_lib from mondv.dt_site_zone) t 
        on b.site_name = t.site
        group by t.zone_lib
    ) b on a.zone_pmo = b.zone_lib
    group by coalesce(zone_pmo,zone_lib) 
); 