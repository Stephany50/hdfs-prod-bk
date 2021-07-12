
insert into agg.spark_ft_a_customer_base
select
	count(distinct case when parc_group>=1 then msisdn end) parc_group,
	count(distinct case when parc_art>=1 then msisdn end) parc_art,
	count(distinct case when parc_om>=1 then msisdn end) parc_om,
	count(distinct case when parc_actif_om>=1 then msisdn end) parc_actif_om,
	count(distinct case when all30daysbase>=1 then msisdn end) all30daysbase,
	count(distinct case when dailybase>=1 then msisdn end) dailybase,
	count(distinct case when all90dayslost>=1 then msisdn end) all90dayslost,
	count(distinct case when all30dayslost>=1 then msisdn end) all30dayslost,
	count(distinct case when lostat90days>=1 then msisdn end) lostat90days,
	count(distinct case when lostat30days>=1 then msisdn end) lostat30days,
	count(distinct case when all90dayswinback>=1 then msisdn end) all90dayswinback,
	count(distinct case when all30dayswinback>=1 then msisdn end) all30dayswinback,
	count(distinct case when winbackat90days>=1 then msisdn end) winbackat90days,
	count(distinct case when winbackat30days>=1 then msisdn end) winbackat30days,
	count(distinct case when churner>=1 then msisdn end) churner,
	count(distinct case when gross_add>=1 then msisdn end) gross_add,
	count(distinct case when gross_add_om>=1 then msisdn end) gross_add_om,
	count(distinct case when smartphone_user>=1 then msisdn end) smartphone_user,
	
	segment_valeur_premium,
	segment_valeur_high_value,
	segment_valeur_telco,
	type_usage,
	anciennete,
	statut_urbanite,
	site_name,
	ville,
	region_administrative,
	REGION_COMMERCIAL,
	current_timestamp insert_date,
	'###SLICE_VALUE###' event_date
from mon.spark_ft_customer_base
where event_date = '###SLICE_VALUE###'
group by
	site_name,
	ville,
	region_administrative,
	REGION_COMMERCIAL,
	statut_urbanite,
	anciennete,
	type_usage,
	segment_valeur_telco,
	segment_valeur_high_value,
	segment_valeur_premium
