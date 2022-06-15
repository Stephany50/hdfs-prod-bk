insert into AGG.SPARK_FT_BTL_SUBS
select 
	type_forfait,
	msisdn_vendeur,
	ipp_name,
	ipp_stream,
	C.site_name,
	B.site_name site_name_vendeur,
	count(distinct A.msisdn) msisdn_count,
	count(ipp_name) nb_sous,
	sum(prix) ca,
	current_timestamp insert_date,
	'###SLICE_VALUE###' transaction_date
from 
(
	select * 
	from mon.spark_ft_btl_report
	where transaction_date = '###SLICE_VALUE###'
) A
left join 
(
	select
		nvl(b.msisdn, a.msisdn) msisdn,
		max
		(
			case when b.msisdn is not null then b.site_name
			else a.site_name end
		) site_name
	from (select * from mon.spark_ft_client_last_site_day where event_date = '###SLICE_VALUE###') a
	full join (
		select * from mon.spark_ft_client_site_traffic_day where event_date = '###SLICE_VALUE###' 
	) b on a.msisdn = b.msisdn
	group by nvl(b.msisdn, a.msisdn)
) B on GET_NNP_MSISDN_9DIGITS(a.msisdn_vendeur) = GET_NNP_MSISDN_9DIGITS(b.msisdn)
left join 
(
	select
		nvl(b.msisdn, a.msisdn) msisdn,
		max
		(
			case when b.msisdn is not null then b.site_name
			else a.site_name end
		) site_name
	from (select * from mon.spark_ft_client_last_site_day where event_date = '###SLICE_VALUE###') a
	full join (
		select * from mon.spark_ft_client_site_traffic_day where event_date = '###SLICE_VALUE###' 
	) b on a.msisdn = b.msisdn
	group by nvl(b.msisdn, a.msisdn)
) C on GET_NNP_MSISDN_9DIGITS(a.msisdn) = GET_NNP_MSISDN_9DIGITS(C.msisdn)
group by 
	type_forfait,
	msisdn_vendeur,
	ipp_name,
	ipp_stream,
	C.site_name,
	B.site_name


