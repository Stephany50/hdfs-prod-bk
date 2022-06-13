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
	select *
	from MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
	where event_date = '###SLICE_VALUE###'
) B on GET_NNP_MSISDN_9DIGITS(a.msisdn_vendeur) = GET_NNP_MSISDN_9DIGITS(b.msisdn)
left join 
(
	select *
	from MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
	where event_date = '###SLICE_VALUE###'
) C on GET_NNP_MSISDN_9DIGITS(a.msisdn) = GET_NNP_MSISDN_9DIGITS(C.msisdn)
group by 
	type_forfait,
	msisdn_vendeur,
	ipp_name,
	ipp_stream,
	C.site_name,
	B.site_name


