insert into mon.spark_ft_subscriptions_report
select
	ipp,
	kpi_name,
	sum(kpi_value) kpi_value,
	statut_urbanite,
	site_name,
	ville,
	region_administrative,
	REGION_COMMERCIAL,
	current_timestamp insert_date,
	'###SLICE_VALUE###' event_date
from
(
	select
		T.msisdn msisdn,
		ipp,
		kpi_name,
		kpi_value,
		typedezone statut_urbanite,
		upper(trim(site_name)) site_name,
		upper(trim(townname)) ville,
		(
			case 
				when 
					upper(trim(townname)) in ('YAOUNDE', 'DOUALA') then upper(trim(townname)) 
				else upper(trim(region_administrative)) 
			end
		) region_administrative,
		upper(commercial_region) REGION_COMMERCIAL
	from
	(
		select
			msisdn,
			ipp,
			KPI_NAME,
			case
				when KPI_NAME = 'nbsubs_myorange' then nbsubs_myorange
				when KPI_NAME = 'nbsubs_ussd' then nbsubs_ussd
				when KPI_NAME = 'casubs_myorange' then casubs_myorange
				when KPI_NAME = 'casubs_ussd' then casubs_ussd
				when KPI_NAME = 'nbsubs_ussd_omaccount' then nbsubs_ussd_omaccount
				when KPI_NAME = 'nbsubs_ussd_mainaccount' then nbsubs_ussd_mainaccount
				when KPI_NAME = 'casubs_ussd_omaccount' then casubs_ussd_omaccount
				when KPI_NAME = 'casubs_ussd_mainaccount' then casubs_ussd_mainaccount
				when KPI_NAME = 'nbsubs_myorange_mtd' then nbsubs_myorange_mtd
				when KPI_NAME = 'nbsubs_ussd_mtd' then nbsubs_ussd_mtd
				when KPI_NAME = 'casubs_myorange_mtd' then casubs_myorange_mtd
				when KPI_NAME = 'casubs_ussd_mtd' then casubs_ussd_mtd
				when KPI_NAME = 'nbsubs_ussd_omaccount_mtd' then nbsubs_ussd_omaccount_mtd
				when KPI_NAME = 'nbsubs_ussd_mainaccount_mtd' then nbsubs_ussd_mainaccount_mtd
				when KPI_NAME = 'casubs_ussd_omaccount_mtd' then casubs_ussd_omaccount_mtd
				when KPI_NAME = 'casubs_ussd_mainaccount_mtd' then casubs_ussd_mainaccount_mtd
				else null
			end KPI_VALUE
		from
		(
			select 'nbsubs_myorange' as KPI_NAME
			union
			select 'nbsubs_ussd' as KPI_NAME
			union
			select 'casubs_myorange' as KPI_NAME
			union
			select 'casubs_ussd' as KPI_NAME
			union
			select 'nbsubs_ussd_omaccount' as KPI_NAME
			union
			select 'nbsubs_ussd_mainaccount' as KPI_NAME
			union
			select 'casubs_ussd_omaccount' as KPI_NAME
			union
			select 'casubs_ussd_mainaccount' as KPI_NAME
			union
			select 'nbsubs_myorange_mtd' as KPI_NAME
			union
			select 'nbsubs_ussd_mtd' as KPI_NAME
			union
			select 'casubs_myorange_mtd' as KPI_NAME
			union
			select 'casubs_ussd_mtd' as KPI_NAME
			union
			select 'nbsubs_ussd_omaccount_mtd' as KPI_NAME
			union
			select 'nbsubs_ussd_mainaccount_mtd' as KPI_NAME
			union
			select 'casubs_ussd_omaccount_mtd' as KPI_NAME
			union
			select 'casubs_ussd_mainaccount_mtd' as KPI_NAME
		) R
		cross join
		(
			select
				msisdn,
				ipp,
				sum(case when transaction_date = '###SLICE_VALUE###' then nbsubs_myorange else 0 end) nbsubs_myorange,
				sum(case when transaction_date = '###SLICE_VALUE###' then nbsubs_ussd else 0 end) nbsubs_ussd,
				sum(case when transaction_date = '###SLICE_VALUE###' then casubs_myorange else 0 end) casubs_myorange,
				sum(case when transaction_date = '###SLICE_VALUE###' then casubs_ussd else 0 end) casubs_ussd,
				sum(case when transaction_date = '###SLICE_VALUE###' then nbsubs_ussd_omaccount else 0 end) nbsubs_ussd_omaccount,
				sum(case when transaction_date = '###SLICE_VALUE###' then nbsubs_ussd_mainaccount else 0 end) nbsubs_ussd_mainaccount,
				sum(case when transaction_date = '###SLICE_VALUE###' then casubs_ussd_omaccount else 0 end) casubs_ussd_omaccount,
				sum(case when transaction_date = '###SLICE_VALUE###' then casubs_ussd_mainaccount else 0 end) casubs_ussd_mainaccount,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then nbsubs_myorange else 0 end) nbsubs_myorange_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then nbsubs_ussd else 0 end) nbsubs_ussd_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then casubs_myorange else 0 end) casubs_myorange_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then casubs_ussd else 0 end) casubs_ussd_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then nbsubs_ussd_omaccount else 0 end) nbsubs_ussd_omaccount_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then nbsubs_ussd_mainaccount else 0 end) nbsubs_ussd_mainaccount_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then casubs_ussd_omaccount else 0 end) casubs_ussd_omaccount_mtd,
				sum(case when transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then casubs_ussd_mainaccount else 0 end) casubs_ussd_mainaccount_mtd
			from
			(
				select 
					transaction_date,
					SERVED_PARTY_MSISDN msisdn,
					upper(subscription_service_details) ipp,
					sum(case when upper(trim(B.channel)) = 'MYORANGE' then 1 else 0 end) nbsubs_myorange,
					sum(case when upper(trim(B.channel)) != 'MYORANGE' then 1 else 0 end) nbsubs_ussd,
					sum(case when upper(trim(B.channel)) != 'MYORANGE' and upper(trim(B.channel)) = 'ORANGEMONEY' then 1 else 0 end) nbsubs_ussd_omaccount,
					sum(case when upper(trim(B.channel)) != 'MYORANGE' and upper(trim(B.channel)) != 'ORANGEMONEY' then 1 else 0 end) nbsubs_ussd_mainaccount,
					sum(case when upper(trim(B.channel)) = 'MYORANGE' then nvl(RATED_AMOUNT, 0) else 0 end) casubs_myorange,
					sum(case when upper(trim(B.channel)) != 'MYORANGE' then nvl(RATED_AMOUNT, 0) else 0 end) casubs_ussd,
					sum(case when upper(trim(B.channel)) != 'MYORANGE' and upper(trim(B.channel)) = 'ORANGEMONEY' then nvl(RATED_AMOUNT, 0) else 0 end) casubs_ussd_omaccount,
					sum(case when upper(trim(B.channel)) != 'MYORANGE' and upper(trim(B.channel)) != 'ORANGEMONEY' then nvl(RATED_AMOUNT, 0) else 0 end) casubs_ussd_mainaccount
				from
				(
					select *
					from mon.spark_ft_subscription 
					where transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
				) A
				left join dim.channel_subscriptions B
				on replace(upper(trim(A.subscription_channel)), ' ', '') = replace(upper(trim(B.code)), ' ', '')
				group by 
					transaction_date,
					SERVED_PARTY_MSISDN,
					subscription_service_details

			) T
			group by 
				msisdn,
				ipp
		) U
	) T
	left join 
	(
		select
			msisdn,
			nvl(site_name, 'ND') site_name,
			typedezone,
			townname,
			region_administrative,
			commercial_region
		from
		(
			select
				msisdn,
				nvl(site_name_b, site_name_a) site_msisdn
			from
			(
				select
					a.msisdn msisdn,
					max(a.site_name) site_name_a,
					max(b.site_name) site_name_b
				from mon.spark_ft_client_last_site_day a
				left join (
					select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
				) b on a.msisdn = b.msisdn
				where a.event_date='###SLICE_VALUE###'
				group by a.msisdn
			) T
		) P
		left join 
		(
			SELECT
				trim(upper(site_name)) SITE_NAME,
				max(typedezone) typedezone,
				max(townname) townname,
				max(region) region_administrative,
				max(commercial_region) commercial_region
			FROM DIM.SPARK_DT_GSM_CELL_CODE 
			group by trim(upper(site_name))
		) Q 
		on TRIM(NVL(upper(trim(site_msisdn)), 'ND')) = upper(trim(SITE_NAME))
	) site
) T
group by 
	ipp,
	kpi_name,
	site_name,
	ville,
	region_administrative,
	REGION_COMMERCIAL,
	statut_urbanite

union

select
	ipp,
	KPI_NAME,
	case
		when KPI_NAME = 'nbr_buyers_mtd' then nbr_buyers_mtd
		when KPI_NAME = 'nbr_buyers_myorange_mtd' then nbr_buyers_myorange_mtd
		when KPI_NAME = 'nbr_buyers_ussd_mtd' then nbr_buyers_ussd_mtd
		when KPI_NAME = 'nbr_buyers_ussd_omaccount_mtd' then nbr_buyers_ussd_omaccount_mtd
		when KPI_NAME = 'nbr_buyers_ussd_mainaccount_mtd' then nbr_buyers_ussd_mainaccount_mtd
		when KPI_NAME = 'nbr_buyers' then nbr_buyers
		when KPI_NAME = 'nbr_buyers_myorange' then nbr_buyers_myorange
		when KPI_NAME = 'nbr_buyers_ussd' then nbr_buyers_ussd
		when KPI_NAME = 'nbr_buyers_ussd_omaccount' then nbr_buyers_ussd_omaccount
		when KPI_NAME = 'nbr_buyers_ussd_mainaccount' then nbr_buyers_ussd_mainaccount
		else null
	end KPI_VALUE,
	statut_urbanite,
	site_name,
	ville,
	region_administrative,
	REGION_COMMERCIAL,
	current_timestamp insert_date,
	'###SLICE_VALUE###' event_date
from
(
	select 'nbr_buyers_mtd' as KPI_NAME
	union
	select 'nbr_buyers_myorange_mtd' as KPI_NAME
	union
	select 'nbr_buyers_ussd_mtd' as KPI_NAME
	union
	select 'nbr_buyers_ussd_omaccount_mtd' as KPI_NAME
	union
	select 'nbr_buyers_ussd_mainaccount_mtd' as KPI_NAME
	union
	select 'nbr_buyers' as KPI_NAME
	union
	select 'nbr_buyers_myorange' as KPI_NAME
	union
	select 'nbr_buyers_ussd' as KPI_NAME
	union
	select 'nbr_buyers_ussd_omaccount' as KPI_NAME
	union
	select 'nbr_buyers_ussd_mainaccount' as KPI_NAME
) R
cross join
(
	select
		ipp,
		count(distinct case when event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' then msisdn end) nbr_buyers_mtd,
		count(distinct case when event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and subscription_channel = 'MYORANGE' then msisdn end) nbr_buyers_myorange_mtd,
		count(distinct case when event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and subscription_channel = 'ORANGEMONEY' then msisdn end) nbr_buyers_ussd_mtd,
		count(distinct case when event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and subscription_channel != 'MYORANGE' and subscription_channel = 'ORANGEMONEY' then msisdn end) nbr_buyers_ussd_omaccount_mtd,
		count(distinct case when event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and subscription_channel != 'MYORANGE' and subscription_channel != 'ORANGEMONEY' then msisdn end) nbr_buyers_ussd_mainaccount_mtd,
		count(distinct case when event_date = '###SLICE_VALUE###' then msisdn end) nbr_buyers,
		count(distinct case when event_date = '###SLICE_VALUE###' and subscription_channel = 'MYORANGE' then msisdn end) nbr_buyers_myorange,
		count(distinct case when event_date = '###SLICE_VALUE###' and subscription_channel != 'MYORANGE' then msisdn end) nbr_buyers_ussd,
		count(distinct case when event_date = '###SLICE_VALUE###' and subscription_channel != 'MYORANGE' and subscription_channel = 'ORANGEMONEY' then msisdn end) nbr_buyers_ussd_omaccount,
		count(distinct case when event_date = '###SLICE_VALUE###' and subscription_channel != 'MYORANGE' and subscription_channel != 'ORANGEMONEY' then msisdn end) nbr_buyers_ussd_mainaccount,
		statut_urbanite,
		site_name,
		ville,
		region_administrative,
		REGION_COMMERCIAL
	from
	(
		select
			T.msisdn msisdn,
			ipp,
			subscription_channel,
			event_date,
			typedezone statut_urbanite,
			site_name,
			upper(trim(townname)) ville,
			(
				case 
					when 
						upper(trim(townname)) in ('YAOUNDE', 'DOUALA') then upper(trim(townname)) 
					else upper(trim(region_administrative)) 
				end
			) region_administrative,
			upper(trim(commercial_region)) REGION_COMMERCIAL
		from
		(
			select
				SERVED_PARTY_MSISDN msisdn,
				upper(subscription_service_details) ipp,
				upper(trim(B.channel)) subscription_channel,
				transaction_date event_date
			from
			(
				select *
				from mon.spark_ft_subscription 
				where transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
			) A
			left join dim.channel_subscriptions B
			on replace(upper(trim(A.subscription_channel)), ' ', '') = replace(upper(trim(B.code)), ' ', '')
		) T
		left join 
		(
			select
				msisdn,
				nvl(site_name, 'ND') site_name,
				typedezone,
				townname,
				region_administrative,
				commercial_region
			from
			(
				select
					msisdn,
					nvl(site_name_b, site_name_a) site_msisdn
				from
				(
					select
						a.msisdn msisdn,
						max(a.site_name) site_name_a,
						max(b.site_name) site_name_b
					from mon.spark_ft_client_last_site_day a
					left join (
						select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
					) b on a.msisdn = b.msisdn
					where a.event_date='###SLICE_VALUE###'
					group by a.msisdn
				) T
			) P
			left join 
			(
				SELECT
					trim(upper(site_name)) SITE_NAME,
					max(typedezone) typedezone,
					max(townname) townname,
					max(region) region_administrative,
					max(commercial_region) commercial_region
				FROM DIM.SPARK_DT_GSM_CELL_CODE 
				group by trim(upper(site_name))
			) Q 
			on TRIM(NVL(upper(trim(site_msisdn)), 'ND')) = upper(trim(SITE_NAME))
		) site
	) T
	group by 
		ipp,
		site_name,
		ville,
		region_administrative,
		REGION_COMMERCIAL,
		statut_urbanite
) U
