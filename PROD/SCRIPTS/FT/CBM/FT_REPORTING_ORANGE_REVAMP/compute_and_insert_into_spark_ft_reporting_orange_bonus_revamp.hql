insert into mon.spark_ft_reporting_orange_bonus_revamp
select
type_orange_bonus,
type_validite,
type_souscription,
kpi_name,
case
when kpi_name = 'total_takers_count' then count(distinct case when transaction_date = '###SLICE_VALUE###' then msisdn end) --280 Mille
when kpi_name = 'upgrader_count' then count(distinct case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE%' then msisdn end)
when kpi_name = 'upgrader_1_count' then count(distinct case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 1%' then msisdn end)
when kpi_name = 'upgrader_2_count' then count(distinct case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 2%' then msisdn end)
when kpi_name = 'total_subscription_sum' then sum(case when transaction_date = '###SLICE_VALUE###' then b.total_occurence else 0 end)
when kpi_name = 'upgrader_1_subscription_sum' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 1%' then b.total_occurence else 0 end)
when kpi_name = 'upgrader_2_subscription_sum' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 2%' then b.total_occurence else 0 end)
when kpi_name = 'upgrader_subscription_sum' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE%' then b.total_occurence else 0 end)
when kpi_name = 'total_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' then nvl(b.bdle_cost, b.bdle_cost) else 0 end)
when kpi_name = 'voice_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_voix, 0) else 0 end)
when kpi_name = 'data_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_data, 0) else 0 end)
when kpi_name = 'upgrade_total_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE%' then nvl(b.bdle_cost, b.bdle_cost) else 0 end)
when kpi_name = 'upgrade_voice_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE%' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_voix, 0) else 0 end)
when kpi_name = 'upgrade_data_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE%' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_data, 0) else 0 end)
when kpi_name = 'upgrade_1_total_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 1%' then nvl(b.bdle_cost, b.bdle_cost) else 0 end)
when kpi_name = 'upgrade_1_voice_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 1%' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_voix, 0) else 0 end)
when kpi_name = 'upgrade_1_data_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 1%' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_data, 0) else 0 end)
when kpi_name = 'upgrade_2_total_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 2%' then nvl(b.bdle_cost, b.bdle_cost) else 0 end)
when kpi_name = 'upgrade_2_voice_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 2%' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_voix, 0) else 0 end)
when kpi_name = 'upgrade_2_data_revenue' then sum(case when transaction_date = '###SLICE_VALUE###' and upper(trim(a.bdle_name)) like '%UPGRADE 2%' then nvl(b.bdle_cost, b.bdle_cost) * nvl(coeff_data, 0) else 0 end)
when kpi_name = 'total_mtd_takers_count' then count(distinct msisdn)
when kpi_name = 'mtd_upgrader_count' then count(distinct case when upper(trim(a.bdle_name)) like '%UPGRADE%' then msisdn end)
when kpi_name = 'mtd_upgrader_1_count' then count(distinct case when upper(trim(a.bdle_name)) like '%UPGRADE 1%' then msisdn end)
when kpi_name = 'mtd_upgrader_2_count' then count(distinct case when upper(trim(a.bdle_name)) like '%UPGRADE 2%' then msisdn end)
end kpi_value,
current_timestamp insert_date,
'###SLICE_VALUE###' event_date
from
(
select 'total_takers_count' as kpi_name union
--select 'migrant_takers_count' as kpi_name union
--select 'non_migrant_takers_count' as kpi_name union
select 'upgrader_count' as kpi_name union
select 'upgrader_1_count' as kpi_name union
select 'upgrader_2_count' as kpi_name union
select 'total_subscription_sum' as kpi_name union
--select 'migrant_subscription_sum' as kpi_name union
--select 'non_migrant_subscription_sum' as kpi_name union
select 'upgrader_1_subscription_sum' as kpi_name union
select 'upgrader_2_subscription_sum' as kpi_name union
select 'upgrader_subscription_sum' as kpi_name union
select 'total_revenue' as kpi_name union
select 'voice_revenue' as kpi_name union
select 'data_revenue' as kpi_name union
--select 'migrant_total_revenue' as kpi_name union
--select 'migrant_voice_revenue' as kpi_name union
--select 'migrant_data_revenue' as kpi_name union
--select 'non_migrant_total_revenue' as kpi_name union
--select 'non_migrant_voice_revenue' as kpi_name union
--select 'non_migrant_data_revenue' as kpi_name union
select 'upgrade_total_revenue' as kpi_name union
select 'upgrade_voice_revenue' as kpi_name union
select 'upgrade_data_revenue' as kpi_name union
select 'upgrade_1_total_revenue' as kpi_name union
select 'upgrade_1_voice_revenue' as kpi_name union
select 'upgrade_1_data_revenue' as kpi_name union
select 'upgrade_2_total_revenue' as kpi_name union
select 'upgrade_2_voice_revenue' as kpi_name union
select 'upgrade_2_data_revenue' as kpi_name union
select 'total_mtd_takers_count' as kpi_name union
--select 'mtd_migrant_takers_count' as kpi_name union
--select 'mtd_non_migrant_takers_count' as kpi_name union
select 'mtd_upgrader_count' as kpi_name union
select 'mtd_upgrader_1_count' as kpi_name union
select 'mtd_upgrader_2_count' as kpi_name
) b
cross join
(
select
bdle_name,
type_forfait type_orange_bonus, -- voix, sms, data, combo ...
(
case
when validite=1 then 'JOUR'
when validite=7 then 'HEBDO'
when validite=30 then 'MOIS'
when validite=3 then 'MI-HEBDO'
when validite is null 
then (case when upper(offer_1) ='OB MOIS' then 'MOIS'  
when upper(offer_1) ='OB HEBDO' then 'HEBDO'
when upper(offer_1) ='OB JOUR' then 'JOUR' end)
else 'OTHER'
end
) type_validite,
prix bdle_cost,
case when (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0)) = 100 then  nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0) end coeff_data,
case when (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming_voix, 0)) = 100 then nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming_voix, 0) end coeff_voix ,
case when (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0)) != 100 and (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming_voix, 0)) != 100 then  nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming_voix, 0)  end combo
from dim.dt_cbm_ref_souscription_price where upper(trim(type_ocm)) like '%OB%'--insertion dans une table pour projet à venir
--where validite is null and upper(trim(type_ocm)) like '%OB%';
) a
left join
(
select
served_party_msisdn as msisdn,
subscription_service_details as bdle_name,
rated_amount as bdle_cost, total_occurence,
(
case
when cast(subscription_channel as int) in (32) then 'ussd_wallet_om'
when cast(subscription_channel as int) in (18) then 'myorange_aas'
when cast(subscription_channel as int) in (20) then 'myorange_gos_sdp'
when cast(subscription_channel as int) in (9) then 'ussd_main_balance'
when subscription_channel = '__channel_my_orange_main_balance__' then 'myorange_main_balance'
when subscription_channel = '__channel_my_orange_wallet_om__' then 'myorange_wallet_om'
end
) type_souscription,
transaction_date
from MON.SPARK_FT_SUBSCRIPTION
where TRANSACTION_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
) b on upper(trim(a.bdle_name)) = upper(trim(b.bdle_name))
group by type_orange_bonus, type_validite, type_souscription, kpi_name