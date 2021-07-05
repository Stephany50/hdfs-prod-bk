
insert into mon.spark_ft_perco_q1_2021_incremental
select
     b5.site_name,
     b5.region,
     b.msisdn msisdn,

     (nvl(b.revenu_subs_total,0)-nvl(c0.avg_revenu_subs_total,0)) + (nvl(b.revenu_paygo_total,0)-nvl(c0.avg_revenu_paygo_total,0)) ca_incremental_daily,
     (nvl(b.revenu_subs_voice,0) - nvl(c0.avg_revenu_subs_voice,0)) + (nvl(b.revenu_paygo_voice,0) - nvl(c0.avg_revenu_paygo_voice,0)) ca_voice_incremental_daily,
     (nvl(b.revenu_subs_data,0) - nvl(c0.avg_revenu_subs_data,0))  ca_data_incremental_daily,
     (nvl(b.revenu_subs_sms,0) - nvl(c0.avg_revenu_subs_sms,0)) + (nvl(b.revenu_paygo_sms,0) - nvl(c0.avg_revenu_paygo_sms,0)) ca_sms_incremental_daily,

     (nvl(b.revenu_subs_total,0)-nvl(c0.avg_revenu_subs_total,0)) ca_subs_incremental_daily,
     (nvl(b.revenu_subs_voice,0) - nvl(c0.avg_revenu_subs_voice,0)) ca_subs_voice_incremental_daily,
     (nvl(b.revenu_subs_data,0) -nvl(c0.avg_revenu_subs_data,0)) ca_subs_data_incremental_daily,
     (nvl(b.revenu_subs_sms,0) - nvl(c0.avg_revenu_subs_sms,0)) ca_subs_sms_incremental_daily,

     (nvl(b.revenu_paygo_total,0)-nvl(c0.avg_revenu_paygo_total,0)) ca_paygo_incremental_daily,
     (nvl(b.revenu_paygo_voice,0) - nvl(c0.avg_revenu_paygo_voice,0)) ca_paygo_voice_incremental_daily,
     0 ca_paygo_data_incremental_daily,
      (nvl(b.revenu_paygo_sms,0) - nvl(c0.avg_revenu_paygo_sms,0)) ca_paygo_sms_incremental_daily,

     (nvl(b.usage_voice,0)-nvl(c0.avg_usage_voice,0)) usage_incremental_voice_daily,
     (nvl(b.usage_data,0)-nvl(c0.avg_usage_data,0)) usage_incremental_data_daily,
     (nvl(b.usage_sms,0)-nvl(c0.avg_usage_sms,0)) usage_incremental_sms_daily,

     current_date insert_date,
     '###SLICE_VALUE###' event_date
from
(
  select
    nvl(z.msisdn,c.msisdn) msisdn,
    revenu_subs_total,
    revenu_subs_voice,
    revenu_subs_data,
    revenu_subs_sms,
    revenu_paygo_total,
    revenu_paygo_voice,
    revenu_paygo_sms,
    usage_voice,
    usage_data,
    usage_sms,
    nvl(z.event_date,c.event_date) event_date
  from
  (
 select 
    MSISDN,
    event_date,
    sum(nvl(total_cost,0)) revenu_subs_total,
    sum(nvl(total_cost_voice,0)) revenu_subs_voice,
    sum(nvl(total_cost_data,0)) revenu_subs_data,
    sum(nvl(total_cost_sms,0)) revenu_subs_sms
 from (
     select
         msisdn,
         nber_purchase,
         total_cost,
         total_cost*nvl(coeff_voice,0) total_cost_voice,
         total_cost*nvl(coeff_data,0) total_cost_data,
         total_cost*nvl(coeff_sms,0) total_cost_sms,
         event_date
     from
     (
        select
            msisdn,
            bdle_name,
            nber_purchase,
            bdle_cost total_cost,
            period event_date
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        where period = '###SLICE_VALUE###'
    ) a0
    left join
    (
        select
            (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_voice,
            (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0))/100 coeff_data,
            (nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_sms,
            bdle_name
        from DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
    ) b ON UPPER(TRIM(a0.bdle_name)) = UPPER(TRIM(b.bdle_name))
 )
 group by msisdn,event_date
)z
  full join
  (
    select
        msisdn,
        event_date,
        max(nvl(og_total_call_duration,0)/60) usage_voice,
        max(nvl((data_bytes_received + data_bytes_sent),0)/(1024*1024)) usage_data,
        max(nvl(og_sms_total_count,0)) usage_sms,
        max(nvl(main_rated_tel_amount, 0)+nvl(main_rated_sms_amount, 0)) revenu_paygo_total,
        max(nvl(main_rated_tel_amount, 0)) revenu_paygo_voice,
        max(nvl(main_rated_sms_amount, 0)) revenu_paygo_sms
    from mon.spark_ft_marketing_datamart
    where event_date = '###SLICE_VALUE###'
    group by msisdn, event_date
  ) c on z.msisdn = c.msisdn and z.event_date = c.event_date
)b
left join tmp.kpi_before_perco_q1_2021 c0 on c0.msisdn = b.msisdn
left JOIN
(
   SELECT
    nvl(b50.MSISDN, b51.MSISDN) MSISDN,
    UPPER(NVL(b51.SITE_NAME, b50.SITE_NAME)) SITE_NAME,
    "REGION" REGION
   FROM
   (
    select
        msisdn,
        site_name
    from
    (
        select
            msisdn,
            site_name,
            row_number() over(partition by msisdn order by nbre_apparition_msisdn_site desc) line_number
        from
        (
            SELECT
                MSISDN,
                SITE_NAME,
                count(*) nbre_apparition_msisdn_site
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
            WHERE EVENT_DATE='###SLICE_VALUE###'
            GROUP BY MSISDN, SITE_NAME, administrative_region
        ) b500
    ) x
    where line_number = 1
   ) b50
   FULL JOIN
   (
    select
        msisdn,
        site_name
    from
    (
        select
            msisdn,
            site_name,
            row_number() over(partition by msisdn order by nbre_apparition_msisdn_site desc) line_number
        from
        (
            SELECT
                MSISDN,
                SITE_NAME,
                count(*) nbre_apparition_msisdn_site
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE='###SLICE_VALUE###'
            GROUP BY MSISDN, SITE_NAME , administrative_region
        ) b510
    ) x
    where line_number = 1
   ) b51
   ON b50.MSISDN = b51.MSISDN
) b5 on b.msisdn = b5.msisdn