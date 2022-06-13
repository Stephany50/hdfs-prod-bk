insert into MON.SPARK_FT_BTL_DATA_DETAIL
select 
    aa.msisdn,
    case 
        when est_ga_data = 1 then 'GROSS_ADD_DATA' 
        when est_ga_data = 0 and segment_data = 'active_30d' then 'RETAINED_DATA'
        when est_ga_data = 0 and segment_data = 'active_31-90d' then 'WAKEUP_DATA'
        when est_ga_data = 0 and segment_data = 'active_91-120d' then 'RECONNEXIONS_DATA'
        when est_ga_data = 0 and segment_data not in ('active_30d','active_31-90d','active_91-120d') then 'CONVERSIONS_DATA'
        else 'OTHER'
    end datauser_type,
    site_name,
    site_name_vendeur,
    current_timestamp insert_date,
    aa.transaction_date
from 
(
    select distinct 
        a.transaction_date,
        a.msisdn,
        c.site_name site_name_vendeur,
        d.site_name site_name,
        case 
            when b.msisdn is not null 
            then 1 
            else 0 
        end est_ga_data
    from
    (
        select distinct * 
        from mon.spark_ft_btl_report 
        where transaction_date = '###SLICE_VALUE###' 
        and ipp_stream in ('DATA_ONLY','COMBO')
    ) a  
    left join 
    (
        select distinct * 
        from MON.SPARK_FT_GROSSADD_DAY 
        where transaction_date = '###SLICE_VALUE###'
    ) b on GET_NNP_MSISDN_9DIGITS(a.msisdn) = GET_NNP_MSISDN_9DIGITS(b.msisdn)
    left join 
    (
        select *
        from MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
        where event_date = '###SLICE_VALUE###'
    ) c on GET_NNP_MSISDN_9DIGITS(a.msisdn_vendeur) = GET_NNP_MSISDN_9DIGITS(c.msisdn)
    left join 
    (
        select *
        from MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
        where event_date = '###SLICE_VALUE###'
    ) d on GET_NNP_MSISDN_9DIGITS(a.msisdn) = GET_NNP_MSISDN_9DIGITS(d.msisdn)
) aa 
full join 
(
    select distinct 
        msisdn,
        segment_data,
        last_day_month 
    from MON.SPARK_FT_DATAUSERS_DAY
    where event_month=substr(add_months('###SLICE_VALUE###', -1), 1, 7) 
) bb 
on GET_NNP_MSISDN_9DIGITS(aa.msisdn) = GET_NNP_MSISDN_9DIGITS(bb.msisdn) 
where aa.msisdn is not null