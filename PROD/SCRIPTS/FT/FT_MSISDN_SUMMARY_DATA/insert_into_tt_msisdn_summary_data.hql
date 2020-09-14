insert into TMP.TT_MSISDN_SUMMARY_DATA
select
    msisdn,
    couverture,
    technologie,
    case when bytes_used > 0 then 'User_Data' else 'Not_User_Data' end status,
    bytes_used,
    imei as last_imei_used,
    event_date
from
(
    select *
    from MON.SPARK_FT_MSISDN_TRAFIC_DATA_SITE
    where event_date = '2020-05-31'
) a
left join
(
    select
        site_name,
        max(technologie) couverture
    from VW_SDT_CI_INFO_NEW
    group by site_name
) b
on a.site_name = b.site_name
left join
(
    select
       tac_code,
       max(technologie) technologie
    from dim.dt_handset_ref
    group by tac_code
)
where a.site_name = b.site_name
and substr(imei, 1, 8) = tac_code