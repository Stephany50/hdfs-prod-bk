insert into TMP.TT_MSISDN_SUMMARY_DATA_TRAFIC_DATA_SITE
select
    event_date,
     msisdn ,
     couverture,
     technologie,
    case
        when bytes_used > 0 then 'User_Data'
        else 'Not_User_Data'
    end status,
    bytes_used,
    imei
from
(
    select *
    from MON.SPARK_FT_MSISDN_TRAFIC_DATA_SITE
    where event_date = '###SLICE_VALUE###'        -- '02/01/2017'        --d_slice_value--between  '01/01/2017' and '31/01/2017'
)a
left join (
select site_name, max(technologie) couverture
from default.VW_SDT_CI_INFO_NEW --DIM.SPARK_DT_GSM_CELL_CODE
group by site_name
)b ON a.site_name = b.site_name
LEFT JOIN (
    select tac_code, max(technologie) technologie from dim.dt_handset_ref group by tac_code
) c on  substring(imei, 1, 8) = tac_code
