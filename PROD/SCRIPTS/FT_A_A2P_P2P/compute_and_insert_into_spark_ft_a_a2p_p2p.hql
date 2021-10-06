insert into MON.SPARK_FT_A_A2P_P2P
select service_centre, count(*) nb, current_timestamp insert_date,transaction_date
from MON.SPARK_FT_MSC_TRANSACTION
where transaction_date = '###SLICE_VALUE###'
and substr(transaction_type, 1, 3) = 'SMS'
and ( SERVICE_CENTRE like '%32033532%'
    or  SERVICE_CENTRE like '%32030112%'
    or SERVICE_CENTRE like '%3315096%'
    or SERVICE_CENTRE like '%3384141%'
    )
group by transaction_date, service_centre;