insert into AGG.SPARK_FT_BTL_DATA
select 
    site_name,
    site_name_vendeur,
    datauser_type,
    count(distinct msisdn) msisdn_count,
    current_timestamp insert_date,
    '###SLICE_VALUE###' transaction_date
from 
(
    select * from MON.SPARK_FT_BTL_DATA_DETAIL 
    where transaction_date = '###SLICE_VALUE###'
) A 
left join 
(
    select distinct msisdn msisdn_process_month
    from MON.SPARK_FT_BTL_DATA_DETAIL
    where transaction_date >= concat(substr('###SLICE_VALUE###', 1, 8), '01') 
    and transaction_date < '###SLICE_VALUE###'
) B on A.msisdn = B.msisdn_process_month
where B.msisdn_process_month is null
group by 
    site_name,
    site_name_vendeur,
    datauser_type