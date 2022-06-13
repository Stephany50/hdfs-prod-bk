insert into AGG.SPARK_FT_BTL_DATA
select 
    site_name,
    site_name_vendeur,
    datauser_type,
    count(distinct msisdn) msisdn_count,
    current_timestamp insert_date,
    '###SLICE_VALUE###' transaction_date
from MON.SPARK_FT_BTL_DATA_DETAIL 
where transaction_date = '###SLICE_VALUE###'
group by 
    site_name,
    site_name_vendeur,
    datauser_type