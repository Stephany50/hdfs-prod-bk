insert into mon.spark_ft_cbm_recycles_daily
select 
    distinct msisdn,
    to_date(created_date) activation_date,
    to_date(prod_state_date) recyclage_date
from 
(
    select 
        first_value(acc_nbr) over(partition by acc_nbr order by update_date desc) msisdn, 
        subs_id 
    from CDR.SPARK_IT_ZTE_SUBS_EXTRACT 
    where original_file_date = '###SLICE_VALUE###'
) a
JOIN
(
    select 
        prod_id, 
        created_date, 
        prod_state_date
    from CDR.SPARK_IT_ZTE_PROD_EXTRACT 
    where prod_state='B' and to_date(prod_state_date)='###SLICE_VALUE###' and original_file_date=date_add('###SLICE_VALUE###', 1)
) b
on a.subs_id=b.prod_id
