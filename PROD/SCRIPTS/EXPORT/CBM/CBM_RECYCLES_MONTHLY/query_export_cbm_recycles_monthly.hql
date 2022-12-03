SELECT
    msisdn,
    activation_date,
    recyclage_date
FROM mon.spark_ft_cbm_recycles_daily 
where prod_state_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))
