INSERT OVERWRITE TABLE MON.SPARK_TMP_ACTIVATION_MONTH
SELECT 
*
FROM MON.SPARK_FT_ACTIVATION_MONTH1
where ACTIVATION_DATE = date_sub(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), 1)