INSERT OVERWRITE TABLE MON.SPARK_TMP_ACTIVATION_MONTH
SELECT 
*
FROM MON.SPARK_FT_ACTIVATION_MONTH1
where EVENT_MONTH = substr('###SLICE_VALUE###', 1, 7)