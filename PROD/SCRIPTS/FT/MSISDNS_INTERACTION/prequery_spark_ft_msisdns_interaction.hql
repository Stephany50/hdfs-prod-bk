SELECT IF(T1.FT_COUNT = 0  AND T2_COUNT =  DATEDIFF(last_day(ADD_MONTHS(concat('###SLICE_VALUE###', '-01'), -1)), date_sub(concat('###SLICE_VALUE###', '-01'), ${hivevar:nb_jour_30}))+1 ,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_MSISDNS_INTERACTION_MONTH WHERE event_month= '###SLICE_VALUE###') T1,
    (SELECT COUNT(DISTINCT transaction_date) T2_COUNT FROM MON.SPARK_FT_MSC_TRANSACTION
        WHERE transaction_date BETWEEN date_sub(concat('###SLICE_VALUE###', '-01'), ${hivevar:nb_jour_30}) AND last_day(ADD_MONTHS(concat('###SLICE_VALUE###', '-01'), -1)) 
    ) T2
  