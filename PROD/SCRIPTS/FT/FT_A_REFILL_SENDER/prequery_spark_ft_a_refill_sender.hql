SELECT IF(
    A.FT_EXIST = 0
    and B.FT_COUNT > datediff(last_day('###SLICE_VALUE###'||'-01') , '###SLICE_VALUE###'||'-01')
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.spark_FT_A_REFILL_SENDER WHERE REFILL_MONTH = '###SLICE_VALUE###') A,
(SELECT COUNT(DISTINCT refill_date) FT_COUNT FROM MON.spark_FT_REFILL WHERE refill_date BETWEEN '###SLICE_VALUE###'||'-01' AND LAST_DAY('###SLICE_VALUE###'||'-01')) B

