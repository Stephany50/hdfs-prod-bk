SELECT IF(
    T_1.ft_exist = 0
    and T_2.FT_EXIT > 1
    , 'OK'
    , 'NOK'
)
FROM
(select count(*) ft_exist from  where TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIT FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2
