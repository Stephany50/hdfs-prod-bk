SELECT IF(
    T_1.ft_exist = 0
    and T_2.ft_exist > 0
    , 'OK'
    , 'NOK'
)
FROM
(select count(*) ft_exist from AGG.SPARK_FT_BTL_DATA where transaction_date = '###SLICE_VALUE###' ) T_1,
(select count(*) ft_exist from MON.SPARK_FT_BTL_DATA_DETAIL where transaction_date = '###SLICE_VALUE###' ) T_2