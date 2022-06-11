SELECT IF(
    T_1.ft_exist = 0
    and T_2.ft_exist > 0
    and T_3.ft_exist > 0
    , 'OK'
    , 'NOK'
)
FROM
(select count(*) ft_exist from AGG.SPARK_FT_BTL_SUBS where transaction_date = '###SLICE_VALUE###' ) T_1,
(select count(*) ft_exist from mon.spark_ft_btl_report where transaction_date = '###SLICE_VALUE###' ) T_2,
(select count(*) ft_exist from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date = '###SLICE_VALUE###' ) T_3