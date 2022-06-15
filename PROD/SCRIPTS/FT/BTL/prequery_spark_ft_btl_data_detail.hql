SELECT IF(
    T_1.ft_exist = 0
    and T_2.ft_exist > 0
    and T_3.ft_exist > 0
    and T_4.ft_exist > 0
    and T_5.ft_exist > 0
    and T_6.ft_exist > 0
    , 'OK'
    , 'NOK'
)
FROM
(select count(*) ft_exist from MON.SPARK_FT_BTL_DATA_DETAIL where transaction_date = '###SLICE_VALUE###' ) T_1,
(select count(*) ft_exist from mon.spark_ft_btl_report where transaction_date = '###SLICE_VALUE###' ) T_2,
(select count(*) ft_exist from MON.SPARK_FT_GROSSADD_DAY where transaction_date = '###SLICE_VALUE###' ) T_3,
(select count(*) ft_exist from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date = '###SLICE_VALUE###' ) T_4,
(select count(*) ft_exist from MON.spark_ft_client_site_traffic_day where event_date = '###SLICE_VALUE###' ) T_5,
(select count(*) ft_exist from MON.SPARK_FT_DATAUSERS_DAY where event_month = substr(add_months('###SLICE_VALUE###', -1), 1, 7) ) T_6