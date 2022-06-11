SELECT IF(
    T_1.ft_exist = 0
    and T_2.nbr = 120
    , 'OK'
    , 'NOK'
)
FROM
(select count(*) ft_exist from MON.SPARK_FT_DATAUSERS_DAY where event_month = '###SLICE_VALUE###' ) T_1,
(SELECT COUNT(distinct session_date) nbr FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE BETWEEN date_sub(last_day('###SLICE_VALUE###'), 119) AND last_day('###SLICE_VALUE###')) T_2,
