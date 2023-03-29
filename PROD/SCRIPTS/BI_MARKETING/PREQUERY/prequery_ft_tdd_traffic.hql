SELECT IF(
    T_1.ft_tab = 0
    AND T_4.ft_tab > 0
    AND T_5.ft_tab > 0
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_TDD_TRAFFIC WHERE event_date = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CRA_GPRS WHERE session_date = '###SLICE_VALUE###') T_4,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CRA_GPRS_POST WHERE session_date = '###SLICE_VALUE###') T_5