SELECT IF(
    T_1.ft_tab = 0
    AND T_2.ft_tab > 0
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_TDD_PARC WHERE event_date = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_TDD_SNAPSHOT WHERE event_date = '###SLICE_VALUE###') T_2