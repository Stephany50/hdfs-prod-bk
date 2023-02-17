SELECT IF(
    T_1.ft_tab = 0
    AND T_2.ft_tab > 0
    AND T_3.ft_tab > 0
    AND T_4.ft_tab > 0
    AND T_5.ft_tab > 0
    AND T_6.ft_tab > 0
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_TDD_SNAPSHOT WHERE event_date = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE event_date = DATE_ADD('###SLICE_VALUE###', 1)) T_2,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_IMEI_ONLINE WHERE sdate = '###SLICE_VALUE###') T_3,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CRA_GPRS WHERE session_date = '###SLICE_VALUE###') T_4,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CRA_GPRS_POST WHERE session_date = '###SLICE_VALUE###') T_5,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_TDD_SNAPSHOT WHERE event_date = DATE_SUB('###SLICE_VALUE###', 1)) T_6