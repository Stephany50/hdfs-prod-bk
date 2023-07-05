SELECT IF(
    T_1.ft_tab = 0
    AND T_2.ft_tab > 0
    AND T_3.ft_tab > 0
    AND T_4.ft_tab > 0
    AND T_5.ft_tab > 0
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CBM_REGIONAL_REPORTING WHERE event_date = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE event_date = DATE_ADD('###SLICE_VALUE###', 1)) T_2,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CBM_ARPU_MOU WHERE event_date = '###SLICE_VALUE###') T_3,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_LOCALISATION_DATA_USERS WHERE event_date = '###SLICE_VALUE###') T_4,
(SELECT COUNT(*) ft_tab FROM MON.SPARK_FT_CBM_SEGMENT_VALUE WHERE event_date = DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM-01')) T_5