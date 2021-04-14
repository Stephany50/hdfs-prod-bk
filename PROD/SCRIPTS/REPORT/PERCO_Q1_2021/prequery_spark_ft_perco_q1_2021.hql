SELECT IF(
    A.FT_EXIST = 0
    and G.FT_EXIST > 0
    and B.FT_EXIST > 0
    and H.FT_EXIST > 0
    and I.FT_EXIST > 0
    and J.IT_EXIST > 0
    and K.IT_EXIST > 0
    , "OK"
    , "NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_perco_q1_2021 WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) IT_EXIST FROM CDR.SPARK_IT_ZEMBLAREPORT WHERE EVENT_DATE='###SLICE_VALUE###') J,
(SELECT COUNT(*) IT_EXIST FROM CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND WHERE EVENT_DATE='###SLICE_VALUE###') B,
(SELECT COUNT(*) IT_EXIST FROM CDR.SPARK_IT_MYWAY_REPORT WHERE EVENT_DATE='###SLICE_VALUE###') K,
(SELECT COUNT(*) FT_EXIST FROM cdr.spark_it_omny_transactions WHERE transfer_datetime='###SLICE_VALUE###') G,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='###SLICE_VALUE###') H,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###') I
