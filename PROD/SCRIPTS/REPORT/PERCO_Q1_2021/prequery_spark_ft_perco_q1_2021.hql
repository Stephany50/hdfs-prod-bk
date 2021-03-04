SELECT IF(
    A.FT_EXIST = 0
    and B.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and F.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and G.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and H.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and I.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and J.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    and K.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1
    , "OK"
    , "NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_perco_q1_2021 WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') B,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_ZEMBLAREPORT WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') J,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_MYWAY_REPORT WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') K,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_MARKETING_DATAMART WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') F,
(SELECT COUNT(distinct transfer_datetime) FT_EXIST FROM cdr.spark_it_omny_transactions WHERE transfer_datetime between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') G,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') H,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') I
