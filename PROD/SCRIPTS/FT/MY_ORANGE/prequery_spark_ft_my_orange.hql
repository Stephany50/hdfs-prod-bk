SELECT IF(
    A.FT_EXIST = 0
    and B.IT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    --and C.IT_EXIST>0
    and D.FT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    and E.FT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    and F.FT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    and G.FT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    and H.FT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    and I.FT_EXIST = datediff('###SLICE_VALUE###', '${hivevar:date_debut_perco}') + 1
    , "OK"
    , "NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_MY_ORANGE WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(distinct event_date) IT_EXIST FROM CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND WHERE EVENT_DATE between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') B,
--(SELECT COUNT(*) IT_EXIST FROM CDR.SPARK_IT_MY_ORANGE_USERS_FOLLOW WHERE EVENT_DATE='###SLICE_VALUE###') C,
(SELECT COUNT(distinct period) FT_EXIST FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE period between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') D,
(SELECT COUNT(distinct period) FT_EXIST FROM MON.SPARK_FT_CBM_DA_USAGE_DAILY WHERE PERIOD between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') E,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_MARKETING_DATAMART WHERE EVENT_DATE between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') F,
(SELECT COUNT(distinct transfer_datetime) FT_EXIST FROM cdr.spark_it_omny_transactions WHERE transfer_datetime between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') G,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') H,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###') I
