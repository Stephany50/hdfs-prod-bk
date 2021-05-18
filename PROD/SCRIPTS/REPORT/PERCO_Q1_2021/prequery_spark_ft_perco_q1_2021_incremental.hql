SELECT IF(
    A.FT_EXIST = 0
    and B.FT_EXIST > 0
    and C.FT_EXIST > 0
    , "OK"
    , "NOK"
) FROM
    (SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_perco_q1_2021_incremental WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_EXIST FROM mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE period ='###SLICE_VALUE###') B,
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_marketing_datamart WHERE EVENT_DATE ='###SLICE_VALUE###') C
