SELECT IF(
    A.ft_bdi2 >= 10
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) ft_bdi2 FROM MON.SPARK_FT_BDI_CRM_B2C WHERE event_date  ='###SLICE_VALUE###') A,
(SELECT COUNT(*) NB_EXPORT FROM
(SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='LOAD_EXPORT_FT_BDI_CBM' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) B
