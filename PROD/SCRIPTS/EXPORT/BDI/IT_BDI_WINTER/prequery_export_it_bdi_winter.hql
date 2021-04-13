SELECT IF(
    A.it_bdi > 0
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) it_bdi FROM CDR.SPARK_IT_BDI WHERE original_file_date  ='###SLICE_VALUE###') A,
(SELECT COUNT(*) NB_EXPORT FROM
(SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='LOAD_EXPORT_IT_BDI_WINTER' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) B
