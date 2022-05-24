SELECT IF(
    A.ft_bdi2 >= 10
    AND flotte >= 10
    AND pp >= 10
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) ft_bdi2 FROM CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date  ='###SLICE_VALUE###') A,
(SELECT COUNT(*) flotte FROM MON.SPARK_FT_KYC_BDI_FLOTTE WHERE event_date  = DATE_SUB('###SLICE_VALUE###',1)) flotte,
(SELECT COUNT(*) pp FROM mon.spark_ft_kyc_bdi_pp WHERE event_date  = DATE_SUB('###SLICE_VALUE###',1)) pp,
(SELECT COUNT(*) NB_EXPORT FROM
(SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='LOAD_EXPORT_FT_BDI_CBM' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) B
