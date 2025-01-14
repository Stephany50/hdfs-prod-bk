SELECT IF(
    A.it_bdi_pers_morale > 0
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) it_bdi_pers_morale FROM mon.spark_ft_kyc_crm_B2B WHERE event_date  ='###SLICE_VALUE###') A,
(SELECT COUNT(*) NB_EXPORT FROM
(SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='LOAD_EXPORT_IT_BDI_PERS_MORALE_WINTER' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) B
