SELECT IF(A.nb_A = 0 and B.nb_B > 0,'OK','NOK') FROM
(SELECT COUNT(*) nb_A FROM AGG.SPARK_FT_BDI_OM_KYA WHERE event_date='###SLICE_VALUE###') A,
(SELECT count(*) nb_B FROM  MON.SPARK_FT_BDI_OM_KYA WHERE event_date='###SLICE_VALUE###') B
