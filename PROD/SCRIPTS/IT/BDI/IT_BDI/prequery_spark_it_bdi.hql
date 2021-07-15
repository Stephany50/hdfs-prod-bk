SELECT IF(A.nb_A = 0  and B.nb_B>0 and C.nb_C>0
and D.nb_D >0 AND E.nb_E > 0 AND F.nb_F >= 12000000 AND I.hlr_nb_file_is_ok=6,'OK','NOK') FROM
(SELECT COUNT(*)  nb_A FROM CDR.SPARK_IT_BDI WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT COUNT(*)  nb_B FROM CDR.SPARK_IT_BDI_LIGNE_FLOTTE WHERE original_file_date='###SLICE_VALUE###') B,
(SELECT count(*) nb_C FROM CDR.SPARK_IT_BDI_FULL WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) C,
(SELECT COUNT(*) nb_D FROM CDR.SPARK_it_bdi_crm_b2c WHERE original_file_date= '###SLICE_VALUE###' )D,
(SELECT COUNT(*) nb_E FROM CDR.spark_it_bdi_hlr WHERE original_file_date='###SLICE_VALUE###') E,
(SELECT COUNT(*) nb_F FROM CDR.SPARK_IT_BDI_ZSMART  WHERE original_file_date='###SLICE_VALUE###')F,
(SELECT count(distinct  original_file_name) hlr_nb_file_is_ok FROM CDR.spark_it_bdi_hlr WHERE original_file_date='###SLICE_VALUE###') I
