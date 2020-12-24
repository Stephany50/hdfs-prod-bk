SELECT IF(A.nb_A = 0  and B.nb_B=0 and C.nb_C=0
and D.nb_D >0 AND E.nb_E > 0 AND F.nb_F > 0  AND G.nb_G > 0 AND
H.ZSMART_SIZE_IS_OK>3700000000 AND I.hlr_nb_file_is_ok=6 AND J.nb_J > 0 AND K.nb_K > 0
and L.nb_L = 0
,'OK','NOK') FROM
(SELECT COUNT(*)  nb_A FROM CDR.spark_it_bdi WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT COUNT(*)  nb_B FROM CDR.SPARK_IT_BDI_LIGNE_FLOTTE WHERE original_file_date='###SLICE_VALUE###') B,
(SELECT COUNT(*) nb_C FROM CDR.SPARK_IT_BDI_PERS_MORALE WHERE original_file_date='###SLICE_VALUE###') C,
(SELECT COUNT(*) nb_D FROM CDR.SPARK_it_bdi_crm_b2c WHERE original_file_date= '###SLICE_VALUE###' )D,
(SELECT COUNT(*) nb_E FROM CDR.spark_it_bdi_hlr WHERE original_file_date='###SLICE_VALUE###') E,
(SELECT COUNT(*) nb_F FROM CDR.SPARK_IT_BDI_ZSMART  WHERE original_file_date='###SLICE_VALUE###')F,
(SELECT COUNT(*) nb_G FROM CDR.SPARK_IT_BDI_CRM_B2B WHERE original_file_date='###SLICE_VALUE###') G,
(SELECT max(original_file_size) ZSMART_SIZE_IS_OK FROM CDR.SPARK_IT_BDI_ZSMART  WHERE original_file_date='###SLICE_VALUE###') H,
(SELECT count(distinct  original_file_name) hlr_nb_file_is_ok FROM CDR.spark_it_bdi_hlr WHERE original_file_date='###SLICE_VALUE###') I,
(SELECT count(*) nb_J FROM CDR.SPARK_IT_BDI_FULL1 WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) J,
(SELECT COUNT(*) nb_K FROM CDR.SPARK_IT_BDI_PERS_MORALE WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) K,
(SELECT COUNT(*) nb_L FROM CDR.SPARK_IT_BDI_FULL1 WHERE original_file_date='###SLICE_VALUE###') L
