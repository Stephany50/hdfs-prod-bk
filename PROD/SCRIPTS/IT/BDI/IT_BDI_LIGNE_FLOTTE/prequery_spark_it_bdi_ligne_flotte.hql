SELECT IF(A.nb_A = 0  and B.nb_B > 0 and C.nb_C > 0 AND E.nb_E > 0 AND F.nb_F >= 12000000  
AND D.hlr_nb_file_is_ok=6 AND G.nb_G > 0 
,'OK','NOK') FROM
(SELECT COUNT(*)  nb_A FROM CDR.SPARK_IT_BDI_LIGNE_FLOTTE WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT count(*) nb_B FROM CDR.SPARK_IT_BDI_FULL WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) B,
(SELECT COUNT(*) nb_C FROM CDR.SPARK_IT_BDI_CRM_B2C WHERE original_file_date= '###SLICE_VALUE###' )C,
(SELECT count(distinct  original_file_name) hlr_nb_file_is_ok FROM CDR.SPARK_IT_BDI_HLR WHERE original_file_date='###SLICE_VALUE###') D,
(SELECT COUNT(*) nb_E FROM CDR.SPARK_IT_BDI_HLR WHERE original_file_date='###SLICE_VALUE###') E,
(SELECT COUNT(*) nb_F FROM CDR.SPARK_IT_BDI_ZSMART  WHERE original_file_date='###SLICE_VALUE###')F,
(SELECT COUNT(*) nb_G FROM CDR.SPARK_IT_BDI_PERS_MORALE WHERE original_file_date='###SLICE_VALUE###') G


