SELECT IF(A.nb_A = 0  and B.nb_B > 0 and C.nb_C > 0,'OK','NOK') FROM
(SELECT COUNT(*) nb_A FROM CDR.SPARK_IT_BDI_FULL c WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT COUNT(*)  nb_B FROM CDR.SPARK_IT_BDI_LIGNE_FLOTTE WHERE original_file_date='###SLICE_VALUE###') B,
(SELECT COUNT(*)  nb_C FROM CDR.SPARK_IT_BDI WHERE original_file_date='###SLICE_VALUE###') C

