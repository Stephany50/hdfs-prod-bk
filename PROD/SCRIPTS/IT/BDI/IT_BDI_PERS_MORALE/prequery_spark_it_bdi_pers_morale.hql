SELECT IF(A.nb_A = 0  and B.nb_B > 0 and C.nb_C > 0
and D.nb_D >0 AND E.nb_E > 0 ,'OK','NOK') FROM
(SELECT COUNT(*) nb_A FROM CDR.SPARK_IT_BDI_PERS_MORALE WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nb_B FROM CDR.SPARK_IT_BDI_CRM_B2C WHERE original_file_date= '###SLICE_VALUE###' ) B,
(SELECT COUNT(*) nb_C FROM CDR.SPARK_IT_BDI_CRM_B2B WHERE original_file_date='###SLICE_VALUE###') C,
(SELECT count(*) nb_D FROM CDR.SPARK_IT_BDI_FULL WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) D,
(SELECT COUNT(*) nb_E FROM CDR.SPARK_IT_BDI_PERS_MORALE WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) E
