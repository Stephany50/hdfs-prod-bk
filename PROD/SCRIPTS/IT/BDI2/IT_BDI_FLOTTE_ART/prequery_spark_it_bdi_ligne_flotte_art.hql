SELECT IF(A.nb_A = 0  and B.nb_B>0 and C.nb_C>0 
,'OK','NOK') FROM
(SELECT COUNT(*)  nb_A FROM CDR.SPARK_IT_BDI_FLOTTE_ART WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT COUNT(*)  nb_B FROM Mon.spark_ft_bdi_art WHERE event_date='###SLICE_VALUE###') B,
(SELECT COUNT(*)  nb_C FROM cdr.spark_it_bdi_art where original_file_date=date_add(to_date('###SLICE_VALUE###'),1)) C




