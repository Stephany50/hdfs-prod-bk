SELECT IF(A.nbr = 0  and B.nbr >= 1 and C.nbr >= 1 and D.nbr >= 1 ,'OK','NOK') FROM
(SELECT COUNT(*) nbr FROM mon.spark_ft_qualif_imso WHERE event_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr FROM mon.spark_ft_qualif_imso WHERE event_date=DATE_SUB('###SLICE_VALUE###',1)) B,
(SELECT COUNT(*) nbr FROM MON.SPARK_FT_KYC_BDI_PP WHERE event_date='###SLICE_VALUE###') C,
(SELECT COUNT(*) nbr FROM cdr.spark_it_qualif_imso WHERE original_file_date='###SLICE_VALUE###') D