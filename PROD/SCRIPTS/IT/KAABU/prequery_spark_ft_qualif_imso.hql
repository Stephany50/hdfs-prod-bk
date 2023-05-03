SELECT IF(A.nbr = 0  and B.nbr >= 1 ,'OK','NOK') FROM
(SELECT COUNT(*) nbr FROM mon.spark_ft_qualif_imso WHERE event_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr FROM mon.spark_ft_qualif_imso WHERE event_date=DATE_SUB('###SLICE_VALUE###',1)) B,