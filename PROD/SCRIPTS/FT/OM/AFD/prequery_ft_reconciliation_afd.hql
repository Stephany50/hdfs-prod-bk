SELECT IF(A.nbr = 0  and B.nbr > 0 ,'OK','NOK') FROM
(SELECT COUNT(*) nbr FROM MON.SPARK_FT_RECONCILIATION_AFD where transfer_datetime='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr FROM cdr.spark_it_omny_transactions WHERE file_date='###SLICE_VALUE###') B