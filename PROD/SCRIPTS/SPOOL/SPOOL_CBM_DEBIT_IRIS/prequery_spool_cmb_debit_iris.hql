SELECT IF(T_1.SPOOL_COUNT = 0 AND T_2.it_zte_adjustment >= 10 ,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_CMB_DEBIT_IRIS WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(select count(*) it_zte_adjustment from CDR.spark_it_zte_adjustment where file_date='###SLICE_VALUE###') T_2

