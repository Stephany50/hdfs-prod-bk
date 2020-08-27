SELECT IF(T_1.SPOOL_COUNT = 0 AND  A.nb_mois1 >= 10 AND B.nb_mois2 >= 10 AND C.nb_mois3 >=10  AND D.nb_om >=10 AND E.nb_in >= 10,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_CMB_DEBIT_IRIS WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(select count(*) it_zte_adjustment from CDR.spark_it_zte_adjustment where file_date='###SLICE_VALUE###') T2

