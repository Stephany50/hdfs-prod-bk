SELECT IF(T_1.SPOOL_COUNT = 0 AND
          A.nb_bal >= 10 AND B.nb_bar >= 10,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPARK_SPOOL_BLOCAGE_OM WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (select count(*) nb_bal from CDR.SPARK_OMNY_ALL_BALANCE where ORIGINAL_FILE_DATE=date_add('###SLICE_VALUE###',1)) A,
    (select count(*) nb_bar from CDR.SPARK_IT_OMNY_BARRED_ACCOUNTS_IMPORTED where ORIGINAL_FILE_DATE=date_add('###SLICE_VALUE###',1)) B
