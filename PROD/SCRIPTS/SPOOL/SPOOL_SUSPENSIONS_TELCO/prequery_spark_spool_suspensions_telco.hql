SELECT IF(T_1.SPOOL_COUNT = 0 AND T_1_PREV.SPOOL_PREV >= 10 AND
          A.nb_bdi >= 10 AND B.nb_in >= 10
          AND C.nb_loc >= 10 AND D.nb_zsmart >= 10,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPARK_SPOOL_SUSPENSIONS_TELCO WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) SPOOL_PREV FROM SPOOL.SPARK_SPOOL_SUSPENSIONS_TELCO WHERE EVENT_DATE=date_sub('###SLICE_VALUE###',1)) T_1_PREV,
    (select count(*) nb_bdi from MON.SPARK_FT_BDI where event_date='###SLICE_VALUE###') A,
    (select count(*) nb_in from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date='###SLICE_VALUE###') B,
    (select count(*) nb_loc from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date='###SLICE_VALUE###') C,
    (select count(*) nb_zsmart from CDR.SPARK_IT_BDI_ZSMART where ORIGINAL_FILE_DATE=date_add('###SLICE_VALUE###',1)) D

