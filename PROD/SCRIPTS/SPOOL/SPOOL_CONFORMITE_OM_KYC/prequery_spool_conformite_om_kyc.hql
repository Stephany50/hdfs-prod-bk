SELECT IF(T_1.SPOOL_COUNT = 0 AND
          A.nb_mois1 >= 10 AND B.nb_mois2 >= 10 AND C.nb_mois3 >=10,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_CONFORMITE_OM_KYC WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (select count(*) nb_mois1 from MON.SPARK_STATIC_FT_OM_ACTIVE_USER where event_date=date_sub(current_date, 1)) A,
    (select count(*) nb_mois2 from MON.SPARK_STATIC_FT_OM_ACTIVE_USER where event_date=date_sub(current_date, 31)) B,
    (select count(*) nb_mois3 from MON.SPARK_STATIC_FT_OM_ACTIVE_USER where event_date=date_sub(current_date, 61)) C

