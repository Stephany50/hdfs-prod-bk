SELECT IF(T_1.SPOOL_COUNT = 0 AND T_2.FT_COUNT > 0,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_CBM_ACTIVATION_LOCATION_DAILY WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_ACTIVATIONS_SITE_DAY WHERE ACTIVATION_DATE='###SLICE_VALUE###') T_2