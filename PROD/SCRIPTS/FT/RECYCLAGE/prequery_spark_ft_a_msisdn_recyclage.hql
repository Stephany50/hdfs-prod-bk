<<<<<<< Updated upstream
SELECT IF( FTA=0 and FT>10000, 'OK', 'NOK' ) TABLES_STATES
FROM ( SELECT COUNT(*) FTA FROM AGG.SPARK_FT_A_MSISDN_RECYCLAGE WHERE EVENT_DATE='###SLICE_VALUE###' ) T,
( SELECT COUNT(*) FT FROM MON.SPARK_FT_MSISDN_RECYCLAGE WHERE EVENT_DATE='###SLICE_VALUE###'  ) A
=======
SELECT IF( FTA=0 and FT>10000, 'OK', 'NOK' ) TABLES_STATES
FROM ( SELECT COUNT(*) FTA FROM AGG.SPARK_FT_A_MSISDN_RECYCLAGE WHERE EVENT_DATE='###SLICE_VALUE###' ) T,
( SELECT COUNT(*) FT FROM MON.SPARK_FT_MSISDN_RECYCLAGE WHERE EVENT_DATE='###SLICE_VALUE###'  ) A
>>>>>>> Stashed changes
