SELECT IF(T_1.FT_A_COUNT = 0 and T_2.FT_COUNT > 1 and T_3.NB_INSERT_DATE= 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_A_COUNT FROM AGG.SPARK_FT_A_DATA_TRANSFER WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_DATA_TRANSFER WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(distinct insert_date) NB_INSERT_DATE FROM MON.SPARK_FT_DATA_TRANSFER WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_3

