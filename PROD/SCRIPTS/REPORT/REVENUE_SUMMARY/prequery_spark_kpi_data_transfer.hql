SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_A_DATA_TRANSFER_EXIST>0 ,"OK","NOK") EXTRACT_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM AGG.SPARK_REVENUE_SUMMARY_DAILY WHERE EVENT_DATE='###SLICE_VALUE###' and source_data='FT_A_DATA_TRANSFER') T_1,
(SELECT COUNT(*) FT_A_DATA_TRANSFER_EXIST FROM AGG.SPARK_FT_A_DATA_TRANSFER WHERE EVENT_DATE = '###SLICE_VALUE###') T_2


