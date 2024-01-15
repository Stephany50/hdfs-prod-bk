SELECT IF(T_1.TABLE_COUNT = 0 AND T_3.IT_OMNY_CHANNEL_USERS > 0,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) TABLE_COUNT FROM MON.SPARK_FT_OMNY_USER_REGISTRATION WHERE ORIGINAL_FILE_DATE= '###SLICE_VALUE###' and original_file_name like '%ChannelUsers_%') T_1,
(SELECT COUNT(*) IT_OMNY_CHANNEL_USERS FROM CDR.SPARK_IT_OMNY_ChannelUsers WHERE ORIGINAL_FILE_DATE="###SLICE_VALUE###") T_3