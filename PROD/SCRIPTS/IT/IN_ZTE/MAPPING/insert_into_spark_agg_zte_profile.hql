INSERT INTO AGG.SPARK_ZTE_PROFILE

SELECT
        PROFILE_ID,
        MAX(UPPER(PROFILE_NAME)) PROFILE_NAME,
        date_sub("###SLICE_VALUE###",1)  MAX_ORIGINAL_FILE_DATE
FROM CDR.SPARK_IT_ZTE_PROFILE  where  ORIGINAL_FILE_DATE<= '###SLICE_VALUE###' and profile_id not like "%Profile ID%"
GROUP BY PROFILE_ID