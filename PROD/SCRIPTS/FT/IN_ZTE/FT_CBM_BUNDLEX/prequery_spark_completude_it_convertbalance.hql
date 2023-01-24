SELECT IF(T_3.CHECK_FILE_ALL_EXIST > 0 AND T_6.CHECK_FILE_ALL_EXIST_AND_TYPE > 0 AND T_4.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###') T_3,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST_AND_TYPE FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}') T_6,
(
SELECT COUNT(C.FILE_NAME) MISSING_FILES
 FROM
   (
      SELECT
         A.FILE_NAME
      FROM
         (
            SELECT replace(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}'
         ) A
   ) C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM ${hivevar:it_table_name} B
      WHERE
         ${hivevar:it_partition_column} ='###SLICE_VALUE###' AND   B.ORIGINAL_FILE_NAME = C.FILE_NAME
   )
) T_4