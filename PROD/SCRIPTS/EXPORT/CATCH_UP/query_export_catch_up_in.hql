SELECT C.FILE_NAME MISSING_FILES
 FROM
   (
      SELECT
         A.FILE_NAME
      FROM
         (
            SELECT replace(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = '${hivevar:cdr_type}'
            UNION
            SELECT replace(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}'
         ) A
   )C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM ${hivevar:it_table_name} B
      WHERE
         ${hivevar:it_partition_column} BETWEEN DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}) AND '###SLICE_VALUE###' AND B.ORIGINAL_FILE_NAME = C.FILE_NAME
   )