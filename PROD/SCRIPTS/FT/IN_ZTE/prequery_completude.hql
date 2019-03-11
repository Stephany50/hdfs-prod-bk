SELECT IF((T_1.CHECK_FILE_EXIST > 0 OR T_2.CHECK_FILE_ALL_EXIST > 0) AND T_3.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(*) CHECK_FILE_EXIST FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = '${hivevar:cdr_type}') T_1,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}') T_2,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
 FROM
   (
      SELECT
         A.FILE_NAME
      FROM
         (
            SELECT CDR_NAME FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = '${hivevar:cdr_type}'
            UNION
            SELECT FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}'
         ) A
   )C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM ${hivevar:table_name} B
      WHERE
         ${hivevar:partition_name} BETWEEN date_sub('###SLICE_VALUE###', ${hivevar:date_offset}) AND '###SLICE_VALUE###'
         AND TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###'
         AND B.ORIGINAL_FILE_NAME = C.FILE_NAME
   )
) T_3
;
