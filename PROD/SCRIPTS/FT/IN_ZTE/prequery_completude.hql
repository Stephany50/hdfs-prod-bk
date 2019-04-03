SELECT IF((T_1.CHECK_FILE_COUNT > 23 AND T_2.CHECK_FILE_EXIST > 0 AND T_3.CHECK_FILE_ALL_EXIST > 0) AND T_4.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(*) CHECK_FILE_COUNT FROM RECEIVED_FILES WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = CONCAT('ZTE_CHECKFILE_','${hivevar:flux_type}')) T_1,
(SELECT COUNT(*) CHECK_FILE_EXIST FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = '${hivevar:cdr_type}') T_2,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}') T_3,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
 FROM
   (
      SELECT
         A.FILE_NAME
      FROM
         (
            SELECT replace(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = '${hivevar:cdr_type}'
            UNION
            SELECT replace(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = '${hivevar:cdr_type}'
         ) A
   )C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM RECEIVED_FILES B
      WHERE
         ORIGINAL_FILE_MONTH BETWEEN DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM') AND FILE_TYPE = '${hivevar:cdr_type}'
         AND TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###'
         AND B.ORIGINAL_FILE_NAME = C.FILE_NAME
   )
) T_4
;