SELECT IF( (A.CHECK_FILE_COUNT >=24 and B.CHECK_FILE_ALL_EXIST > 0) AND C.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(distinct original_file_name) CHECK_FILE_COUNT FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' and original_file_name not like '%POST%'
 and FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(original_file_name, -14, 8),'yyyyMMdd'), 'yyyy-MM-dd')='###SLICE_VALUE###' GROUP BY SUBSTRING(original_file_name, -14, 8)) A,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' ) B,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
 FROM
   (
      SELECT
         A.FILE_NAME
      FROM
         (
            SELECT replace(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE in ( ${hivevar:list_cdr_type})
            UNION
            SELECT replace(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE in ( ${hivevar:list_cdr_type})
         ) A
   )C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM RECEIVED_FILES B
      WHERE
         ORIGINAL_FILE_MONTH BETWEEN DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM') AND FILE_TYPE in (${hivevar:list_cdr_type})
         AND TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###'
         AND B.ORIGINAL_FILE_NAME = C.FILE_NAME
   )
) C
;
