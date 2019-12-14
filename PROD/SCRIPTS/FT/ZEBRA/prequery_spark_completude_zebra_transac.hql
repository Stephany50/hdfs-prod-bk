SELECT IF(T_1.CHECK_FILE_EXIST > 0 AND T_2.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(*) CHECK_FILE_EXIST FROM CDR.SPARK_IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '###SLICE_VALUE###' and CDR_NAME like '%Transac%') T_1,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
 FROM
   (
      SELECT DISTINCT reverse(split(reverse(CDR_NAME), '[/]')[0]) FILE_NAME FROM CDR.SPARK_IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '###SLICE_VALUE###' and CDR_NAME like '%Transac%'
   )C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM CDR.SPARK_IT_ZEBRA_TRANSAC B
      WHERE
         TRANSFER_DATE BETWEEN date_sub('###SLICE_VALUE###', ${hivevar:date_offset}) AND '###SLICE_VALUE###'
         AND TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###'
         AND split(B.ORIGINAL_FILE_NAME, '_')[0] = split(C.FILE_NAME, '\\.')[0]
   )
) T_2

