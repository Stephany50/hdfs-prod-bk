SELECT concat(split(C.FILE_NAME, '.csv')[0], '_', replace('###SLICE_VALUE###', '-', ''), '.csv') MISSING_FILES 
FROM
(
   SELECT DISTINCT reverse(split(reverse(CDR_NAME), '[/]')[0]) FILE_NAME FROM CDR.SPARK_IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '###SLICE_VALUE###' and CDR_NAME like '%${hivevar:cdr_zebra_name}%'
) C
WHERE
   NOT EXISTS
   (
      SELECT 1  FROM ${hivevar:it_table_name} B
      WHERE
         ${hivevar:it_partition_column} BETWEEN DATE_SUB('###SLICE_VALUE###', ${hivevar:date_offset}) AND '###SLICE_VALUE###' 
         AND TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###'
         AND split(B.ORIGINAL_FILE_NAME, '_')[0] = split(C.FILE_NAME, '\\.')[0]
   )