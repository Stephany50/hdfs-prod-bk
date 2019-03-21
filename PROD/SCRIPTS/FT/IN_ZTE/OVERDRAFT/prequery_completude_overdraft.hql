-- ---***********************************************************---
---------Prequery FT OVERDRAFT ---------------------------
-------- ARNOLD CHUENFFO 08-02-2019 
---***********************************************************---
SELECT IF(T_1.CHECK_FILE_EXIST > 0 AND T_2.CHECK_FILE_ALL_EXIST > 0 AND T_3.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(*) CHECK_FILE_EXIST FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###'
AND (CDR_TYPE = 'ZTE_TRANSFER_CDR' OR CDR_TYPE = 'ZTE_RECHARGE_CDR' OR CDR_TYPE = 'ZTE_ADJUSTMENT_CDR') ) T_1,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###'
AND (FILE_TYPE = 'ZTE_TRANSFER_CDR' OR FILE_TYPE = 'ZTE_RECHARGE_CDR' OR FILE_TYPE = 'ZTE_ADJUSTMENT_CDR')) T_2,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
 FROM
 (
 SELECT
 A.FILE_NAME
 FROM
 (
 SELECT replace(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###'
 AND (CDR_TYPE = 'ZTE_TRANSFER_CDR' OR CDR_TYPE = 'ZTE_RECHARGE_CDR' OR CDR_TYPE = 'ZTE_ADJUSTMENT_CDR')
 UNION
 SELECT replace(FILE_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###'
 AND (FILE_TYPE = 'ZTE_TRANSFER_CDR' OR FILE_TYPE = 'ZTE_RECHARGE_CDR' OR FILE_TYPE = 'ZTE_ADJUSTMENT_CDR')
 ) A
 )C
 WHERE
 NOT EXISTS
 (
 SELECT 1 FROM RECEIVED_FILES B
 WHERE
 ORIGINAL_FILE_MONTH BETWEEN DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
  AND (FILE_TYPE = 'ZTE_TRANSFER_CDR' OR FILE_TYPE = 'ZTE_RECHARGE_CDR' OR FILE_TYPE = 'ZTE_ADJUSTMENT_CDR')
  AND TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###'
  AND B.ORIGINAL_FILE_NAME = C.FILE_NAME
 )
) T_3
;

