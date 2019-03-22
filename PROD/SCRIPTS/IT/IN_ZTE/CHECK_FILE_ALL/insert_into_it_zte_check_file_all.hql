INSERT INTO CDR.IT_ZTE_CHECK_FILE_ALL PARTITION(FILE_DATE)
SELECT
 CDR_NAME,
 ( CASE 
 WHEN CDR_NAME like '%postpaid_pr%sms%' THEN 'ZTE_VOICE_SMS_POST_CDR'
 WHEN CDR_NAME like '%ab%voice%' THEN 'ZTE_AB_VOICE_SMS_CDR'
 WHEN CDR_NAME like '%postpaid_pr%voice%' THEN 'ZTE_VOICE_SMS_POST_CDR'
 WHEN CDR_NAME like '%in_pr%voice%' THEN 'ZTE_VOICE_SMS_CDR'
 WHEN CDR_NAME like '%in_pr%data%' THEN 'ZTE_DATA_CDR'
 WHEN CDR_NAME like '%in_postpaid_pr%data%' THEN 'ZTE_DATA_POST_CDR'
 WHEN CDR_NAME like '%in_pr_transfer%' THEN 'ZTE_TRANSFER_CDR'
 WHEN CDR_NAME like '%in_pr%sms%' THEN 'ZTE_VOICE_SMS_CDR'
 WHEN CDR_NAME like '%in_pr_adjustment%' THEN 'ZTE_ADJUSTMENT_CDR'
 WHEN CDR_NAME like '%in_pr_recharge%' THEN 'ZTE_RECHARGE_CDR'
 WHEN CDR_NAME like '%in_pr_subscription%' THEN 'ZTE_SUBSCRIPTION_CDR'
 WHEN CDR_NAME like '%in_ab%data%' THEN 'ZTE_AB_DATA_CDR'
 WHEN CDR_NAME like '%in_pr_ec%' THEN 'ZTE_EC_CDR'
 WHEN CDR_NAME like '%in_pr_ed%' THEN 'ZTE_ED_CDR'
 WHEN CDR_NAME like '%recurr%' THEN 'ZTE_RECURR_CDR'
 ELSE 'UNKNOWN VALUE'
 END ) FILE_TYPE,
 ORIGINAL_FILE_NAME,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
 CURRENT_TIMESTAMP INSERT_DATE,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(regexp_extract(CDR_NAME, '\\d{8}',0),'yyyyMMdd'))) FILE_DATE FROM CDR.TT_ZTE_CHECK_FILE_ALL;
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM')
    AND B.FILE_TYPE = 'ZTE_CHECKFILE_ALL' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
)
;

--***************************
--- Log Filed received
-------------------------

INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT
  ORIGINAL_FILE_NAME,
  'ZTE_CHECKFILE_ALL' FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_ZTE_BALANCE_RESET C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM')
                   AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.FILE_TYPE = 'ZTE_CHECKFILE_ALL' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
GROUP BY ORIGINAL_FILE_NAME
;