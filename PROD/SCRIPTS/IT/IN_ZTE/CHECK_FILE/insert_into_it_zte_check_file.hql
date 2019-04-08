INSERT INTO CDR.IT_ZTE_CHECK_FILE PARTITION(CDR_DATE)
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
 END ) CDR_TYPE,
 CDR_GEN_DATE,
 CDR_FILE_SIZE,
 CDR_COUNT CDR_LINE_COUNT,
 ORIGINAL_FILE_NAME,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -14, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
 CURRENT_TIMESTAMP INSERT_DATE,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(regexp_extract(CDR_NAME, '\\d{8}',0),'yyyyMMdd'))) CDR_DATE 
FROM CDR.TT_ZTE_CHECK_FILE C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM')
    AND (B.FILE_TYPE = 'ZTE_CHECKFILE_PREPAID' OR B.FILE_TYPE = 'ZTE_CHECKFILE_POSTPAID')  AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
);


