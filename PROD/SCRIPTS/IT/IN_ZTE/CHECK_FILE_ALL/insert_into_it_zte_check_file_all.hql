INSERT INTO CDR.IT_ZTE_CHECK_FILE_ALL PARTITION(FILE_DATE)
SELECT
 replace(replace(replace(CDR_NAME, '.cdr', '.csv'), 'LoanCdr_ec', 'in_pr_ec') , 'LoanCdr_ed', 'in_pr_ed') CDR_NAME,
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
 WHEN CDR_NAME like '%LoanCdr_ec%' THEN 'ZTE_EC_CDR'
 WHEN CDR_NAME like '%LoanCdr_ed%' THEN 'ZTE_ED_CDR'
 WHEN CDR_NAME like '%recurr%' THEN 'ZTE_RECURR_CDR'
 WHEN CDR_NAME like '%in_pr_bal_reset%' THEN 'ZTE_BALRESET_CDR'
 ELSE 'UNKNOWN VALUE'
 END ) FILE_TYPE,
 ORIGINAL_FILE_NAME,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, 27, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
 CURRENT_TIMESTAMP INSERT_DATE,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(regexp_extract(CDR_NAME, '\\d{8}',0),'yyyyMMdd'))) FILE_DATE 
FROM CDR.TT_ZTE_CHECK_FILE_ALL C
--LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
--WHERE T.FILE_NAME IS NULL
