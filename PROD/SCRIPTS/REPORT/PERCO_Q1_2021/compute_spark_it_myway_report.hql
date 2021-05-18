INSERT INTO CDR.SPARK_IT_MYWAY_REPORT
SELECT
 sb_id,
 sb_msisdn,
 sb_type ,
 sb_amount_data,
 sb_amount_onnet,
 sb_amount_allnet,
 sb_created_at,
 sb_treated_by,
 sb_treated_at,
 sb_contact_chanel,
 sb_error,
 sb_payment_means,
 sb_envoi_bonus_allnet,
 sb_envoi_bonus_onnet,
 sb_envoi_cout,
 sb_envoi_validity,
 sb_envoi_bonus_data,
 sb_txn_id,
 sb_identical,
 sb_status_in,
 sb_status_tango,
 sb_validity,
 sb_canal    ,
 sb_channel_msisdn,
 trim(ORIGINAL_FILE_NAME) AS ORIGINAL_FILE_NAME,
 CURRENT_TIMESTAMP() INSERT_DATE,
 TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) EVENT_DATE
FROM CDR.SPARK_TT_MYWAY_REPORT C
  LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_MYWAY_REPORT WHERE EVENT_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE ) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL