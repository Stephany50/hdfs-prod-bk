INSERT INTO CDR.SPARK_IT_BILL_DELIVERY
SELECT
account_code,
msisdn,
customer_name,
customer_category,
bill_amount,
account_status,
email_address,
delivery_method,
delivery_statut,
failed_reason,
trim(ORIGINAL_FILE_NAME) AS ORIGINAL_FILE_NAME,
current_timestamp() AS INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) bill_month
FROM CDR.SPARK_TT_BILL_DELIVERY C
  LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_BILL_DELIVERY) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL


