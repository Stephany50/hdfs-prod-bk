INSERT INTO CDR.SPARK_IT_DAILY_PAYMENT
SELECT
gl_code,
marche,
txn_no,
txn_type_code,
txn_type_name,
pay_amount,
cust_code,
cust_type_name,
acct_nbr,
cust_name,
to_date(invoice_date) invoice_date,
invoice_no,
cust_category,
payment_no,
payment_method_name,
check_no,
credit_card_no,
credit_note_no,
bank_code_no,
trim(ORIGINAL_FILE_NAME) AS ORIGINAL_FILE_NAME,
current_timestamp() AS INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) stat_date
FROM CDR.SPARK_TT_DAILY_PAYMENT C
  LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_DAILY_PAYMENT) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL


