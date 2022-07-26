SELECT
msisdn,
price_plan_code,
amount,
transaction_type,
contact_channel,
transaction_date,
original_file_name,
original_file_size,
original_file_line_count,
insert_date,
fee,
original_file_date
FROM CDR.SPARK_IT_ZTE_LOAN_CDR
WHERE original_file_date = '###SLICE_VALUE###'
