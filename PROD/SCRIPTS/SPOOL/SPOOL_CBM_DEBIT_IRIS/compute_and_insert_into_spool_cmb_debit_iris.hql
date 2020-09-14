INSERT INTO SPOOL.SPOOL_CMB_DEBIT_IRIS
SELECT
acct_code,
acc_nbr,
acct_book_id,
acct_res_code,
pre_real_balance,
(charge/100) charge,
pre_exp_date,
days,
channel_id,
nq_create_date,
transactionsn,
provider_id,
prepay_flag,
loan_amount,
commission_amount,
CURRENT_TIMESTAMP insert_date,
create_date,
'###SLICE_VALUE###' AS EVENT_DATE
FROM cdr.spark_it_zte_adjustment where file_date='###SLICE_VALUE###' and  channel_id ='21' and acct_res_code = '1'