CREATE TABLE SPOOL.SPOOL_CMB_DEBIT_IRIS(
acct_code varchar(16),
acc_nbr varchar(30),
acct_book_id bigint     ,
acct_res_code varchar(16),
pre_real_balance bigint,
charge bigint,
pre_exp_date timestamp,
days int,
channel_id int,
nq_create_date timestamp,
transactionsn varchar(25),
provider_id int,
prepay_flag int,
loan_amount bigint,
commission_amount bigint,
insert_date timestamp,
create_date date
) PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')