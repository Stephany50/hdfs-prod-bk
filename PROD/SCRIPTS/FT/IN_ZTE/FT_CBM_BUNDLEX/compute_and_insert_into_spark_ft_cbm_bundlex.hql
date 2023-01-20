INSERT INTO MON.SPARK_FT_CBM_BUNDLEX PARTITION(CREATEDDATE)
select 
MSISDN MSISDN, 
SUB_ACCOUNT_DEPART SUB_ACCOUNT_DEPART, 
TYPE_DEPART TYPE_DEPART, 
VOLUME_DEPART VOLUME_DEPART, 
FROM_UNIXTIME(UNIX_TIMESTAMP(VALIDITE_DEPART, 'yyyy-MM-dd HH:mm:ss')) VALIDITE_DEPART, 
TYPE_ACTION TYPE_ACTION, 
COST COST, 
SUB_ACCOUNT_FIN SUB_ACCOUNT_FIN, 
TYPE_FIN TYPE_FIN, 
VOLUME_FIN VOLUME_FIN, 
FROM_UNIXTIME(UNIX_TIMESTAMP(VALIDITE_FIN, 'yyyy-MM-dd HH:mm:ss')) VALIDITE_FIN, 
FROM_UNIXTIME(UNIX_TIMESTAMP(DATETIME, 'yyyy-MM-dd HH:mm:ss')) DATETIME, 
FROM_UNIXTIME(UNIX_TIMESTAMP(INSERTED_DATE, 'yyyy-MM-dd HH:mm:ss')) INSERTED_DATE,  
ORIGINAL_FILE_NAME ORIGINAL_FILE_NAME, 
CREATEDDATE CREATEDDATE 
from (
select distinct
C.msisdn  MSISDN, 
D.BALANCE_NAME  SUB_ACCOUNT_DEPART, 
D.TYPE_BALANCE  TYPE_DEPART,
case when D.TYPE_BALANCE = "VOIX" then abs(C.input_value)/100 else IF(D.TYPE_BALANCE = "DATA", abs(C.input_value)/1048576, abs(C.input_value)) end VOLUME_DEPART,
NULL VALIDITE_DEPART, 
"CONVERSION"  TYPE_ACTION,
abs(C.charge_fee) COST,
OD.BALANCE_NAME  SUB_ACCOUNT_FIN,
case when D.TYPE_BALANCE = "VOIX" then "DATA" else IF(D.TYPE_BALANCE = "DATA", "VOIX", NULL) end TYPE_FIN,
case when D.TYPE_BALANCE = "DATA" then abs(C.output_value)/100 else IF(D.TYPE_BALANCE = "VOIX", abs(C.output_value)/1048576, abs(C.output_value)) end VOLUME_FIN,
NULL VALIDITE_FIN,
C.datetime DATETIME,
CURRENT_TIMESTAMP INSERTED_DATE,
C.original_file_name ORIGINAL_FILE_NAME,
C.CREATEDDATE CREATEDDATE
from CDR.SPARK_IT_CONVERTBALANCE C 
left join DIM.DIM_MAPPING_BALANCE_BUNDLEX D on C.INPUT_BALANCE=D.BALANCE_CODE 
left join DIM.DIM_MAPPING_BALANCE_BUNDLEX OD on C.OUTPUT_BALANCE=OD.BALANCE_CODE 
where C.createddate = "###SLICE_VALUE###"

UNION ALL

select distinct
A.acc_nbr MSISDN,
D.BALANCE_NAME SUB_ACCOUNT_DEPART, 
D.TYPE_BALANCE  TYPE_DEPART,
case when D.TYPE_BALANCE = "VOIX" then A.pre_real_balance else A.pre_real_balance end VOLUME_DEPART,
-- case when D.TYPE_BALANCE = "VOIX" then abs(NVL(A.pre_real_balance_new, 0))/100 else IF(D.TYPE_BALANCE = "DATA", abs(NVL(A.pre_real_balance_new, 0))/1048576, NULL end VOLUME_DEPART,
A.pre_exp_date VALIDITE_DEPART,
"PROLONGATION"  TYPE_ACTION,
abs(B.charge)/100 COST,
D.BALANCE_NAME SUB_ACCOUNT_FIN, 
D.TYPE_BALANCE  TYPE_FIN,
case when D.TYPE_BALANCE = "VOIX" then A.pre_real_balance else A.pre_real_balance end VOLUME_FIN, 
-- case when D.TYPE_BALANCE = "VOIX" then abs(NVL(A.pre_real_balance_new, 0))/100 else IF(D.TYPE_BALANCE = "DATA", abs(NVL(A.pre_real_balance_new, 0))/1048576, NULL end VOLUME_FIN,
FROM_UNIXTIME(UNIX_TIMESTAMP(
CONCAT(DATE_ADD(A.pre_exp_date, NVL(cast(SPLIT(A.days, ',')[0] as int), 0)), " ", SPLIT(A.pre_exp_date, ' ')[1]), 
'yyyy-MM-dd HH:mm:ss'))  VALIDITE_FIN,
A.nq_create_date DATETIME,
CURRENT_TIMESTAMP  INSERTED_DATE,
A.original_file_name ORIGINAL_FILE_NAME,
A.CREATE_DATE CREATEDDATE
from 
(select * from CDR.SPARK_IT_ZTE_ADJUSTMENT where create_date = "###SLICE_VALUE###"  and channel_id = 112) A 
left join (select * from CDR.SPARK_IT_ZTE_ADJUSTMENT where create_date = "###SLICE_VALUE###" and channel_id = 112) B on A.acc_nbr = B.acc_nbr AND A.nq_create_date = B.nq_create_date
left join DIM.DIM_MAPPING_BALANCE_BUNDLEX D on A.acct_res_code=D.BALANCE_ID  
where B.acct_res_code != A.acct_res_code and B.acct_res_code = 1
)
 