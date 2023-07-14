
add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar;

DROP TEMPORARY  FUNCTION  IF EXISTS fn_get_sorted_list_item;
create temporary function fn_get_sorted_list_item as 'cm.orange.bigdata.udf.FnGetSortedListItem';

DROP TEMPORARY  FUNCTION  IF EXISTS fn_format_msisdn_to_9digits;
create temporary function fn_format_msisdn_to_9digits as 'cm.orange.bigdata.udf.GetNnpMsisdn9Digits';

DROP TEMPORARY  FUNCTION  IF EXISTS FN_GET_NNP_MSISDN_SIMPLE_DESTN;
create temporary function FN_GET_NNP_MSISDN_SIMPLE_DESTN as 'cm.orange.bigdata.udf.GetNnpMsisdnSimpleDestn';

DROP TEMPORARY  FUNCTION  IF EXISTS FN_NNP_SIMPLE_DESTINATION;
create temporary function FN_NNP_SIMPLE_DESTINATION as 'cm.orange.bigdata.udf.GetNnpSimpleDestn';

--select * from tmp.daily_cbm_usage_solde;

DROP TABLE IF EXISTS tmp.daily_cbm_usage_solde;
CREATE TABLE IF NOT EXISTS tmp.daily_cbm_usage_solde as
-- TABLE EVALUEE A J-1
SELECT DISTINCT
-- MSISDN = ACC_NBR PRIS DANS SUBS_EXTRACT A J
A.acc_nbr AS SUBS_E_MSISDN,
A.acct_id AS SUBS_E_ACCT_ID, 

-- ETAT DES BALANCES A J ET J-1 PRIS DANS BAL_EXTRACT 
-- NON DES SOUS COMPTE VIA LA DIM.spark_DT_BALANCE_TYPE_ITEM (ACCT_RES_ID : ACCT_RES_NAME)
B.acct_res_id AS BAL_E_ACCT_RES_ID,
B.bal_id AS BAL_E_BAL_ID,
B.acct_res_name AS BAL_E_ACCT_RES_NAME,
B.acct_res_rating_service_code AS BAL_E_ACCT_RES_RATING_SERVICE_CODE,
B.remain_bal_start AS BAL_E_REMAIN_BAL_START,
B.remain_bal_end AS BAL_E_REMAIN_BAL_END,
B.eff_date AS BAL_E_EFF_DATE,
B.exp_date AS BAL_E_EXP_DATE,

-- DEPOT RECURRENT PRIS DANS IT_ZTE_RECURRING
C.MSISDN AS DR_MSISDN, 
C.BEN_ACCT_ID AS DR_ACCT_RES_ID, 
C.acct_res_name AS DR_ACCT_RES_NAME, 
C.acct_res_rating_service_code AS DR_ACCT_RES_RATING_SERVICE_CODE, 
C.PROFILE AS DR_PROFILE, 
C.DEPOT AS DR_DEPOT, 
C.BEN_ACCT_ADD_RESULT AS BEN_ACCT_ADD_RESULT, 
C.BEN_ACCT_ADD_VAL AS BEN_ACCT_ADD_VAL, 
C.USAGE_NORMAL AS DR_USAGE_NORMAL, 
C.USAGE_DIM AS DR_USAGE_DIM, 
C.UNITE AS DR_UNITE, 
C.BEN_ACCT_ADD_EXP_DATE AS DR_BEN_ACCT_ADD_EXP_DATE,

-- DEPOT VIA SOUSCRIPTION PRIS DANS FT_SUBSCRIPTION
--SUBS.MSISDN AS SUBSCRIP_MSISDN,
--SUBS.TRANSACTION_DATE AS SUBSCRIP_TRANSACTION_DATE,
--SUBS.transaction_time AS SUBSCRIP_TRANSACTION_TIME,
--SUBS.transactionsn AS SUBSCRIP_TRANSACTIONSN,
SUBS.ACCT_RES_ID AS SUBSCRIP_ACCT_RES_ID,
SUBS.BEN_ACCT_ADD_VAL AS SUBSCRIP_BEN_ACCT_ADD_VAL,
--SUBS.BEN_ACCT_ADD_RESULT AS SUBSCRIP_BEN_ACCT_ADD_RESULT,
--SUBS.BEN_ACCT_ADD_ACT_DATE AS SUBSCRIP_BEN_ACCT_ADD_ACT_DATE,
--SUBS.BEN_ACCT_ADD_EXP_DATE AS SUBSCRIP_BEN_ACCT_ADD_EXP_DATE,

-- USAGE DATA PRIS DANS CRA GPRS
--CRA.session_date AS CRA_SESSION_DATE,
-- CRA.session_time AS CRA_SESSION_TIME,
CRA.served_party_msisdn AS CRA_MSISDN,
CRA.USED_BAL_ID AS CRA_BAL_ID,
--CRA.USED_ACCT_ITEM_TYPE_ID AS CRA_USED_ACCT_ITEM_TYPE_ID,
CRA.USED_ACCT_RES_ID AS CRA_USED_ACCT_RES_ID,
CRA.used_balance AS CRA_USED_BALANCE,
CRA.bytes_sent AS CRA_BYTES_SENT,
CRA.bytes_received AS CRA_BYTES_RECEIVED,
CRA.charge_sum AS CRA_CHARGE_SUM,
CRA.used_volume AS CRA_USED_VOLUME,
--CRA.remaining_volume AS CRA_REMAINING_VOLUME ,
--CRA.preceding_volume AS CRA_PRECEDING_VOLUME ,

CURRENT_TIMESTAMP AS INSERT_DATE,
'2023-07-01' AS event_date
FROM 
(
SELECT DISTINCT
acct_id,
acc_nbr
FROM CDR.SPARK_IT_ZTE_SUBS_EXTRACT
WHERE original_file_date = DATE_ADD('2023-07-01', 1) 
and acc_nbr like '%699948312%'
) A
LEFT JOIN -- BAL_EXTRACT
(
select 
NVL(BAL1.bal_id, BAL2.bal_id) bal_id, 
NVL(BAL1.acct_id, BAL2.acct_id) acct_id, 
NVL(BAL1.acct_res_id, BAL2.acct_res_id) acct_res_id, 
NVL(BAL1.acct_res_name, BAL2.acct_res_name) acct_res_name, 
NVL(BAL1.acct_res_rating_service_code, BAL2.acct_res_rating_service_code) acct_res_rating_service_code, 
NVL(BAL1.remain_bal, 0) remain_bal_start, 
NVL(BAL2.remain_bal, 0) remain_bal_end, 
NVL(BAL1.eff_date, BAL2.eff_date) eff_date, 
NVL(BAL1.exp_date, BAL2.exp_date) exp_date 
FROM
(-- BAL_EXTRACT A J-1
SELECT DISTINCT
bal_id,
acct_id,
A1.acct_res_id,
A2.acct_res_name,
A2.acct_res_rating_service_code,
remain_bal,
eff_date,
exp_date
FROM 
(
SELECT 
( NVL(gross_bal, 0) + NVL(consume_bal, 0) + NVL(reserve_bal, 0) ) remain_bal,
bal_id,
acct_id,
acct_res_id,
eff_date,
exp_date                 
FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT
WHERE original_file_date = '2023-07-01'
and acct_id in (select acct_id FROM CDR.SPARK_IT_ZTE_SUBS_EXTRACT WHERE original_file_date = DATE_ADD('2023-07-01', 1) and acc_nbr like '%699948312%' limit 1) 
) A1 
LEFT JOIN  DIM.spark_DT_BALANCE_TYPE_ITEM A2 on A1.acct_res_id = A2.acct_res_id
--where acct_res_rating_service_code like "%DATA%"
) BAL1
FULL JOIN 
(-- BAL_EXTRACT A J
SELECT DISTINCT
bal_id,
acct_id,
A1.acct_res_id,
A2.acct_res_name,
A2.acct_res_rating_service_code,
remain_bal,
eff_date,
exp_date
FROM 
(
SELECT 
( NVL(gross_bal, 0) + NVL(consume_bal, 0) + NVL(reserve_bal, 0) ) remain_bal,
bal_id,
acct_id,
acct_res_id,
eff_date,
exp_date                 
FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT
WHERE original_file_date = DATE_ADD('2023-07-01', 1)
and acct_id in (select acct_id FROM CDR.SPARK_IT_ZTE_SUBS_EXTRACT WHERE original_file_date = DATE_ADD('2023-07-01', 1) and acc_nbr like '%699948312%' limit 1) 
) A1
LEFT JOIN  -- DIM.spark_DT_BALANCE_TYPE_ITEM POUR ACCT_RES_NAME
DIM.spark_DT_BALANCE_TYPE_ITEM A2 on A1.acct_res_id = A2.acct_res_id
--where acct_res_rating_service_code like "%DATA%"
)BAL2
-- ON  BAL1.acct_id = BAL2.acct_id and BAL1.acct_res_id = BAL2.acct_res_id
ON  (BAL1.bal_id = BAL2.bal_id and BAL1.acct_res_rating_service_code = BAL2.acct_res_rating_service_code)

) B
ON A.acct_id = B.acct_id

LEFT JOIN -- DEPOT RECURRENT
( 
SELECT
MSISDN,
BAL_ID,
STD_CODE BEN_ACCT_ID, -- acct_res_id
A2.acct_res_rating_service_code acct_res_rating_service_code,
A2.acct_res_name acct_res_name,
PROFILE,
main_credit,
BEN_ACCT_ADD_RESULT,
BEN_ACCT_ADD_VAL,
FROM_UNIXTIME(UNIX_TIMESTAMP(BEN_ACCT_ADD_ACT_DATE, 'yyyyMMddHHmmss')) BEN_ACCT_ADD_ACT_DATE,
FROM_UNIXTIME(UNIX_TIMESTAMP(BEN_ACCT_ADD_EXP_DATE, 'yyyyMMddHHmmss')) BEN_ACCT_ADD_EXP_DATE,
( CASE
WHEN RT_1.USAGE like '%Compte%' THEN DEPOT/100
WHEN STD_CODE ='253' THEN DEPOT
WHEN STD_CODE IN ('187','60','195','200') THEN DEPOT/100
WHEN STD_CODE IN ('165','242','2534') THEN DEPOT
WHEN STD_CODE IN ('131','89') THEN DEPOT/100
WHEN RT_1.USAGE like '%International%' THEN DEPOT/60
WHEN RT_1.USAGE like '%Data%' THEN DEPOT/(1024*1024)
WHEN RT_1.USAGE like '%SMS%' THEN DEPOT
WHEN RT_1.USAGE like '%Roaming%' THEN DEPOT/60
ELSE DEPOT END ) DEPOT,
RT_1.USAGE USAGE_NORMAL,
A2.USAGE USAGE_DIM,
( CASE
WHEN RT_1.USAGE like '%Compte%' THEN 'U'
WHEN RT_1.USAGE like '%Local%' THEN 'U'
WHEN RT_1.USAGE like '%Onnet%' THEN 'U'
WHEN RT_1.USAGE like '%International%' THEN 'Min'
WHEN RT_1.USAGE like '%Data%' THEN 'Mo'
WHEN RT_1.USAGE like '%SMS%' THEN 'Nb'
WHEN RT_1.USAGE like '%Roaming%' THEN 'Min'
ELSE 'Autres' END ) UNITE,
EVENT_DATE
FROM 
(
SELECT
MSISDN,
BAL_ID,
BEN_ACCT_ID STD_CODE,
MAIN main_credit,
VAL_2 BEN_ACCT_ADD_RESULT,
VAL_1 BEN_ACCT_ADD_VAL,
PROFILE,
DATE_AVANT BEN_ACCT_ADD_ACT_DATE,
DATE_APRES BEN_ACCT_ADD_EXP_DATE,
EVENT_DATE,
( CASE
WHEN BEN_ACCT_ID IN ('1','73','78','36','38','1034','206') THEN VAL_1
WHEN BEN_ACCT_ID IN ('187','50','131','37','183','61','64','253','2534','35','58','59','60','62','69','89','91','165','195','200','201','202','218','222','242','259','1234','1434') THEN VAL_2
WHEN BEN_ACCT_ID IN ('74','75') AND  UPPER(PROFILE)  rlike '(FLEX [123456789]?[0-9]?[0-9](\.[05])?K)' THEN VAL_1
WHEN BEN_ACCT_ID IN ('74','75') AND  UPPER(PROFILE) rlike '(FLEX +([a-zA-Z]))' THEN VAL_2
ELSE 0 END ) DEPOT,
( CASE
WHEN BEN_ACCT_ID = '1' THEN 'Compte principal'
WHEN BEN_ACCT_ID IN ('50','75','37','35','59','62','69','91','206','218','222','259','1234','1434','1034','36','38') THEN 'Data'
WHEN BEN_ACCT_ID IN ('183','61','73','58','201','202') THEN 'SMS'
WHEN BEN_ACCT_ID IN ('187','253','60','195','200') THEN 'Voix Local'
WHEN BEN_ACCT_ID IN ('131','2534','89','165','242') THEN 'Voix Onnet'
WHEN BEN_ACCT_ID  IN ('64','74') THEN 'Voix International'
WHEN BEN_ACCT_ID = '78' THEN 'Voix Roaming'
ELSE 'Autres' END ) USAGE
FROM 
(
SELECT
R2.MSISDN MSISDN,
BAL_ID,
BEN_ACCT_ID,
B.main_credit MAIN,
R2.BEN_ACCT_ADD_RESULT VAL_2,
R2.BEN_ACCT_ADD_VAL VAL_1,
B.commercial_offer PROFILE,
R2.BEN_ACCT_ADD_ACT_DATE DATE_AVANT,
R2.BEN_ACCT_ADD_EXP_DATE DATE_APRES,
R2.EVENT_DATE
FROM 
(
SELECT
    SUBSTR(acc_nbr,4) MSISDN,
    event_cost_list/100 MD_PACC,
    SPLIT(BEN_BAL, '&')[0] BEN_ACCT_ID,
    SPLIT(BEN_BAL, '&')[1] BAL_ID,
    CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[2] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_VAL,
    CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[3] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_RESULT,
    SPLIT(BEN_BAL, '&')[4] BEN_ACCT_ADD_ACT_DATE,
    SPLIT(BEN_BAL, '&')[5] BEN_ACCT_ADD_EXP_DATE,
    EVENT_DATE,
    ID
FROM (
    SELECT A.*, ROW_NUMBER() OVER(ORDER BY EVENT_DATE) ID
    FROM CDR.SPARK_IT_ZTE_RECURRING A
    WHERE A.EVENT_DATE = '2023-07-01' 
    and acc_nbr like '%699948312%'
) A
LATERAL VIEW EXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '\;')) TMP AS BEN_BAL

) R2
INNER JOIN
(
SELECT main_credit,commercial_offer,access_key, event_date FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
WHERE EVENT_DATE  = DATE_ADD('2023-07-01', 1)
and access_key like '%699948312%' 
)B
ON R2.MSISDN=B.access_key 
) RT where cast(BEN_ACCT_ID as INT) <> 1
) RT_1

LEFT JOIN  
DIM.spark_DT_BALANCE_TYPE_ITEM A2 on RT_1.STD_CODE = A2.acct_res_id
-- where upper(RT_1.USAGE) like "%DATA%" order by EVENT_DATE		 
) C
ON B.bal_id = C.BAL_ID -- jointure aussi avec msisdn si nécessaire charged volume provenant des dépot recurrents.

LEFT JOIN -- SUBSCRIPTION
(
SELECT MSISDN, TRANSACTION_DATE, ACCT_RES_ID, sum(rated_amount) rated_amount, sum(BEN_ACCT_ADD_VAL) BEN_ACCT_ADD_VAL
from (
SELECT
fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN) MSISDN,
transaction_date, 
transaction_time, 
transactionsn, 
bal_id ACCT_RES_ID_LIST, 
benefit_balance_list BENEFIT_BALANCE_LIST, 
service_list SERVICE_LIST,
rated_amount rated_amount, 
SPLIT(BEN_BAL, '&')[0] ACCT_RES_ID,
CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[1] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_VAL,
CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[2] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_RESULT,
SPLIT(BEN_BAL, '&')[3] BEN_ACCT_ADD_ACT_DATE,
SPLIT(BEN_BAL, '&')[4] BEN_ACCT_ADD_EXP_DATE
FROM (
SELECT transaction_date, transaction_time, served_party_msisdn, transactionsn, bal_id, benefit_balance_list, benefit_unit_list, combo, benefit_bal_list, service_list, rated_amount
FROM MON.SPARK_FT_SUBSCRIPTION
WHERE TRANSACTION_DATE = '2023-07-01'
--and upper(combo) not like '%NON%'
AND  fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN) like "%699948312%"
--AND fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN) like "%697426474%" 
) A
LATERAL VIEW EXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '#')) TMP AS BEN_BAL
) 
group by MSISDN, TRANSACTION_DATE, ACCT_RES_ID
) SUBS -- il n y a pas de BAL_ID et ACCT_ID DANS LES SUBSCRIPTIONS
ON (A.acc_nbr = SUBS.MSISDN and B.acct_res_id = SUBS.ACCT_RES_ID) -- jointure avec MSISDN et ACCT_RES_ID 

LEFT JOIN -- USAGE DATA CRA_GPRS
(
SELECT SESSION_DATE, SERVED_PARTY_MSISDN, USED_BAL_ID, USED_ACCT_RES_ID, USED_BALANCE, sum(bytes_sent) bytes_sent, sum(bytes_received) bytes_received,  sum(charge_sum) charge_sum,   sum(used_volume) used_volume
from (
select session_date, session_time, served_party_msisdn, bytes_sent, bytes_received, charge_sum, SPLIT(used_volume_list, '\\|')[0] used_volume, SPLIT(used_balance_list, '\\|')[0] used_balance, SPLIT(remaining_volume_list, '\\|')[0] remaining_volume, SPLIT(preceding_volume_list, '\\|')[0] preceding_volume,  SPLIT(USED_BAL_ID_LIST, '\\|')[0] USED_BAL_ID,   SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[0] USED_ACCT_RES_ID,   SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[0] USED_ACCT_ITEM_TYPE_ID 
from MON.SPARK_ft_cra_gprs_with_bal_id 
where session_date = "2023-07-01"
--and session_time like "2115%"
and cast(SPLIT(used_volume_list, '\\|')[0] as DOUBLE) > 0
and SERVED_PARTY_MSISDN is not null
and SERVED_PARTY_MSISDN like "%699948312%"

union all 
select session_date, session_time, served_party_msisdn, bytes_sent, bytes_received, charge_sum, SPLIT(used_volume_list, '\\|')[1] used_volume, SPLIT(used_balance_list, '\\|')[1] used_balance, SPLIT(remaining_volume_list, '\\|')[1] remaining_volume, SPLIT(preceding_volume_list, '\\|')[1] preceding_volume,  SPLIT(USED_BAL_ID_LIST, '\\|')[1] USED_BAL_ID,   SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[1] USED_ACCT_RES_ID,   SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[1] USED_ACCT_ITEM_TYPE_ID 
from MON.SPARK_ft_cra_gprs_with_bal_id 
where session_date = "2023-07-01"
--and session_time like "2115%"
and cast(SPLIT(used_volume_list, '\\|')[1] as DOUBLE) > 0
and SERVED_PARTY_MSISDN is not null
and SERVED_PARTY_MSISDN like "%699948312%"

union all
select session_date, session_time, served_party_msisdn, bytes_sent, bytes_received, charge_sum, SPLIT(used_volume_list, '\\|')[2] used_volume, SPLIT(used_balance_list, '\\|')[2] used_balance, SPLIT(remaining_volume_list, '\\|')[2] remaining_volume, SPLIT(preceding_volume_list, '\\|')[2] preceding_volume,  SPLIT(USED_BAL_ID_LIST, '\\|')[2] USED_BAL_ID,   SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[2] USED_ACCT_RES_ID,   SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[2] USED_ACCT_ITEM_TYPE_ID 
from MON.SPARK_ft_cra_gprs_with_bal_id 
where session_date = "2023-07-01"
--and session_time like "2115%"
and cast(SPLIT(used_volume_list, '\\|')[2] as DOUBLE) > 0
and SERVED_PARTY_MSISDN is not null
and SERVED_PARTY_MSISDN like "%699948312%"

union all
select session_date, session_time, served_party_msisdn, bytes_sent, bytes_received, charge_sum, SPLIT(used_volume_list, '\\|')[3] used_volume, SPLIT(used_balance_list, '\\|')[3] used_balance, SPLIT(remaining_volume_list, '\\|')[3] remaining_volume, SPLIT(preceding_volume_list, '\\|')[3] preceding_volume,  SPLIT(USED_BAL_ID_LIST, '\\|')[3] USED_BAL_ID,   SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[3] USED_ACCT_RES_ID,   SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[3] USED_ACCT_ITEM_TYPE_ID 
from MON.SPARK_ft_cra_gprs_with_bal_id 
where session_date = "2023-07-01"
--and session_time like "2115%"
and cast(SPLIT(used_volume_list, '\\|')[3] as DOUBLE) > 0
and SERVED_PARTY_MSISDN is not null
and SERVED_PARTY_MSISDN like "%699948312%"
) 
group by session_date, served_party_msisdn, used_bal_id, used_acct_res_id, USED_BALANCE
) CRA
ON B.BAL_ID = CRA.USED_BAL_ID
WHERE 
upper(B.acct_res_name) like "%DATA%" or (B.acct_res_id = 1 and upper(B.acct_res_rating_service_code) like '%DATA%')
--upper(B.acct_res_name) like "%DATA%" and cast(C.BEN_ACCT_ID as INT) <> 1 -- le sous compte provenant de bal_extract est de type data et l'id du sous compte des dépots reccurents doit être différent du main car il n y a pas de dépot récurrent data dans le main
;
