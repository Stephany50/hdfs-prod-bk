case
    when category='Distribution' then 'd. Distribution'
    when category='Digital' then 'e. Digital'
    when category='Subscriber overview' then 'b. Subscriber overview'
    when category='Revenue overview' then 'a. Revenue overview'
    when category='Leviers de croissance' then 'c. Leviers de croissance'
    else category 
end

case
    when kpi='Telco (prepayé+hybrid) + OM' then 'Revenu Telco (prepayé+hybrid) + Revenu OM'
    when kpi='dont sortant (~recharges)' then 'Somme des recharges des points de vente vers le consommateurs final + auto rechargement des clients (OM) + rechargement via CAG'
    when kpi='dont Voix' then 'Revenu de la voix/sms bundle + revenu voix/sms PAY AS YOU GO'
    when kpi='Subscriber base' then 'Le parc group actif sur les 90 derniers jours'
    when kpi='Gross Adds' then 'Les acquisitions de la période'
    when kpi='Churn' then 'Les déconnexions de la période'
    when kpi='Net adds' then 'Net adds (PARC S0-PARC S-1)'
    when kpi='Tx users (30jrs) en %' then 'Le pourcentage du parc ayant trafiqué sur les 30 derniers jours'
    when kpi='Revenue Data Mobile' then 'Revenue Data bundle + Revenu Data PAY AS YOU GO (Roaming) '
    when kpi='Price Per Megas' then 'Revenu data / le traffic data'
    when kpi='Data users (30jrs, >1Mo)' then 'Le nombre de users ayant fait la data sur les 30 derniers jours'
    when kpi='Tx users data(30jrs) en %' then 'Le pourcentage du parc ayant fait la DATA sur les 30 derniers jours'
    when kpi='Revenue Orange Money' then 'Revenue Orange Money'
    when kpi='Users OM (30jrs)' then 'Le nombre de users OM actifs sur les 30 derniers jours'
    when kpi='Tx users OM(30jrs) en %' then 'Le pourcentage du parc ayant fait des opérations OM 30 derniers jours'
    when kpi='Cash In Valeur' then 'Cash In Valeur'
    when kpi='Cash Out Valeur' then 'Cash Out Valeur'

    when kpi='Payments(Bill, Merch)' then 'Payments(Bill, Merch)'
    when kpi='Self Top UP ratio (%)' then 'Auto rechargement du client final (via OM, Tango...) /recharges globales'
    when kpi='Stock total client(OM)' then 'Stock total client(OM)'
    when kpi='Nombre de Pos Airtime actif(30jrs)' then 'Nombre de Pos Airtime actif(30jrs)'
    when kpi='Nombre de Pos OM actif(30jrs)' then 'Nombre de Pos OM actif(30jrs)'
    when kpi='Niveau de stock @ distributor level (nb jour)' then '(Somme des stocks dans les SIM de catégorie A et A'' à un instant donné)/moyenne de vente au consommateur final de l''ensemble du réseau de distribution'
    when kpi='Niveau de stock @ retailer level (nb jour)' then '(Somme des stocks dans les SIM de détail (PO, PPOS, NPOS) à un instant donné)/moyenne de vente au consommateur final de l''ensemble du réseau de distribution'
    when kpi='Engagements' then 'Engagements'
    when kpi='Pourcentage traité en numérique' then 'Pourcentage traité en numérique'
    when kpi='Pourcentage appel traité par BOT' then 'Pourcentage appel traité par BOT'
    when kpi='Durée moyenne de réponse numérique' then 'Durée moyenne de réponse numérique'
    else kpi
end definition


DECLARE
  SAMPLE_TABLE VARCHAR2(200)  MIN_DATE_PARTITION VARCHAR2(200)  MAX_DATE_PARTITION VARCHAR2(200)  KEY_COLUMN_PART_NAME VARCHAR2(200)
  KEY_COLUMN_PART_TYPE VARCHAR2(200)  PART_OWNER VARCHAR2(200)  PART_TABLE_NAME VARCHAR2(200)  PART_PARTITION_NAME VARCHAR2(200)
  PART_TYPE_PERIODE VARCHAR2(200)  PART_RETENTION NUMBER  PART_TBS_CIBLE VARCHAR2(200)  PART_GARDER_01_DU_MOIS VARCHAR2(200)
  PART_PCT_FREE NUMBER   PART_COMPRESSION VARCHAR2(200)  PART_ROTATION_ACTIVE VARCHAR2(200)  PART_FORMAT VARCHAR2(200)
BEGIN
  SAMPLE_TABLE := 'MON.SPARK_KPIS_REG_FINAL'
  MIN_DATE_PARTITION := '20180531'
  MAX_DATE_PARTITION := '20220714'
  KEY_COLUMN_PART_NAME := 'EVENT_DAY'
  KEY_COLUMN_PART_TYPE := 'JOUR'
  PART_OWNER := 'MON'
  PART_TABLE_NAME := 'SPARK_KPIS_REG_FINAL2'
  PART_PARTITION_NAME := 'SPARK_KPIS_REG_FINAL_'
  PART_TYPE_PERIODE := 'JOUR'
  PART_RETENTION := 1000
  PART_TBS_CIBLE := 'TAB_P_MON_Jour_256M'
  PART_GARDER_01_DU_MOIS := 'NON'
  PART_PCT_FREE := 0
  PART_COMPRESSION := 'COMPRESS'
  PART_ROTATION_ACTIVE := 'OUI'
  PART_FORMAT := 'yyyymmdd'
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT )
  COMMIT
END

insert into cdr.spark_it_zebra_master
select
channel_user_id,
parent_user_id,
owner_user_id,
user_type,
external_code,
primary_msisdn,
user_status,
login_id,
category_code,
category_name,
geographical_domain_code,
geographical_domain_name,
channel_user_name,
city,
state,
country,
file_name original_file_name,
null original_file_size,
null original_file_line_count,
generated_date original_file_date,
insert_date,
to_date(generated_date) transaction_date,
to_date(generated_date) file_date
from backup_dwh.IT_ZEBRA_MASTER

insert into cdr.spark_it_om_all_balance2
--create table cdr.spark_it_om_all_balance2 as
select
account_type,
account_name,
account_id,
balance,
user_name,
last_name,
user_domain,
user_category,
wallet_number,
frozen_amount,
null  payment_type_id,
original_file_name,
null original_file_size,
null original_file_line_count,
insert_date,
to_date(original_file_date) original_file_date
from backup_dwh.IT_OM_ALL_BALANCE_DWH2

insert into mon.spark_FT_SUBSCRIPTION
select
transaction_time,
served_party_msisdn,
contract_type,
commercial_offer,
operator_code,
subscription_channel,
service_list,
subscription_service,
subscription_service_details,
subscription_related_service,
rated_amount,
main_balance_used,
active_date,
active_time,
expire_date,
expire_time,
subscription_status,
previous_commercial_offer,
previous_status,
previous_subs_service_details,
previous_subs_related_service,
termination_indicator,
benefit_balance_list,
benefit_unit_list,
benefit_added_value_list,
benefit_result_value_list,
benefit_active_date_list,
benefit_expire_date_list,
total_occurence,
insert_date,
source_insert_date,
original_file_name,
service_code,
amount_voice_onnet,
amount_voice_offnet,
amount_voice_inter,
amount_voice_roaming,
amount_sms_onnet,
amount_sms_offnet,
amount_sms_inter,
amount_sms_roaming,
amount_data,
amount_sva,
null combo,
null benefit_bal_list,
null bal_id,
transaction_date
from mon.FT_SUBSCRIPTION where transaction_date <'2019-12-09'

insert into cdr.spark_it_zebra_master_balance
select
event_time,
channel_user_id,
user_name,
mobile_number,
category,
mobile_number_1,
geographical_domain,
product,
parent_user_name,
owner_user_name,
available_balance,
agent_balance,
original_file_name,
original_file_date,
insert_date,
user_status,
to_change,
modified_on,
null original_file_size,
null original_file_line_count,
to_date(event_date) event_date
from backup_dwh.IT_ZEBRA_MASTER_BALANCE_DWH

insert into cdr.spark_it_omny_transactions2
select
sender_msisdn,
receiver_msisdn,
receiver_user_id,
sender_user_id,
transaction_amount,
commissions_paid,
commissions_received,
commissions_others,
service_charge_received,
service_charge_paid,
taxes,
service_type,
transfer_status,
sender_pre_bal,
sender_post_bal,
receiver_pre_bal,
receiver_post_bal,
sender_acc_status,
receiver_acc_status,
error_code,
error_desc,
reference_number,
created_on,
created_by,
modified_on,
modified_by,
app_1_date,
app_2_date,
transfer_id,
transfer_datetime transfer_datetime_nq,
sender_category_code,
sender_domain_code,
sender_grade_name,
sender_group_role,
sender_designation,
sender_state,
receiver_category_code,
receiver_domain_code,
receiver_grade_name,
receiver_group_role,
receiver_designation,
receiver_state,
sender_city,
receiver_city,
app_1_by,
app_2_by,
request_source,
gateway_type,
transfer_subtype,
payment_type,
payment_number,
payment_date,
remarks,
action_type,
transaction_tag,
reconciliation_by,
reconciliation_for,
ext_txn_number,
original_ref_number,
zebra_ambiguous,
attempt_status,
other_msisdn,
sender_wallet_number,
receiver_wallet_number,
sender_user_name,
receiver_user_name,
tno_msisdn,
tno_id,
unreg_first_name,
unreg_last_name,
unreg_dob,
unreg_id_number,
bulk_payout_batchid,
is_financial,
transfer_done,
initiator_msisdn,
validator_msisdn,
initiator_comments,
validator_comments,
sender_wallet_name,
reciever_wallet_name,
sender_user_type,
receiver_user_type,
original_file_name,
null original_file_size,
null original_file_line_count,
original_file_date,
insert_date,
to_date(transfer_datetime ) transfer_datetime,
to_date(transfer_datetime )file_date
from backup_dwh.it_omny_transactions_dwh3

insert into mon.spark_ft_conso_msisdn_day select
msisdn,
formule,
conso,
sms_count,
tel_count,
tel_duration,
conso_tel_main,
billed_sms_count,
billed_tel_count,
billed_tel_duration,
conso_sms,
conso_tel,
promotional_call_cost,
main_call_cost,
src_table,
others_vas_total_count,
others_vas_duration,
others_vas_main_cost,
others_vas_promo_cost,
national_total_count,
national_sms_count,
national_duration,
national_main_cost,
national_promo_cost,
national_sms_main_cost,
national_sms_promo_cost,
mtn_total_count,
mtn_sms_count,
mtn_duration,
mtn_total_conso,
mtn_sms_conso,
camtel_total_count,
camtel_sms_count,
camtel_duration,
camtel_total_conso,
camtel_sms_conso,
international_total_count,
international_sms_count,
international_duration,
international_total_conso,
international_sms_conso,
onnet_sms_count,
onnet_duration,
onnet_total_conso,
onnet_main_conso,
onnet_main_tel_conso,
onnet_promo_tel_conso,
onnet_sms_conso,
mtn_main_conso,
camtel_main_conso,
international_main_conso,
roam_total_count,
roam_sms_count,
roam_duration,
roam_total_conso,
roam_main_conso,
roam_sms_conso,
set_total_count,
set_sms_count,
set_duration,
set_total_conso,
set_sms_conso,
set_main_conso,
inroam_total_count,
inroam_sms_count,
inroam_duration,
inroam_total_conso,
inroam_main_conso,
inroam_sms_conso,
nexttel_total_count,
nexttel_sms_count,
nexttel_duration,
nexttel_total_conso,
nexttel_sms_conso,
nexttel_main_conso,
bundle_sms_count,
bundle_tel_duration,
set_main_tel_conso,
mtn_main_tel_conso,
nexttel_main_tel_conso,
camtel_main_tel_conso,
international_main_tel_conso,
roam_main_tel_conso,
inroam_main_tel_conso,
set_promo_tel_conso,
mtn_promo_tel_conso,
nexttel_promo_tel_conso,
camtel_promo_tel_conso,
international_promo_tel_conso,
roam_promo_tel_conso,
inroam_promo_tel_conso,
onnet_billed_tel_duration,
set_billed_tel_duration,
nexttel_billed_tel_duration,
mtn_billed_tel_duration,
camtel_billed_tel_duration,
international_bil_tel_duration,
roam_billed_tel_duration,
inroam_billed_tel_duration,
onnet_billed_tel_count,
set_billed_tel_count,
nexttel_billed_tel_count,
mtn_billed_tel_count,
camtel_billed_tel_count,
international_billed_tel_count,
roam_billed_tel_count,
inroam_billed_tel_count,
onnet_tel_count,
set_tel_count,
nexttel_tel_count,
mtn_tel_count,
camtel_tel_count,
international_tel_count,
roam_tel_count,
inroam_tel_count,
sva_count,
sva_duration,
sva_main_conso,
sva_promo_conso,
sva_tel_count,
sva_billed_duration,
sva_billed_tel_conso,
sva_sms_count,
sva_sms_conso,
operator_code,
insert_date,
to_date(event_date) event_date from backup_dwh.ft_conso_msisdn_day where to_date(event_date)>='2020-01-27'

insert into MON.spark_FT_REFILL select
refill_id,
refill_time,
receiver_msisdn,
receiver_profile,
receiver_imsi,
sender_msisdn,
sender_profile,
refill_mean,
refill_type,
refill_amount,
refill_bonus,
termination_ind,
refill_code,
refill_description,
receiver_operator_code,
sender_operator_code,
sender_category,
receiver_category,
sender_pre_bal,
sender_post_bal,
receiver_pre_bal,
receiver_post_bal,
entry_date,
original_file_name,
insert_date,
commission,
refill_date,
refill_date file_date from spark_ft_refill_old where refill_date<"2020-01-01"


 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-157-141.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_157_141.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-140-131.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_140_131.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-130-121.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_130_121.log &

 --ok
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-120-111.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_120_111.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-110-105.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_110_105.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-104-100.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_104_100.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-99-95.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_99_95.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-94-90.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_94_90.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-89-85.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_89_85.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-84-80.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_84_80.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-79-75.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_79_75.log &


 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-74-70.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_74_70.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-69-65.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_69_65.log &

---ok

 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-64-60.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_64_60.log &



 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-59-55.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_59_55.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-54-00.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_54_50.log &

 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-44-40.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_44_40.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-39-35.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_39_35.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-34-30.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_34_30.log &


 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-29-25.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_29_25.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-24-20.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_24_20.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-19-15.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_19_15.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-14-10.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_14_10.log &
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-09-05.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_09_05.log &
 --ok
 nohup /usr/hdp/current/spark2-client/bin/spark-submit --driver-java-options "-Dlog4j.configuration=file:/var/crash/driver_log4j.properties" --master yarn  --executor-cores 5 --num-executors 5 --driver-memory 5G --executor-memory 5G --principal "kafka/edge01.adcm.orangecm@OCMHDP.COM" --keytab "/etc/security/keytabs/kafka.service.keytab"  --files  /opt/app/cronmanager/conf/sample-jaas.conf,/opt/app/cronmanager/conf/sample-yarn-jaas.conf,/etc/security/keytabs/hdfs.headless.keytab,/etc/security/keytabs/nifi.service.keytab,/etc/security/keytabs/spark.headless.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./sample-yarn-jaas.conf" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=/opt/app/cronmanager/conf/sample-jaas.conf" --class cm.orange.processor.BatchScriptLauncher /opt/app/cronmanager/bin/CronManager-assembly-0.2.jar -c /DATALAB/FBCD1867/CONF/compute-agregations-daily-04-00.conf -i LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY > /tmp/LOAD_SPARK_KPI_DG_AGREGATIONS_DAILY_04_00.log &
