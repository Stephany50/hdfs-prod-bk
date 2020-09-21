insert into SPOOL.SPARK_SPOOL_SUSPENSIONS_TELCO
select
nvl(DATE_SUSPENSION,'') DATE_SUSPENSION,
replace(trim(MSISDN),';','')  msisdn,
statut_validation_bo,
statut_validation_boo,
nvl(DATE_ACTIVATION,'') date_activation,
date_validation_bo,
nvl(trim(replace(SITE_NAME,';',' ')),'') site_name,
nvl(trim(replace(TOWNNAME,';',' ')),'') townname,
nvl(trim(replace(administrative_region,';',' ')),'') administrative_region,
nvl(trim(replace(commercial_region,';',' ')),'') commercial_region,
last_location_day,
statut_zm,
date_validation_bo_zm,
statut_validation_bo_zm,
nvl(trim(replace(order_reason,';',' ')),'') order_reason,
nvl(trim(replace(block_reason,';',' ')),'') block_reason,
current_timestamp() insert_date,
'###SLICE_VALUE###' event_date
from (
select a.*,
row_number() over(partition by msisdn order by date_suspension desc nulls last) as rang
from TMP.TT_SUSPENSION_TELCO a) b
where rang = 1

----

create table TMP.Test_spool_suspensions_telco
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
as
select *
from SPOOL.SPARK_SPOOL_SUSPENSIONS_TELCO where 1 = 2;