CREATE TABLE DIM.DT_REF_CBM(
MSISDN VARCHAR(120),
MULTI VARCHAR(150),
OPERATEUR VARCHAR(150),
TENURE VARCHAR(159),
SEGMENT VARCHAR(145),
REGION VARCHAR(255)
)STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')



CREATE TABLE BACKUP_DWH.DT_REF_CBM(
MSISDN VARCHAR(120),
MULTI VARCHAR(150),
OPERATEUR VARCHAR(150),
TENURE VARCHAR(159),
SEGMENT VARCHAR(145),
REGION VARCHAR(255)
)STORED AS ORC


insert into mon.SPARK_FT_REVENU_LOCALISE
SELECT
site_name,
offer_profile_code,
destination_type,
destination,
rated_tel_total_count,
rated_sms_total_count,
rated_duration,
voice_main_rated_amount,
voice_promo_rated_amount,
sms_main_rated_amount,
sms_promo_rated_amount,
revenu_data,
mbytes_used,
src_table,
site_code,
in_call_count,
in_duration,
CURRENT_TIMESTAMP insert_date,
event_date
FROM  BACKUP_DWH.FT_REVENU_LOCALISE