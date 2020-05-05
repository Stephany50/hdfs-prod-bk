    add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
    create temporary function GENERATE_SEQUENCE_FROM_INTERVALE as 'cm.orange.bigdata.udf.GenerateSequenceFromIntervale';
    SELECT
    SEQUENCE
    FROM (
        SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ FROM (
            SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
                SELECT
                    DISTINCT
                    CAST(SUBSTRING(SOURCE,11,9) AS INT) INDEX,
                    SUBSTRING(SOURCE,5,11) MSC_TYPE
                FROM CDR.SPARK_IT_CRA_MSC_HUAWEI
                    WHERE CALLDATE = '2020-05-01' --AND TO_DATE(ORIGINAL_FILE_DATE)='2020-04-11'
            )A
        )D WHERE INDEX-PREVIOUS >1
    )R
    LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE


SELECT
concat('HUA_DWH-010520-',SEQUENCE)
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
            SELECT
                DISTINCT
                cast (substring(original_file_name,16,21) as int) INDEX,
                1 MSC_TYPE
            FROM CDR.SPARK_IT_CRA_MSC_HUAWEI
            WHERE file_date = '2020-04-19' --AND TO_DATE(ORIGINAL_FILE_DATE)='2020-04-11'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
Name                  Null? Type
--------------------- ----- ------------


insert into  tmp.ft_group_subscriber_summary2
select
EVENT_DATE,
PROFILE,
STATUT,
ETAT,
BSCS_COMM_OFFER,
TRANCHE_AGE,
CUST_BILLCYCLE,
CREDIT,
EFFECTIF,
ACTIVATIONS,
RESILIATIONS,
SRC_TABLE,
INSERT_DATE,
PLATFORM_STATUS,
EFFECTIF_CLIENTS_OCM,
DECONNEXIONS,
CONNEXIONS,
RECONNEXIONS,
OPERATOR_CODE
from  mon.spark_ft_group_subscriber_summary where EVENT_DATE>"2020-04-10"


insert into  tmp.ft_commercial_subscrib_summary2
select
DATECODE,
NETWORK_DOMAIN,
NETWORK_TECHNOLOGY,
SUBSCRIBER_CATEGORY,
CUSTOMER_ID,
SUBSCRIBER_TYPE,
COMMERCIAL_OFFER,
ACCOUNT_STATUS,
LOCK_STATUS,
ACTIVATION_MONTH,
CITYZONE,
USAGE_TYPE,
TOTAL_COUNT,
TOTAL_ACTIVATION,
TOTAL_DEACTIVATION,
TOTAL_EXPIRATION,
TOTAL_PROVISIONNED,
TOTAL_MAIN_CREDIT,
TOTAL_PROMO_CREDIT,
TOTAL_SMS_CREDIT,
TOTAL_DATA_CREDIT,
SOURCE,
REFRESH_DATE,
PROFILE_NAME,
PLATFORM_ACCOUNT_STATUS,
PLATFORM_ACTIVATION_MONTH
from mon.spark_ft_commercial_subscrib_summary where datecode>'2020-04-09'




insert into  tmp.ft_a_subscriber_summary2
select
DATECODE,
NETWORK_DOMAIN,
NETWORK_TECHNOLOGY,
SUBSCRIBER_CATEGORY,
CUSTOMER_ID,
SUBSCRIBER_TYPE,
COMMERCIAL_OFFER,
ACCOUNT_STATUS,
LOCK_STATUS,
ACTIVATION_MONTH,
CITYZONE,
USAGE_TYPE,
TOTAL_COUNT,
TOTAL_ACTIVATION,
TOTAL_DEACTIVATION,
TOTAL_EXPIRATION,
TOTAL_PROVISIONNED,
TOTAL_MAIN_CREDIT,
TOTAL_PROMO_CREDIT,
TOTAL_SMS_CREDIT,
TOTAL_DATA_CREDIT,
SOURCE,
REFRESH_DATE,
PROFILE_NAME,
PROCESS_NAME
from agg.ft_a_subscriber_summary where datecode>='2020-04-10'





insert into tmp.ft_a_subscription2  select
    TRANSACTION_DATE,
    TRANSACTION_TIME,
    CONTRACT_TYPE,
    OPERATOR_CODE,
    MAIN_USAGE_SERVICE_CODE,
    COMMERCIAL_OFFER,
    PREVIOUS_COMMERCIAL_OFFER,
    SUBS_SERVICE,
    SUBS_BENEFIT_NAME,
    SUBS_CHANNEL,
    SUBS_RELATED_SERVICE,
    SUBS_TOTAL_COUNT,
    SUBS_AMOUNT,
    SOURCE_PLATFORM,
    SOURCE_DATA,
    INSERT_DATE,
    SERVICE_CODE,
    MSISDN_COUNT,
    SUBS_EVENT_RATED_COUNT,
    SUBS_PRICE_UNIT,
    AMOUNT_SVA,
    AMOUNT_VOICE_ONNET,
    AMOUNT_VOICE_OFFNET,
    AMOUNT_VOICE_INTER,
    AMOUNT_VOICE_ROAMING,
    AMOUNT_SMS_ONNET,
    AMOUNT_SMS_OFFNET,
    AMOUNT_SMS_INTER,
    AMOUNT_SMS_ROAMING,
    AMOUNT_DATA,
    COMBO
    FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE >='2020-04-11' ;



insert into CDR.spark_iT_BDI_FLOTTE
select
g.MSISDN  MSISDN,
h.CUSTOMER_ID  CUSTOMER_ID,
h.CONTRACT_ID  CONTRACT_ID,
h.COMPTE_CLIENT  COMPTE_CLIENT,
'M2M'  TYPE_PERSONNE,
null  TYPE_PIECE,
null  NUMERO_PIECE,
null  ID_TYPE_PIECE,
null  NOM_PRENOM,
null  NOM,
null  PRENOM,
null  DATE_NAISSANCE,
null  DATE_EXPIRATION,
null  ADRESSE,
'DOUALA'  VILLE,
'AKWA'  QUARTIER,
h.DATE_SOUSCRIPTION  DATE_SOUSCRIPTION,
h.DATE_ACTIVATION  DATE_ACTIVATION,
h.STATUT  STATUT,
h.RAISON_STATUT  RAISON_STATUT,
h.DATE_CHANGEMENT_STATUT  DATE_CHANGEMENT_STATUT,
null  PLAN_LOCALISATION,
null  CONTRAT_SOUCRIPTION,
null  DISPONIBILITE_SCAN,
null  ACCEPTATION_CGV,
null  TYPE_PIECE_TUTEUR,
null  NUMERO_PIECE_TUTEUR,
null  NOM_TUTEUR,
null  PRENOM_TUTEUR,
null  DATE_NAISSANCE_TUTEUR,
null  DATE_EXPIRATION_TUTEUR,
null  ADRESSE_TUTEUR,
'4.4335'  COMPTE_CLIENT_STRUCTURE,
'FORIS TELECOM'  NOM_STRUCTURE,
'RC/DLA/2008/B/782' NUMERO_REGISTRE_COMMERCE,
'110046646' NUMERO_PIECE_REPRESENTANT_LEGAL,
h.IMEI  IMEI,
null  STATUT_DEROGATION,
h.REGION_ADMINISTRATIVE  REGION_ADMINISTRATIVE,
h.REGION_COMMERCIALE  REGION_COMMERCIALE,
h.SITE_NAME  SITE_NAME,
h.VILLE_SITE  VILLE_SITE,
h.OFFRE_COMMERCIALE  OFFRE_COMMERCIALE,
h.TYPE_CONTRAT  TYPE_CONTRAT,
h.SEGMENTATION  SEGMENTATION,
h.odbIncomingCalls  odbIncomingCalls,
h.odbOutgoingCalls  odbOutgoingCalls,
null  DEROGATION_IDENTIFICATION
    from (
            select e.*
            from (
                select MSISDN,NUMERO_PIECE
                from TMP.TT_BDI3
                where NUMERO_PIECE in  ( select NUMERO_PIECE from (
                SELECT NUMERO_PIECE, COUNT(*) AS NB
                FROM TMP.TT_BDI3
                WHERE (odbIncomingCalls = '0' ANd odbOutgoingCalls = '0') AND
                NUMERO_PIECE NOT LIKE "1122334455%" GROUP BY NUMERO_PIECE HAVING NB > 3
                    ) a
                )
            ) e
        left join
        (
            select msisdn, numero_piece
            from (
                select
                    msisdn, numero_piece,
                    row_number() over( partition by numero_piece order by msisdn ) AS RANG
                from (
                select MSISDN,NUMERO_PIECE
                from TMP.TT_BDI3
                where NUMERO_PIECE in (
                    select NUMERO_PIECE from (
                    SELECT NUMERO_PIECE, COUNT(*) AS NB
                    FROM TMP.TT_BDI3
                    WHERE (odbIncomingCalls = '0' ANd odbOutgoingCalls = '0') AND
                    NUMERO_PIECE NOT LIKE "1122334455%" GROUP BY NUMERO_PIECE HAVING NB > 3
                    ) a
                 )
                ) c
            ) d
            where RANG <= 1
        ) f on substr(trim(e.msisdn),-9,9) = substr(trim(f.msisdn),-9,9) where f.msisdn is null
) g
left join  TMP.tt_bdi3 h on substr(nvl(g.msisdn,''),-9,9) = substr(nvl(h.msisdn,''),-9,9)


select MSISDN,NUMERO_PIECE, count(distinct MSISDN) NB
from TMP.TT_BDI3 group by MSISDN,NUMERO_PIECE
WHERE odbIncomingCalls = '0' ANd odbOutgoingCalls = '0') AND
        NUMERO_PIECE NOT LIKE "1122334455%"

INSERT INTO AGG.SPARK_FT_CBM_AGREGAT
select
site_name,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
 MOU_OFNET,
MA_VOICE_ONNET,
 MA_VOICE_INTER,
MA_SMS_INTER,
MA_DATA,
MA_GOS_SVA,
MA_SMS_ROAMING,
 MA_SMS_SVA,
MA_VOICE_SVA,
user_voice,
bdle_name,
BDLE_COST,
volume_data,
user_data1,
user_data2 ,
MONTANT,
destination_type,
destination,
traffic_data,
rated_sms_total_count,
in_duration,
rated_duration,
paygo_data_revenue,
user_data,
paygo_voice_revenue,
og_total_call_duration,
data_type,
INSERT_DATE,
event_date
from
(
SELECT
site_name,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
sum(nvl(MOU_ONNET,0)) as MOU_ONNET , sum(nvl(MOU_OFNET,0)) as MOU_OFNET,
sum(nvl(MOU_INTER,0)) as MOU_INTER,sum(nvl(MA_VOICE_ONNET,0)) as MA_VOICE_ONNET,
sum(nvl(MA_VOICE_OFNET,0)) as MA_VOICE_OFNET,sum(nvl(MA_VOICE_INTER,0)) as MA_VOICE_INTER,
sum(nvl(MA_SMS_OFNET,0)) as MA_SMS_OFNET,sum(nvl(MA_SMS_INTER,0)) as MA_SMS_INTER,
sum(nvl(MA_SMS_ONNET,0)) as MA_SMS_ONNET, sum(nvl(MA_DATA,0)) as MA_DATA,
sum(nvl(MA_VAS,0)) as MA_VAS, sum(nvl(MA_GOS_SVA,0)) as MA_GOS_SVA,
sum(nvl(MA_VOICE_ROAMING,0)) as MA_VOICE_ROAMING, sum(nvl(MA_SMS_ROAMING,0)) as MA_SMS_ROAMING,
sum(nvl(MA_SMS_SVA,0)) as MA_SMS_SVA,
sum(nvl(MA_VOICE_SVA,0)) as MA_VOICE_SVA,
sum(is_user_voice) as user_voice,
null as bdle_name,
null as BDLE_COST,
null as volume_data,
null as user_data1,
null as  user_data2 ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as user_data,
null as paygo_voice_revenue,
null as og_total_call_duration,
'CI' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS event_date
FROM
(select *, case when (MOU_ONNET+MOU_OFNET+MOU_INTER) > 0 then 1 else 0 end as is_user_voice from mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
WHERE PERIOD  ='2020-04-05') A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE  ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, multi, operateur, tenure, segment
UNION ALL
SELECT
site_name,
multi,
operateur,
tenure,
segment,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as user_voice,
bdle_name,
sum(nvl(BDLE_COST,0))  BDLE_COST,
null as volume_data,
null as user_data1,
null as user_data2 ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as paygo_data_revenue,
null as user_data,
null as paygo_voice_revenue,
null as og_total_call_duration,
'SUBS' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS event_date
FROM
(select * from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
WHERE PERIOD  ='2020-04-05') A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, bdle_name, multi, operateur, tenure, segment
UNION ALL
SELECT
site_name,
multi,
operateur,
tenure,
segment,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as  MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as  user_voice,
bdle_name,
sum(nvl(BDLE_COST,0)) BDLE_COST,
null as volume_data,
null as user_data1,
null as  user_data2 ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as  user_data,
null as  paygo_voice_revenue,
null as og_total_call_duration,
'RETAIL' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS  event_date
FROM
(select Sdate as period, sub_msisdn as msisdn, offer_name as bdle_name,
sum(RECHARGE_AMOUNT) as BDLE_COST
from MON.SPARK_FT_VAS_RETAILLER_IRIS
where upper(offer_type) not in ('TOPUP') and sdate ='2020-04-05' and PRETUPS_STATUSCODE = '200'
group by Sdate, sub_msisdn, offer_name) A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, bdle_name, multi, operateur, tenure, segment
UNION ALL
SELECT
site_name,
multi,
operateur,
tenure,
segment,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as  MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as  user_voice,
null as bdle_name,
null as BDLE_COST,
sum(nbytest)/(1024*1024) as volume_data,
sum(is_user_data1) as user_data1,
sum(is_user_data2) as user_data2  ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as  user_data,
null as  paygo_voice_revenue,
null as og_total_call_duration,
'TRAFFIC_DATA' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS  event_date
FROM
(select TRANSACTION_DATE as period,
msisdn, nbytest, case when nbytest/(1024*1024)>1  then 1 else 0 end as is_user_data1,
case when nbytest/(1024*1024)>5  then 1 else 0 end as is_user_data2  from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
WHERE TRANSACTION_DATE ='2020-04-05') A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, multi, operateur, tenure, segment
UNION ALL
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as  MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as  user_voice,
null as bdle_name,
null as BDLE_COST,
null as volume_data,
null as user_data1,
null as user_data2 ,
sum(MONTANT) as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as  user_data,
null as  paygo_voice_revenue,
null as og_total_call_duration,
'VAS' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
CREATE_DATE as EVENT_DATE
FROM
(
SELECT
A.CREATE_DATE,
A.MSISDN,
A.SERVICE,
CASE WHEN C.ACCT_RES_RATING_UNIT = 'QM' THEN -A.CHARGE/100 ELSE -A.CHARGE END AS MONTANT
FROM
(
SELECT
CREATE_DATE,
if(substr(trim(ACC_NBR),1,3)=237,substr(trim(ACC_NBR),4,9),trim(ACC_NBR)) MSISDN,
CHANNEL_ID SERVICE,
ACCT_RES_CODE,
COUNT(1) NOMBRE_TRANSACTION,
SUM(NVL(CHARGE,0)) CHARGE
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT
WHERE CREATE_DATE ='2020-04-05'
GROUP BY CREATE_DATE, if(substr(trim(ACC_NBR),1,3)=237,substr(trim(ACC_NBR),4,9),trim(ACC_NBR)), CHANNEL_ID, ACCT_RES_CODE
) A
LEFT JOIN DIM.DT_ZTE_USAGE_TYPE B ON A.SERVICE = B.USAGE_CODE
LEFT JOIN
(
SELECT DISTINCT
ACCT_RES_ID,
UPPER(ACCT_RES_NAME) ACCT_RES_NAME,
ACCT_RES_RATING_TYPE,
ACCT_RES_RATING_UNIT
FROM DIM.SPARK_DT_BALANCE_TYPE_ITEM
) C ON A.ACCT_RES_CODE = C.ACCT_RES_ID) FI
LEFT JOIN
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') EN on FI.msisdn=EN.msisdn and FI.CREATE_DATE=EN.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) ES on FI.msisdn=ES.msisdn
WHERE SERVICE IN (9,13,28,29,33)
group by FI.CREATE_DATE, EN.SITE_NAME, multi, operateur, tenure, segment
UNION ALL
select
site_name,
null as MULTI,
null as OPERATEUR,
null as TENURE,
null as SEGMENT,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as user_voice,
null as bdle_name,
null as BDLE_COST,
null as volume_data,
null as user_data1,
null as user_data2 ,
null as MONTANT,
destination_type,
destination,
mbytes_used as traffic_data,
rated_sms_total_count,
in_duration,
rated_duration,
null as paygo_data_revenue,
null as user_data,
null as paygo_voice_revenue,
null as og_total_call_duration,
'GEOMART' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
'2020-04-05' event_date
from mon.SPARK_FT_REVENU_LOCALISE
where event_date  = '2020-04-05'
UNION ALL
select
null as site_name,
null as MULTI,
null as OPERATEUR,
null as TENURE,
null as SEGMENT,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
count(distinct case when og_total_call_duration>0 then msisdn else null end) as user_voice,
null as bdle_name,
null as BDLE_COST,
sum(data_bytes_received + data_bytes_sent)/(1024*1024) as volume_data,
null as user_data1,
null as user_data2 ,
null as MONTANT,
null as  destination_type,
null as  destination,
null as  traffic_data,
null as  rated_sms_total_count,
null as  in_duration,
null as  rated_duration,
sum(data_main_rated_amount) as paygo_data_revenue,
count(distinct case when (data_bytes_received + data_bytes_sent)/(1024*1024)> 1 then msisdn else null end) as user_data,
sum(main_rated_tel_amount) as paygo_voice_revenue,
sum(og_total_call_duration) as og_total_call_duration,
'REPORTING_DG' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
'2020-04-05' event_date
from MON.SPARK_FT_MARKETING_DATAMART
where event_date = '2020-04-05'
) T



SELECT 
  TRANSACTION_DATE,
  TRANSACTION_TIME,
  CONTRACT_TYPE,
  OPERATOR_CODE,
  MAIN_USAGE_SERVICE_CODE,
  COMMERCIAL_OFFER,
  PREVIOUS_COMMERCIAL_OFFER,
  SUBS_SERVICE,
  SUBS_BENEFIT_NAME,
  SUBS_CHANNEL,
  SUBS_RELATED_SERVICE,
  SUM(SUBS_TOTAL_COUNT) SUBS_TOTAL_COUNT,
  SUM(SUBS_AMOUNT) SUBS_AMOUNT,
  SOURCE_PLATFORM,
  SOURCE_DATA,
  max(INSERT_DATE) INSERT_DATE,
  SERVICE_CODE,
  sum(MSISDN_COUNT) MSISDN_COUNT,
  sum(SUBS_EVENT_RATED_COUNT) SUBS_EVENT_RATED_COUNT,
  SUBS_PRICE_UNIT,
  sum(AMOUNT_SVA ) AMOUNT_SVA,
  sum(AMOUNT_VOICE_ONNET) AMOUNT_VOICE_ONNET,
  sum(AMOUNT_VOICE_OFFNET) AMOUNT_VOICE_OFFNET ,
  sum(AMOUNT_VOICE_INTER) AMOUNT_VOICE_INTER,
  sum(AMOUNT_VOICE_ROAMING) AMOUNT_VOICE_ROAMING,
  sum(AMOUNT_SMS_ONNET) AMOUNT_SMS_ONNET,
  sum(AMOUNT_SMS_OFFNET)AMOUNT_SMS_OFFNET,
  sum(AMOUNT_SMS_INTER) AMOUNT_SMS_INTER,
  sum(AMOUNT_SMS_ROAMING) AMOUNT_SMS_ROAMING,
  sum(AMOUNT_DATA) AMOUNT_DATA,
  COMBO 
FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###'
group by 
TRANSACTION_DATE,
TRANSACTION_TIME,
CONTRACT_TYPE,
OPERATOR_CODE,
MAIN_USAGE_SERVICE_CODE,
COMMERCIAL_OFFER,
PREVIOUS_COMMERCIAL_OFFER,
SUBS_SERVICE,
SUBS_BENEFIT_NAME,
SUBS_CHANNEL,
SUBS_RELATED_SERVICE,
SOURCE_PLATFORM,
SOURCE_DATA,
SERVICE_CODE,
SUBS_PRICE_UNIT,
AMOUNT_SVA,
AMOUNT_VOICE_ONNET,
AMOUNT_VOICE_OFFNET,
AMOUNT_VOICE_INTER,
AMOUNT_VOICE_ROAMING,
AMOUNT_SMS_ONNET,
AMOUNT_SMS_OFFNET,
AMOUNT_SMS_INTER,
AMOUNT_SMS_ROAMING,
AMOUNT_DATA,
COMBO

xxx\|\s+(\w+)\s+\|\s+\w+\(?\d*\)?\s+\|\s+\|
event_inst_id|re_id|billing_nbr|billing_imsi|calling_nbr|called_nbr|third_part_nbr|start_time|duration|lac_a|cell_a|lac_b|cell_b|calling_imei|called_imei|price_id1|price_id2|price_id3|price_id4|price_plan_id1|price_plan_id2|price_plan_id3|price_plan_id4|acct_res_id1|acct_res_id2|acct_res_id3|acct_res_id4|charge1|charge2|charge3|charge4|bal_id1|bal_id2|bal_id3|bal_id4|acct_item_type_id1|acct_item_type_id2|acct_item_type_id3|acct_item_type_id4|prepay_flag|pre_balance1|balance1|pre_balance2|balance2|pre_balance3|balance3|pre_balance4|balance4|international_roaming_flag|call_type|byte_up|byte_down|bytes|price_plan_code|session_id|result_code|prod_spec_std_code|yzdiscount|byzcharge1|byzcharge2|byzcharge3|byzcharge4|onnet_offnet|provider_id|prod_spec_id|termination_cause|b_prod_spec_id|b_price_plan_code|callspetype|chargingratio|sgsn_address|ggsn_address|rating_group|called_station_id|pdp_address|gpp_pdp_type|gpp_user_location_info|charge_unit|ismp_product_offer_id|ismp_provide_id|mnp_prefix|file_tap_id|ismp_product_id|