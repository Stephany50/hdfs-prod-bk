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
                WHERE CALLDATE = '2020-04-16' --AND TO_DATE(ORIGINAL_FILE_DATE)='2020-04-11'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE


SELECT
concat('HUA_DWH-010420-',SEQUENCE)
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
            SELECT
                DISTINCT
                cast (substring(original_file_name,16,21) as int) INDEX,
                1 MSC_TYPE
            FROM CDR.SPARK_IT_CRA_MSC_HUAWEI
            WHERE CALLDATE = '2020-04-01' --AND TO_DATE(ORIGINAL_FILE_DATE)='2020-04-11'
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


xxx\|\s+(\w+)\s+\|\s+\w+\(?\d*\)?\s+\|\s+\|
event_inst_id|re_id|billing_nbr|billing_imsi|calling_nbr|called_nbr|third_part_nbr|start_time|duration|lac_a|cell_a|lac_b|cell_b|calling_imei|called_imei|price_id1|price_id2|price_id3|price_id4|price_plan_id1|price_plan_id2|price_plan_id3|price_plan_id4|acct_res_id1|acct_res_id2|acct_res_id3|acct_res_id4|charge1|charge2|charge3|charge4|bal_id1|bal_id2|bal_id3|bal_id4|acct_item_type_id1|acct_item_type_id2|acct_item_type_id3|acct_item_type_id4|prepay_flag|pre_balance1|balance1|pre_balance2|balance2|pre_balance3|balance3|pre_balance4|balance4|international_roaming_flag|call_type|byte_up|byte_down|bytes|price_plan_code|session_id|result_code|prod_spec_std_code|yzdiscount|byzcharge1|byzcharge2|byzcharge3|byzcharge4|onnet_offnet|provider_id|prod_spec_id|termination_cause|b_prod_spec_id|b_price_plan_code|callspetype|chargingratio|sgsn_address|ggsn_address|rating_group|called_station_id|pdp_address|gpp_pdp_type|gpp_user_location_info|charge_unit|ismp_product_offer_id|ismp_provide_id|mnp_prefix|file_tap_id|ismp_product_id|