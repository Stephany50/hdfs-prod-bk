select A.transaction_date ,
    A.SUBS_BENEFIT_NAME ,
     A.SA,(A.SA *coef_voix + A.SA *coef_sms) voice_sms,
      (A.SA *data_combo) data_combo,
      (A.SA *data_pur) data_pur
from
(
    select
        transaction_date ,
        SUBS_BENEFIT_NAME,
        sum(SUBS_AMOUNT) SA
    from ft_a_subscription
    where transaction_date >='01/07/2020' and upper(trim(subs_benefit_name))!='RP DATA SHAPE_5120K' and subs_benefit_name is not null
    group by transaction_date ,SUBS_BENEFIT_NAME
) A,
(select event,
  (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
  (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
  (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
  (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
  nvl(DATA_BUNDLE,0) data
from dim.dt_services
) B
where upper(trim(A.SUBSCRIPTION_SERVICE_DETAILS)) = upper(trim(B.EVENT))

 select sum(data) , count(*)  from (select SUBS_AMOUNT*data data from (
    select * from AGG.spark_ft_a_subscription where transaction_date='2021-01-01'
 ) a left join (select event,
                  (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
                  (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
                  (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
                  (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
                  nvl(DATA_BUNDLE,0) data
                from dim.dt_services
 ) b on upper(trim(A.SUBS_BENEFIT_NAME)) = upper(trim(B.EVENT))
 )d


 select sum(data) , count(*)  from (select SUBS_AMOUNT*data data from (
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
        FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE='2021-02-01'
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
        COMBO
 ) a left join (select event,
                  (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
                  (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
                  (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
                  (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
                  nvl(DATA_BUNDLE,0) data
                from dim.dt_services
 ) b on upper(trim(A.SUBS_BENEFIT_NAME)) = upper(trim(B.EVENT))
 )d
 
 

create table dim.dt_services (
EVENT VARCHAR(250),
EVENT_SOURCE VARCHAR(250),
SERVICE_CODE VARCHAR(250),
DESCRIPTION VARCHAR(250),
INSERT_DATE TIMESTAMP,
FAMILY_MKT VARCHAR(250),
FAMILY_EOY VARCHAR(250),
VALIDITY VARCHAR(250),
USAGE VARCHAR(250),
FLYBOX_DONGLE VARCHAR(250),
OFFRES_B_TO_B VARCHAR(250),
USAGE_MKT VARCHAR(250),
PRODUIT VARCHAR(250),
VOIX_ONNET DOUBLE,
VOIX_OFFNET DOUBLE,
VOIX_INTER DOUBLE,
VOIX_ROAMING DOUBLE,
SMS_ONNET DOUBLE,
SMS_OFFNET DOUBLE,
SMS_INTER DOUBLE,
SMS_ROAMING DOUBLE,
DATA_BUNDLE DOUBLE,
SVA DOUBLE,
VALIDITE DOUBLE,
MARCHE VARCHAR(250),
SEGMENT VARCHAR(250),
FAMILY_MKTSUBS VARCHAR(250),
FAMILY_MAGICBUNDLES VARCHAR(250),
PRIX DOUBLE,
TYPE_OCM VARCHAR(250),
COMBO VARCHAR(250)
)
