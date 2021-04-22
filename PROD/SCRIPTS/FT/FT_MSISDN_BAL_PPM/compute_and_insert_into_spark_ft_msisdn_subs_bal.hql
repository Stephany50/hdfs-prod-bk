
insert into mon.spark_ft_msisdn_subs_bal --- cette table doit exister
select
    msisdn
    , a.bdle_name
    , TRANSACTION_TIME
    , bal_id
    , ACCT_RES_RATING_UNIT
    , BEN_ACCT_ADD_VAL
    , nvl(nvl(BDLE_COST, ref_souscription.prix), 0) * (
        (case when dt_balance_usage.Voice_Onnet is not null then nvl(ref_souscription.coeff_onnet, 0) else 0 end)
        + (case when dt_balance_usage.Voice_Offnet is not null then nvl(ref_souscription.coeff_offnet, 0) else 0 end)
        + (case when dt_balance_usage.Voice_International is not null then nvl(ref_souscription.coeff_inter, 0) else 0 end)
        + (case when dt_balance_usage.Voice_Roaming is not null then nvl(ref_souscription.coeff_roaming_voix, 0) else 0 end)
        + (case when dt_balance_usage.SMS_Onnet is not null or dt_balance_usage.SMS_Offnet is not null or dt_balance_usage.SMS_International is not null then nvl(ref_souscription.coef_sms, 0) else 0 end)
        --+ (case when dt_balance_usage.SMS_Offnet is not null then nvl(ref_souscription.coeff_onnet, 0) else 0 end)
        --+ (case when dt_balance_usage.SMS_International is not null then nvl(ref_souscription.coeff_onnet, 0) else 0 end)
        + (case when dt_balance_usage.SMS_Roaming is not null then nvl(ref_souscription.coeff_roaming_sms, 0) else 0 end)
        + (case when dt_balance_usage.Data_Local is not null then nvl(ref_souscription.coeff_data, 0) else 0 end)
        + (case when dt_balance_usage.Data_roaming is not null then nvl(ref_souscription.coeff_roaming_data, 0) else 0 end)
    )/100 revenu_for_bal
    , ref_souscription.validite
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        ITSUBSC.msisdn
        , NVL(PRICE_PLAN.PRICE_PLAN_NAME, CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING)) bdle_name
        , TRANSACTION_TIME
        , bal_id_col bal_id
        , max(ACCT_RES_RATING_UNIT) ACCT_RES_RATING_UNIT
        , max(BEN_ACCT_ADD_VAL) BEN_ACCT_ADD_VAL
        , MAX(
            CASE
                WHEN HybIPP.OFFER_NAME is not null THEN 0
                WHEN Services_dynamique.MSISDN is not null THEN NVL(cast(Services_dynamique.BDLE_COST as double),0)
                WHEN NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING)) = services_default.BDLE_NAME and Services_dynamique.MSISDN  is null and ITSUBSC.CHANNEL_ID='32' THEN NVL(cast(services_default.BDLE_COST as double), 0)
                WHEN ITSUBSC.CHANNEL_ID='32' and Services_dynamique.BDLE_NAME is null and services_default.BDLE_NAME is null THEN NVL(AMOUNT_VIA_OM_VAS, 0)
                ELSE EVENT_COST / 100
            END
        ) bdle_cost ------------- En attente du reférentiel décrit ci-bas
        , max(BEN_ACCT_ID) BEN_ACCT_ID
    from
    (
        select
            SUBSTRING(ACC_NBR, -9) msisdn
            , DATE_FORMAT(NQ_CREATEDDATE, 'HHmmss') TRANSACTION_TIME
            , bal_id_col
            , CAST(EVENT_COST AS DOUBLE) EVENT_COST
            , cast(SPLIT(BEN_BAL, '&')[0] as bigint) BEN_ACCT_ID --- Va servir pour joindre au reférentiel ci bas du moins soit ca, soit le ACCT_RES_NAME soit le ACCT_RES_STD_CODE
            , CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[1] AS double)) AS double) BEN_ACCT_ADD_VAL
            , SUBS_EVENT_ID
            , CHANNEL_ID
            , PRICE_PLAN_CODE
            , OLD_PRICE_PLAN_CODE
            , PROD_SPEC_CODE
            , OLD_PROD_SPEC_CODE
            , RELATED_PROD_CODE
        from
        (
            SELECT *
            FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION
            WHERE CREATEDDATE = '###SLICE_VALUE###' AND original_file_name not like '%in_postpaid%'
        ) a0
        lateral view posexplode(SPLIT(NVL(bal_id, ''), ',')) TMP1 as pos_bal_id_col, bal_id_col
        lateral view posexplode(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '#')) TMP2 as pos_BEN_BAL, BEN_BAL 
        where pos_bal_id_col = pos_BEN_BAL and bal_id_col is not null and bal_id_col != ''
    ) ITSUBSC
    LEFT JOIN
    (
        SELECT 
            acct_res_id
            , MAX(ACCT_RES_NAME) ACCT_RES_NAME
            , MAX(ACCT_RES_RATING_SERVICE_CODE) ACCT_RES_RATING_SERVICE_CODE
            , MAX(ACCT_RES_RATING_UNIT) ACCT_RES_RATING_UNIT
        FROM DIM.DT_BALANCE_TYPE_ITEM
        GROUP BY acct_res_id
    ) BALANCE_TYPE_ITEM ON ITSUBSC.BEN_ACCT_ID = cast(BALANCE_TYPE_ITEM.acct_res_id as bigint)
    LEFT JOIN DIM.DT_SUBSCRIPTION_SERVICE SERVSUBSC ON NVL(ITSUBSC.SUBS_EVENT_ID, 1000000) = SERVSUBSC.SUBSCRIPTION_SERVICE_ID
    LEFT JOIN DIM.DT_SUBSCRIPTION_CHANNEL CHANSUBSC ON NVL(ITSUBSC.CHANNEL_ID, 1000000) = CHANSUBSC.CHANNEL_ID
    LEFT JOIN
    (
        SELECT
            NVL(PRICE_PLAN_CODE, 'UNKNOWN')PRICE_PLAN_CODE
            , MIN( PRICE_PLAN_NAME) PRICE_PLAN_NAME
        FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT
        WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
        GROUP BY NVL(PRICE_PLAN_CODE, 'UNKNOWN')
    ) PRICE_PLAN ON NVL(ITSUBSC.PRICE_PLAN_CODE, '1000000') = PRICE_PLAN.PRICE_PLAN_CODE
    LEFT JOIN 
    (
        SELECT
            NVL(PRICE_PLAN_CODE, 'UNKNOWN')PRICE_PLAN_CODE
            , MIN(PRICE_PLAN_NAME) PRICE_PLAN_NAME
        FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT
        WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
        GROUP BY NVL(PRICE_PLAN_CODE, 'UNKNOWN')
    ) OLD_PRICE_PLAN ON NVL(ITSUBSC.OLD_PRICE_PLAN_CODE, '1000000') = OLD_PRICE_PLAN.PRICE_PLAN_CODE
    LEFT JOIN 
    (
        SELECT
            STD_CODE
            , MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
        FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
        WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
        GROUP BY STD_CODE
    ) PROD_SPEC ON NVL(ITSUBSC.PROD_SPEC_CODE, '1000000') =  PROD_SPEC.STD_CODE
    LEFT JOIN 
    (
        SELECT
            STD_CODE
            , MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
        FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
        WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
        GROUP BY STD_CODE
    ) REL_PROD ON NVL(ITSUBSC.RELATED_PROD_CODE, '1000000') = REL_PROD.STD_CODE
    LEFT JOIN 
    (
        SELECT
            STD_CODE
            , MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
        FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
        WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
        GROUP BY STD_CODE
    ) OLD_PROD_SPEC ON NVL(ITSUBSC.OLD_PROD_SPEC_CODE, '1000000') =  OLD_PROD_SPEC.STD_CODE
    LEFT JOIN 
    (
        SELECT
            EVENT
            , MAX(SERVICE_CODE) SERVICE_CODE
            , MIN(VOIX_ONNET) VOIX_ONNET
            , MIN(VOIX_OFFNET) VOIX_OFFNET
            , MIN(VOIX_INTER)VOIX_INTER
            , MIN(VOIX_ROAMING)VOIX_ROAMING
            , MIN(SMS_ONNET) SMS_ONNET
            , MIN(SMS_OFFNET) SMS_OFFNET
            , MIN(SMS_INTER) SMS_INTER
            , MIN(SMS_ROAMING)SMS_ROAMING
            , MIN(DATA_BUNDLE) DATA_BUNDLE
            , MIN(SVA) SVA
            , MIN(PRIX) PRIX
            , MIN(combo) COMBO
        FROM DIM.DT_SERVICES DTSVS GROUP BY EVENT 
    ) DTSVS ON NVL(PRICE_PLAN.PRICE_PLAN_NAME, SERVSUBSC.SUBSCRIPTION_SERVICE_NAME) = DTSVS.EVENT
    LEFT JOIN 
    (
        SELECT
            MAX(IPP_AMOUNT) AMOUNT_VIA_OM_VAS
            , IPP_NAME
        FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT IPP_EXTRACT
        WHERE IPP_EXTRACT.ORIGINAL_FILE_DATE IN (SELECT MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT)
        GROUP BY IPP_NAME
    ) IPP_EXTRACT ON NVL(PRICE_PLAN.PRICE_PLAN_NAME, SERVSUBSC.SUBSCRIPTION_SERVICE_NAME) = IPP_EXTRACT.IPP_NAME
    LEFT JOIN 
    (
        select 
            msisdn
            , bdle_name
            , bdle_cost
        from cdr.dt_Services_dynamique
    ) Services_dynamique on Services_dynamique.bdle_name = NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING)) and ITSUBSC.msisdn = Services_dynamique.MSISDN and NVL(CHANSUBSC.CHANNEL_NAME, CAST(ITSUBSC.CHANNEL_ID AS STRING))='32'
    LEFT JOIN
    (
        select *
        from cdr.dt_services_default
    ) services_default on services_default.bdle_name = NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING)) and  NVL(CHANSUBSC.CHANNEL_NAME, CAST(ITSUBSC.CHANNEL_ID AS STRING))='32'
    LEFT JOIN 
    (
        SELECT
            OFFER_NAME
            , MAX(OFFER_ID) OFFER_ID
            , MAX(OFFER_CODE) OFFER_CODE
        FROM DIM.DT_IPP_HYBRID GROUP BY OFFER_NAME 
    ) HybIPP ON NVL(PRICE_PLAN.PRICE_PLAN_NAME, SERVSUBSC.SUBSCRIPTION_SERVICE_NAME) = HybIPP.OFFER_NAME
    group by ITSUBSC.msisdn
        , NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(ITSUBSC.PRICE_PLAN_CODE AS STRING))
        , TRANSACTION_TIME
        , bal_id_col
) a
left join dim.dt_balance_usage dt_balance_usage ON a.BEN_ACCT_ID = cast(dt_balance_usage.Acct_res_id as bigint)
left join DIM.DT_CBM_REF_SOUSCRIPTION_PRICE ref_souscription on trim(upper(a.bdle_name)) = trim(upper(ref_souscription.BDLE_NAME))
where ref_souscription.validite is not null

