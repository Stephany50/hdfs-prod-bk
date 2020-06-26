INSERT INTO MON.SPARK_TF_GLOBAL_CONSO_MSISDN_MONTH
select
msisdn
, max(CASE WHEN type_conso = 'VOIX_SMS' THEN FORMULE ELSE NULL end) profile_voice_sms
, max(CASE WHEN type_conso = 'DATA' THEN FORMULE ELSE NULL end) profile_data
, max(CASE WHEN type_conso = 'SUBSCRIPTION' THEN FORMULE ELSE NULL end) profile_subscr
, sum(main) main_cost_total
, sum(promo) promo_cost_total
, sum(case when type_conso = 'VOIX_SMS' then main else 0 end) main_cost_voice_sms
, sum(case when type_conso = 'DATA' then main else 0 end) main_cost_data --Pay as you go + Gos SVA
, sum(case when type_conso = 'SUBSCRIPTION' then main else 0 end) main_cost_subscr
, sum(case when type_conso = 'VOIX_SMS' then promo else 0 end) promo_cost_voice_sms
, sum(case when type_conso = 'DATA' then promo else 0 end) promo_cost_data
, sum(case when type_conso = 'SUBSCRIPTION' then promo else 0 end) promo_cost_subscr
----- spécifiquement roaming
, sum(main_roam) main_cost_total_roam
, sum(promo_roam) promo_cost_total_roam
, sum(case when type_conso = 'VOIX_SMS' then main_roam else 0 end) main_cost_voice_sms_roam
, sum(case when type_conso = 'DATA' then main_roam else 0 end) main_cost_data_roam
, sum(case when type_conso = 'SUBSCRIPTION' then main_roam else 0 end) main_cost_subscr_roam
, sum(case when type_conso = 'VOIX_SMS' then promo_roam else 0 end) promo_cost_voice_sms_roam
, sum(case when type_conso = 'DATA' then promo_roam else 0 end) promo_cost_data_roam
, sum(case when type_conso = 'SUBSCRIPTION' then promo_roam else 0 end) promo_cost_subscr_roam
--
, sum(main_roam_sortant) main_cost_total_roamout
, sum(promo_roam_sortant) promo_cost_total_roamout
, sum(case when type_conso = 'VOIX_SMS' then main_roam_sortant else 0 end) main_cost_voice_sms_roamout
, sum(case when type_conso = 'DATA' then main_roam_sortant else 0 end) main_cost_data_roamout
, sum(case when type_conso = 'SUBSCRIPTION' then main_roam_sortant else 0 end) main_cost_subscr_roamout
, sum(case when type_conso = 'VOIX_SMS' then promo_roam_sortant else 0 end) promo_cost_voice_sms_roamout
, sum(case when type_conso = 'DATA' then promo_roam_sortant else 0 end) promo_cost_data_roamout
, sum(case when type_conso = 'SUBSCRIPTION' then promo_roam_sortant else 0 end) promo_cost_subscr_roamout
--
, sum(main_roam_entrant) main_cost_total_roamin
, sum(promo_roam_entrant) promo_cost_total_roamin
, sum(case when type_conso = 'VOIX_SMS' then main_roam_entrant else 0 end) main_cost_voice_sms_roamin
, sum(case when type_conso = 'DATA' then main_roam_entrant else 0 end) main_cost_data_roamin
, sum(case when type_conso = 'SUBSCRIPTION' then main_roam_entrant else 0 end) main_cost_subscr_roamin
, sum(case when type_conso = 'VOIX_SMS' then promo_roam_entrant else 0 end) promo_cost_voice_sms_roamin
, sum(case when type_conso = 'DATA' then promo_roam_entrant else 0 end) promo_cost_data_roamin
, sum(case when type_conso = 'SUBSCRIPTION' then promo_roam_entrant else 0 end) promo_cost_subscr_roamin
------------
, CURRENT_TIMESTAMP insert_date
, 'FT_CONSO_MSISDN_MONTH|FT_DATA_CONSO_MSISDN_MONTH|FT_SUBSCRIPTION_MSISDN_MONTH' source_table
, NULL value_segment
, EVENT_MONTH
from (--données voix/sms
SELECT EVENT_MONTH, 'VOIX_SMS' type_conso
, msisdn, upper(FORMULE) FORMULE
, MAIN_CALL_COST main
, PROMOTIONAL_CALL_COST promo
----- spécifiquement roaming
, (ROAM_MAIN_CONSO + INROAM_MAIN_CONSO) main_roam
, ROAM_MAIN_CONSO main_roam_sortant
, INROAM_MAIN_CONSO main_roam_entrant
, (ROAM_TOTAL_CONSO - ROAM_MAIN_CONSO + INROAM_TOTAL_CONSO - INROAM_MAIN_CONSO) promo_roam
, (ROAM_TOTAL_CONSO - ROAM_MAIN_CONSO) promo_roam_sortant
, (INROAM_TOTAL_CONSO - INROAM_MAIN_CONSO) promo_roam_entrant
---------------
FROM MON.SPARK_FT_CONSO_MSISDN_MONTH
WHERE EVENT_MONTH='###SLICE_VALUE###'
--AND ROWNUM < 10
UNION --données data Pay as you go + Gos SVA
SELECT EVENT_MONTH, 'DATA' type_conso
, msisdn, upper(COMMERCIAL_OFFER) FORMULE
, (CASE WHEN MAIN_RATED_AMOUNT < 0 AND GOS_DEBIT_AMOUNT >= 0 THEN GOS_DEBIT_AMOUNT
WHEN GOS_DEBIT_AMOUNT < 0 AND MAIN_RATED_AMOUNT >= 0 THEN MAIN_RATED_AMOUNT
ELSE MAIN_RATED_AMOUNT + GOS_DEBIT_AMOUNT END
) main
, (CASE WHEN PROMO_RATED_AMOUNT < 0 THEN 0 ELSE PROMO_RATED_AMOUNT end) promo
----- spécifiquement roaming
, (CASE WHEN MAIN_RATED_AMOUNT_ROAMING < 0 THEN 0 ELSE MAIN_RATED_AMOUNT_ROAMING end) main_roam
, (CASE WHEN MAIN_RATED_AMOUNT_ROAMING < 0 THEN 0 ELSE MAIN_RATED_AMOUNT_ROAMING end) main_roam_sortant
, 0 main_roam_entrant
, (CASE WHEN PROMO_RATED_AMOUNT_ROAMING < 0 THEN 0 ELSE PROMO_RATED_AMOUNT_ROAMING end) promo_roam
, (CASE WHEN PROMO_RATED_AMOUNT_ROAMING < 0 THEN 0 ELSE PROMO_RATED_AMOUNT_ROAMING end) promo_roam_sortant
, 0 promo_roam_entrant
---------------
FROM MON.SPARK_FT_DATA_CONSO_MSISDN_MONTH
WHERE EVENT_MONTH='###SLICE_VALUE###'
--AND ROWNUM < 10
UNION  --données souscriptions
SELECT EVENT_MONTH, 'SUBSCRIPTION' type_conso
, msisdn, upper(CUSTOMER_PROFILE) FORMULE
, TOTAL_SUBS_AMOUNT main
, 0 promo
----- spécifiquement roaming
, 0 main_roam
, 0 main_roam_sortant
, 0 main_roam_entrant
, 0 promo_roam
, 0 promo_roam_sortant
, 0 promo_roam_entrant
---------------
FROM MON.SPARK_FT_SUBSCRIPTION_MSISDN_MONTH
WHERE EVENT_MONTH='###SLICE_VALUE###'
) T
group by EVENT_MONTH
, msisdn