SELECT
event_date,
access_key,
account_id,
activation_date,
commercial_offer_assign_date,
commercial_offer,
lang,
main_imsi,
provisioning_date,
cast(round(main_credit) as INT) main_credit,
promo_credit,
sms_credit,
data_credit,
osp_status,
operator_code
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
WHERE EVENT_DATE = "###SLICE_VALUE###"