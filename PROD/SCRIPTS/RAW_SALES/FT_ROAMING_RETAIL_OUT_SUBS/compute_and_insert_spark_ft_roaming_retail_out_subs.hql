INSERT INTO MON.SPARK_FT_ROAMING_RETAIL_OUT_SUBS
SELECT E.served_party_msisdn MSISDN, 
E.subscription_service_details FORFAIT_ROAMING, E.subscription_channel SUBS_CHANNEL,
(E.SA *C.coef_voix) REVENU_VOIX, (E.SA *C.coef_sms) REVENU_SMS,
(E.SA *C.data_combo) REVENU_DATA_COMBO, (E.SA *C.data_pur) REVENU_DATA_PUR, E.SA REVENU_TOTAL,
E.transaction_date EVENT_DATE
FROM
(SELECT A.transaction_date,A.served_party_msisdn, A.subscription_service_details,A.subscription_channel, sum(rated_amount) SA
 FROM mon.spark_ft_subscription A 
 INNER JOIN dim.spark_dt_offer_profiles B ON upper(A.commercial_offer) = upper(B.profile_code)
 WHERE B.profile_code is not null
 AND A.transaction_date ='###SLICE_VALUE###' AND (upper(trim(subscription_service_details)) LIKE '%ROAMING%' OR 
 upper(trim(subscription_service_details)) IN ('IPP PREPAID PASS VOYAGE DECOUVERTE','IPP PREPAID PASS VOYAGE JOUR',
'IPP PREPAID PASS VOYAGE SEMAINE','IPP PREPAID PASS VOYAGE MOIS','IPP PREPAID 3G VOYAGE JOUR',
'IPP PREPAID 3G VOYAGE SEMAINE','IPP PREPAID 3G VOYAGE MOIS'))
 GROUP BY A.transaction_date,A.served_party_msisdn, A.subscription_service_details,A.subscription_channel) E
INNER JOIN
(SELECT event,
  (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
  (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
  (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
  (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur
FROM dim.dt_services) C ON upper(trim(E.subscription_service_details)) = upper(trim(C.EVENT))