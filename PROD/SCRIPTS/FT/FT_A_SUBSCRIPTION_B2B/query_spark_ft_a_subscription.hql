SELECT 
E.transaction_date, E.subscription_service_details SUBS_BENEFIT_NAME,
E.SA,(E.SA *C.coef_voix + E.SA *C.coef_sms) voice_sms,
(E.SA *C.data_combo) data_combo,  (E.SA *C.data_pur) data_pur
FROM
(SELECT A.transaction_date, A.subscription_service_details, sum(rated_amount) SA
 FROM mon.spark_ft_subscription A 
 INNER JOIN dim.spark_dt_offer_profiles B ON upper(A.commercial_offer) = upper(B.profile_code)
 WHERE B.profile_code is not null and upper(B.segmentation) = 'B2B' 
 AND transaction_date ='###SLICE_VALUE###' AND upper(trim(subscription_service_details))!='RP DATA SHAPE_5120K' 
 AND subscription_service_details is not null
 GROUP BY transaction_date ,subscription_service_details) E
INNER JOIN
(SELECT event,
  (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
  (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
  (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
  (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur
FROM dim.spark_dt_services) C ON upper(trim(E.subscription_service_details)) = upper(trim(C.EVENT))