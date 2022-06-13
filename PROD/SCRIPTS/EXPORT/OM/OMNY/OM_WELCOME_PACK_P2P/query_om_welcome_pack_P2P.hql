SELECT msisdn,
creation_date,
user_first_name,
user_last_name,
owner_msisdn,
city,
action_type
FROM
CDR.SPARK_IT_OM_SUBSCRIBERS
WHERE upper(trim(action_type))='CREATION'
AND TO_DATE(REGISTERED_ON) ='###SLICE_VALUE###'