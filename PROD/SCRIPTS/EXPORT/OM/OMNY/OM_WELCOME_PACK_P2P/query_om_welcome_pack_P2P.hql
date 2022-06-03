SELECT msisdn,
user_first_name,
user_last_name,
dob,
address1,
address2,
state,
city,
sex,
id_number,
account_status,
creation_date,
registered_on,
created_by,
owner_msisdn,
action_type
FROM
CDR.SPARK_IT_OM_SUBSCRIBERS
WHERE MODIFICATION_DATE = TO_DATE(REGISTERED_ON)
AND upper(trim(action_type))='CREATION'
AND TO_DATE(REGISTERED_ON) ='###SLICE_VALUE###'