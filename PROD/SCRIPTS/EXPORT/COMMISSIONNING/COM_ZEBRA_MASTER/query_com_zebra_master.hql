SELECT
replace(CHANNEL_USER_ID,';','-') CHANNEL_USER_ID,
replace(PARENT_USER_ID,';','-') PARENT_USER_ID,
replace(OWNER_USER_ID,';','-') OWNER_USER_ID,
replace(USER_TYPE,';','-') USER_TYPE,
replace(EXTERNAL_CODE,';','-') EXTERNAL_CODE,
replace(PRIMARY_MSISDN,';','-') PRIMARY_MSISDN,
replace(USER_STATUS,';','-') USER_STATUS,
replace(LOGIN_ID,';','-') LOGIN_ID,
replace(CATEGORY_CODE,';','-') CATEGORY_CODE,
replace(CATEGORY_NAME,';','-') CATEGORY_NAME,
replace(GEOGRAPHICAL_DOMAIN_CODE,';','-') GEOGRAPHICAL_DOMAIN_CODE,
replace(GEOGRAPHICAL_DOMAIN_NAME,';','-') GEOGRAPHICAL_DOMAIN_NAME,
replace(CHANNEL_USER_NAME,';','-') CHANNEL_USER_NAME,
replace(CITY,';','-') CITY,
replace(STATE,';','-') STATE,
replace(COUNTRY,';','-') COUNTRY,
replace(ORIGINAL_FILE_NAME,';','-') FILE_NAME,
transaction_date,
INSERT_DATE
from CDR.SPARK_IT_ZEBRA_MASTER
where transaction_date =  '###SLICE_VALUE###'