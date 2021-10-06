SELECT
replace(nvl(CHANNEL_USER_ID,''),';','-') CHANNEL_USER_ID,
replace(nvl(PARENT_USER_ID,''),';','-') PARENT_USER_ID,
replace(nvl(OWNER_USER_ID,''),';','-') OWNER_USER_ID,
replace(nvl(USER_TYPE,''),';','-') USER_TYPE,
replace(nvl(EXTERNAL_CODE,''),';','-') EXTERNAL_CODE,
replace(nvl(PRIMARY_MSISDN,''),';','-') PRIMARY_MSISDN,
replace(nvl(USER_STATUS,''),';','-') USER_STATUS,
replace(nvl(LOGIN_ID,''),';','-') LOGIN_ID,
replace(nvl(CATEGORY_CODE,''),';','-') CATEGORY_CODE,
replace(nvl(CATEGORY_NAME,''),';','-') CATEGORY_NAME,
replace(nvl(GEOGRAPHICAL_DOMAIN_CODE,''),';','-') GEOGRAPHICAL_DOMAIN_CODE,
replace(nvl(GEOGRAPHICAL_DOMAIN_NAME,''),';','-') GEOGRAPHICAL_DOMAIN_NAME,
replace(nvl(CHANNEL_USER_NAME,''),';','-') CHANNEL_USER_NAME,
replace(nvl(CITY,''),';','-') CITY,
replace(nvl(STATE,''),';','-') STATE,
replace(nvl(COUNTRY,''),';','-') COUNTRY,
replace(nvl(ORIGINAL_FILE_NAME,''),';','-') FILE_NAME,
nvl(to_date(transaction_date),'') transaction_date,
nvl(to_date(INSERT_DATE),'') INSERT_DATE
from CDR.SPARK_IT_ZEBRA_MASTER
where transaction_date =  '###SLICE_VALUE###'
