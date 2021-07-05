select
nvl(to_date(EVENT_DATE),'') EVENT_DATE,
nvl(to_date(EVENT_TIME),'') EVENT_TIME,
replace(nvl(CHANNEL_USER_ID,''),',','-')  CHANNEL_USER_ID,
replace(nvl(USER_NAME,''),',','-') USER_NAME,
nvl(MOBILE_NUMBER,'') MOBILE_NUMBER,
replace(nvl(CATEGORY,''),',','-') CATEGORY,
nvl(MOBILE_NUMBER_1,'') MOBILE_NUMBER_1,
replace(nvl(GEOGRAPHICAL_DOMAIN,''),',','-') GEOGRAPHICAL_DOMAIN,
replace(nvl(PRODUCT,''),',','-') PRODUCT,
replace(nvl(PARENT_USER_NAME,''),',','-') PARENT_USER_NAME,
replace(nvl(OWNER_USER_NAME,''),',','-') OWNER_USER_NAME,
replace(nvl(AVAILABLE_BALANCE,''),',','-') AVAILABLE_BALANCE,
replace(nvl(AGENT_BALANCE,''),',','-') AGENT_BALANCE,
replace(nvl(ORIGINAL_FILE_NAME,''),',','-') ORIGINAL_FILE_NAME,
nvl(to_date(ORIGINAL_FILE_DATE),'') ORIGINAL_FILE_DATE,
nvl(to_date(INSERT_DATE),'') INSERT_DATE,
replace(nvl(USER_STATUS,''),',','-') USER_STATUS,
replace(nvl(TO_CHANGE,''),',','-') TO_CHANGE,
nvl(to_date(MODIFIED_ON),'') MODIFIED_ON
from  CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
where event_date ="###SLICE_VALUE###" 