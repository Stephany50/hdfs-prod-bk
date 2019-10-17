INSERT INTO CTI.DT_RESOURCE
SELECT
RESOURCE_KEY,  
TENANT_KEY,
CREATE_AUDIT_KEY,   
UPDATE_AUDIT_KEY,   
SWITCH_NAME,
IVR_NAME,
RESOURCE_TYPE,
RESOURCE_TYPE_CODE,
RESOURCE_SUBTYPE,
RESOURCE_NAME,
AGENT_FIRST_NAME,
AGENT_LAST_NAME,
EMPLOYEE_ID,
EXTERNAL_RESOURCE_ID,
RESOURCE_CFG_DBID,
RESOURCE_CFG_TYPE_ID,
RESOURCE_ALIAS,
NETWORK_RESOURCE_FLAG,
GMT_START_TIME,
GMT_END_TIME,
PURGE_FLAG,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
   	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
   	CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.tt_exp_dim_resource C
--LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.DT_RESOURCE)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
--WHERE T.FILE_NAME IS NULL;