INSERT INTO CTI.FT_AGENT_SESSION

SELECT

 res.resource_name as RESOURCE_NAME,

 res.agent_first_name as AGENT_FIRST_NAME,

 res.agent_last_name as AGENT_LAST_NAME,

 FROM_UNIXTIME(CAST(RSF.Start_Ts as INT) + 3600) as LOGIN,

 FROM_UNIXTIME(CAST(RSF.End_Ts as INT) + 3600) as LOGOUT,

 RSF.TOTAL_DURATION as DURATION,

 current_timestamp INSERT_DATE,

 TO_DATE(FROM_UNIXTIME(CAST(RSF.Start_Ts as INT) + 3600)) as CALL_DATE

 FROM

 (SELECT * FROM CTI.IT_SM_RES_SESSION_FACT WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') RSF

 INNER JOIN (SELECT * FROM CTI.DT_RESOURCE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_RESOURCE))RES

 ON (RSF.Resource_Key=RES.Resource_Key)

 WHERE RSF.ORIGINAL_FILE_DATE=('###SLICE_VALUE###')

 ORDER BY

 res.resource_name asc,

 FROM_UNIXTIME(CAST(RSF.Start_Ts as INT) + 3600) asc ;