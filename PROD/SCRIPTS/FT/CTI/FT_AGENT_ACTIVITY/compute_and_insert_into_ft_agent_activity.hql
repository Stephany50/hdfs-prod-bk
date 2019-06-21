INSERT INTO CTI.FT_AGENT_ACTIVITY PARTITION(START_TIME)
SELECT RES.Resource_Name EMPLOYE,
 RES.Agent_First_Name FIRST_NAME,
 RES.Agent_Last_Name LAST_NAME,
 FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+3600) START_TIME_NG ,
 FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+3600) END_TIME ,
 STATE.state_type_code TYPE_STATE,
 STATE.state_name_code NAME_STATE,

 case STATE_R.Software_Reason_Value
 when '1' then 'Formation'
 when '2' then 'Debriefing'
 when '3' then 'Retrait'
 when '4' then 'Pause'
 when '5' then 'Réunion'
 when '9' then 'ExtendedACW'
 else STATE_R.Software_Reason_Value
 end REASON_VALUE,
 RSF.TOTAL_DURATION TOTAL_DURATION,
 current_timestamp INSERT_DATE,
 FROM_UNIXTIME(UNIX_TIMESTAMP(TO_DATE(FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+3600)))+(CAST((DATE_FORMAT(FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+3600),'HH')*60 + DATE_FORMAT(FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+4500),'mm') + DATE_FORMAT(FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+3600),'ss')/60)/15 as BIGINT))*15*60) start_time_15min_slice,
 FROM_UNIXTIME(CAST(RSF.Start_Ts AS BIGINT)+3600) START_TIME
 FROM
(select * from CTI.IT_SM_STATE_FACT where original_file_date='###SLICE_VALUE###' ) RSF
 LEFT JOIN (SELECT * FROM CTI.IT_SM_RES_STATE_REASON_FACT WHERE original_file_date='###SLICE_VALUE###') RSRF ON (RSF.Sm_Res_State_Fact_Key=RSRF.Sm_Res_State_Fact_Key)

 LEFT JOIN (SELECT * FROM CTI.DT_RESOURCE_STATE_REASON WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) FROM CTI.DT_RESOURCE_STATE_REASON)) STATE_R
 ON (RSRF.Resource_State_Reason_Key=STATE_R.Resource_State_Reason_Key)

 join (SELECT * FROM CTI.DT_RESOURCE_STATE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_RESOURCE_STATE)) STATE
 ON (RSF.Resource_State_Key=STATE.Resource_State_Key)

 JOIN (SELECT * FROM CTI.DT_RESOURCE WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM CTI.DT_RESOURCE_STATE)) RES
 ON (RSF.Resource_Key=RES.Resource_Key)
 WHERE
 RSF.ACTIVE_FLAG = 0