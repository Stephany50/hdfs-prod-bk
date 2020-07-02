insert into MON.SPARK_tt_analyse_decon_refill1
SELECT a.*
, nvl(c.REFILL_MONTH, '') REFILL_MONTH
--
, nvl(c.REFILL_AMOUNT,0) REFILL_AMOUNT
, nvl(c.REFILL_COUNT,0) REFILL_COUNT
--
, nvl(b.ADMINISTRATIVE_REGION,'') ADMINISTRATIVE_REGION
, nvl(b.COMMERCIAL_REGION,'') COMMERCIAL_REGION
, nvl(b.TOWNNAME,'') TOWNNAME
, nvl(b.SITE_NAME,'') SITE_NAME
--
, nvl(e.TAC_CODE_HANDSET,'') TAC_CODE_HANDSET
, nvl(e.CONSTRUCTOR, '') CONSTRUCTOR_HANDSET
, nvl(e.MODEL,'') MODEL_HANDSET
, nvl(e.DATA_COMPATIBLE,'') DATA_COMPATIBLE_HANDSET
--
, nvl(f.TAC_CODE_HANDSET,'') TAC_CODE_HANDSET_3MBE4
, nvl(f.CONSTRUCTOR, '') CONSTRUCTOR_HANDSET_3MBE4
, nvl(f.MODEL,'') MODEL_HANDSET_3MBE4
, nvl(f.DATA_COMPATIBLE,'') DATA_COMPATIBLE_HANDSET_3MBE4
--ajout du 04/08/2016
, nvl(e.IMEI,'') IMEI_HANDSET
, nvl(e.DATA2G_COMPATIBLE,'') DAT2G_COMPATIBLE_HANDSET
, nvl(e.DATA3G_COMPATIBLE,'') DAT3G_COMPATIBLE_HANDSET
, nvl(e.DATA4G_COMPATIBLE,'') DAT4G_COMPATIBLE_HANDSET
--ajout du 04/08/2016
, nvl(f.IMEI,'') IMEI_HANDSET_3MBE4
, nvl(f.DATA2G_COMPATIBLE,'') DAT2G_COMPATIBLE_HANDSET_3MBE4
, nvl(f.DATA3G_COMPATIBLE,'') DAT3G_COMPATIBLE_HANDSET_3MBE4
, nvl(f.DATA4G_COMPATIBLE,'') DAT4G_COMPATIBLE_HANDSET_3MBE4
,CURRENT_TIMESTAMP AS INSRT_DATE
,'###SLICE_VALUE###' ACCOUNT_ACTIVITY_EVENT_DATE
FROM
(SELECT EVENT_DATE ACCOUNT_ACTIVITY_EVENT_DATE
, MSISDN
,FORMULE
,GP_STATUS_DATE DISCONNEXION_DATE
,ACTIVATION_DATE
,TRUNC(MONTHS_BETWEEN(GP_STATUS_DATE,ACTIVATION_DATE)) AGE
,MAX(SUBSTR(OG_CALL,1,7)) LAST_OUTGOING_CALL_MONTH
,SUBSTR(GREATEST(IC_CALL_1,IC_CALL_2,IC_CALL_3,IC_CALL_4),1,7) LAST_INCOMING_CALL_MONTH
,SUM(REMAIN_CREDIT_MAIN) REMAIN_CREDIT_MAIN
,SUM(REMAIN_CREDIT_PROMO) REMAIN_CREDIT_PROMO
,PLATFORM_STATUS
FROM MON.SPARK_TT_GROUP_SUBS_ACCT_DISCONNECT
WHERE MSISDN NOT LIKE '692%'
AND EVENT_DATE =DATE_SUB(last_day('###SLICE_VALUE###'),-1)
GROUP BY EVENT_DATE
, MSISDN
,FORMULE
,GP_STATUS_DATE
,ACTIVATION_DATE
,TRUNC(MONTHS_BETWEEN(GP_STATUS_DATE,ACTIVATION_DATE))  
,SUBSTR(GREATEST(IC_CALL_1,IC_CALL_2,IC_CALL_3,IC_CALL_4),1,7) 
,PLATFORM_STATUS
) a
LEFT JOIN
(
select MSISDN
, SITE_NAME
, TOWNNAME
, ADMINISTRATIVE_REGION
, COMMERCIAL_REGION
from mon.SPARK_FT_CLIENT_LAST_SITE_LOCATION b
where b.EVENT_MONTH = SUBSTR('###SLICE_VALUE###',1,7)
) b ON a.MSISDN=b.MSISDN
LEFT JOIN
(SELECT REPLACE(REFILL_MONTH,'-','') REFILL_MONTH

,(CASE WHEN length(b.RECEIVER_MSISDN) = 8 THEN '6'||b.RECEIVER_MSISDN ELSE b.RECEIVER_MSISDN END) MSISDN
,RECEIVER_PROFILE
,sum(REFILL_AMOUNT) REFILL_AMOUNT
,sum(REFILL_COUNT) REFILL_COUNT
FROM AGG.SPARK_FT_A_REFILL_RECEIVER b
WHERE REFILL_MONTH BETWEEN SUBSTR(add_months('###SLICE_VALUE###',-5),1,7)  AND  SUBSTR('###SLICE_VALUE###',1,7)
AND REFILL_MEAN IN ('C2S','SCRATCH')
GROUP BY REPLACE(REFILL_MONTH,'-','') 
,(CASE WHEN length(b.RECEIVER_MSISDN) = 8 THEN '6'||b.RECEIVER_MSISDN ELSE b.RECEIVER_MSISDN END)
,RECEIVER_PROFILE
) c ON a.MSISDN=c.MSISDN
LEFT JOIN (
SELECT substr(e.imei,1,8) tac_code_handset, e.*, f.*
FROM 
(SELECT  a.*
, ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN
FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a
WHERE smonth = SUBSTR('###SLICE_VALUE###',1,7)
) e
left join (select  a.*
, (CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or EDGE = 'E' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE
, (CASE WHEN GPRS = 'O' or EDGE = 'O' or EDGE = 'E' THEN 'YES' ELSE 'NO' END) DATA2G_COMPATIBLE
, (CASE WHEN UMTS = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA3G_COMPATIBLE
, (CASE WHEN LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA4G_COMPATIBLE
from dim.dt_handset_ref a
) f
 ON substr(e.imei,1,8) = f.tac_code 
where e.IMEI_RN = 1
) e ON a.MSISDN=e.MSISDN
LEFT JOIN (
SELECT substr(e.imei,1,8) tac_code_handset, e.*, f.*
FROM 
(SELECT a.*
, ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN
FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a
WHERE smonth = SUBSTR(add_months('###SLICE_VALUE###',-3),1,7)
) e
left join (select  a.*
, (CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE 
, (CASE WHEN GPRS = 'O' or EDGE = 'O' or EDGE = 'E' THEN 'YES' ELSE 'NO' END) DATA2G_COMPATIBLE
, (CASE WHEN UMTS = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA3G_COMPATIBLE
, (CASE WHEN LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA4G_COMPATIBLE
from dim.dt_handset_ref a
) f
ON substr(e.imei,1,8) = f.tac_code 
WHERE e.IMEI_RN = 1
) f ON a.MSISDN=f.MSISDN