INSERT INTO AGG.SPARK_FT_A_CBM_AGREGAT
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
MOU_ONNET,
MOU_OFNET,
MOU_INTER,
MA_VOICE_ONNET,
MA_VOICE_OFNET,
MA_VOICE_INTER,
MA_SMS_OFNET,
MA_SMS_INTER,
MA_SMS_ONNET,
MA_DATA,
MA_VAS,
MA_GOS_SVA,
MA_VOICE_ROAMING,
MA_SMS_ROAMING,
MA_SMS_SVA,
MA_VOICE_SVA,
USER_VOICE,
BDLE_NAME,
BDLE_COST_RETAIL,
BDLE_COST_SUBS,
VOLUME_DATA,
USER_DATA1,
USER_DATA2 ,
MONTANT_VAS,
DESTINATION_TYPE,
DESTINATION,
TRAFFIC_DATA,
RATED_SMS_TOTAL_COUNT,
IN_DURATION,
RATED_DURATION,
LOC_SITE_NAME,
ACTIF_90,
INACTIF_90,
GROSS_ADD,
CURRENT_TIMESTAMP AS INSERT_DATE,
'###SLICE_VALUE###' as EVENT_DATE
FROM
(
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
SUM(NVL(MOU_ONNET,0)) AS MOU_ONNET ,
SUM(NVL(MOU_OFNET,0)) AS MOU_OFNET,
SUM(NVL(MOU_INTER,0)) AS MOU_INTER,
SUM(NVL(MA_VOICE_ONNET,0)) AS MA_VOICE_ONNET,
SUM(NVL(MA_VOICE_OFNET,0)) AS MA_VOICE_OFNET,
SUM(NVL(MA_VOICE_INTER,0)) AS MA_VOICE_INTER,
SUM(NVL(MA_SMS_OFNET,0)) AS MA_SMS_OFNET,
SUM(NVL(MA_SMS_INTER,0)) AS MA_SMS_INTER,
SUM(NVL(MA_SMS_ONNET,0)) AS MA_SMS_ONNET,
SUM(NVL(MA_DATA,0)) AS MA_DATA,
SUM(NVL(MA_VAS,0)) AS MA_VAS,
SUM(NVL(MA_GOS_SVA,0)) AS MA_GOS_SVA,
SUM(NVL(MA_VOICE_ROAMING,0)) AS MA_VOICE_ROAMING,
SUM(NVL(MA_SMS_ROAMING,0)) AS MA_SMS_ROAMING,
SUM(NVL(MA_SMS_SVA,0)) AS MA_SMS_SVA,
SUM(NVL(MA_VOICE_SVA,0)) AS MA_VOICE_SVA,
SUM(IS_USER_VOICE) AS USER_VOICE,
NULL AS BDLE_NAME,
NULL AS BDLE_COST_RETAIL,
NULL AS BDLE_COST_SUBS,
NULL AS VOLUME_DATA,
NULL AS USER_DATA1,
NULL AS  USER_DATA2 ,
NULL AS MONTANT_VAS,
NULL AS DESTINATION_TYPE,
NULL AS DESTINATION,
NULL AS TRAFFIC_DATA,
NULL AS RATED_SMS_TOTAL_COUNT,
NULL AS IN_DURATION,
NULL AS RATED_DURATION,
NULL AS LOC_SITE_NAME,
NULL AS ACTIF_90,
NULL AS INACTIF_90,
NULL AS GROSS_ADD
FROM
(SELECT *, CASE WHEN (MOU_ONNET+MOU_OFNET+MOU_INTER) > 0 THEN 1 ELSE 0 END AS IS_USER_VOICE FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY
WHERE PERIOD  ='###SLICE_VALUE###') A
LEFT JOIN
(SELECT EVENT_DATE, MSISDN, SITE_NAME FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE= '###SLICE_VALUE###') B ON A.MSISDN=B.MSISDN AND A.PERIOD=B.EVENT_DATE
LEFT JOIN
(SELECT MSISDN, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.DT_REF_CBM) C ON A.MSISDN=C.MSISDN
GROUP BY A.PERIOD, B.SITE_NAME, MULTI, OPERATEUR, TENURE, SEGMENT
UNION
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
NULL AS MOU_ONNET,
NULL AS MOU_OFNET,
NULL AS MOU_INTER,
NULL AS MA_VOICE_ONNET,
NULL AS MA_VOICE_OFNET,
NULL AS MA_VOICE_INTER,
NULL AS MA_SMS_OFNET,
NULL AS MA_SMS_INTER,
NULL AS MA_SMS_ONNET,
NULL AS MA_DATA,
NULL AS MA_VAS,
NULL AS MA_GOS_SVA,
NULL AS MA_VOICE_ROAMING,
NULL AS MA_SMS_ROAMING,
NULL AS MA_SMS_SVA,
NULL AS MA_VOICE_SVA,
NULL AS USER_VOICE,
BDLE_NAME,
NULL AS BDLE_COST_RETAIL,
SUM(NVL(BDLE_COST,0))   BDLE_COST_SUBS,
NULL AS VOLUME_DATA,
NULL AS USER_DATA1,
NULL AS USER_DATA2 ,
NULL AS MONTANT_VAS,
NULL AS DESTINATION_TYPE,
NULL AS DESTINATION,
NULL AS TRAFFIC_DATA,
NULL AS RATED_SMS_TOTAL_COUNT,
NULL AS IN_DURATION,
NULL AS RATED_DURATION,
NULL AS LOC_SITE_NAME,
NULL AS ACTIF_90,
NULL AS INACTIF_90,
NULL AS GROSS_ADD 
FROM
(SELECT * FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
WHERE PERIOD  ='###SLICE_VALUE###') A
LEFT JOIN
(SELECT EVENT_DATE, MSISDN, SITE_NAME FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE= '###SLICE_VALUE###') B ON A.MSISDN=B.MSISDN AND A.PERIOD=B.EVENT_DATE
LEFT JOIN
(SELECT MSISDN, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.DT_REF_CBM) C ON A.MSISDN=C.MSISDN
GROUP BY A.PERIOD, B.SITE_NAME, BDLE_NAME, MULTI, OPERATEUR, TENURE, SEGMENT
UNION
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
NULL AS MOU_ONNET,
NULL AS MOU_OFNET,
NULL AS MOU_INTER,
NULL AS MA_VOICE_ONNET,
NULL AS MA_VOICE_OFNET,
NULL AS MA_VOICE_INTER,
NULL AS MA_SMS_OFNET,
NULL AS MA_SMS_INTER,
NULL AS MA_SMS_ONNET,
NULL AS MA_DATA,
NULL AS MA_VAS,
NULL AS  MA_GOS_SVA,
NULL AS MA_VOICE_ROAMING,
NULL AS MA_SMS_ROAMING,
NULL AS MA_SMS_SVA,
NULL AS MA_VOICE_SVA,
NULL AS  USER_VOICE,
BDLE_NAME,
SUM(NVL(BDLE_COST,0)) BDLE_COST_RETAIL,
NULL AS BDLE_COST_SUBS,
NULL AS VOLUME_DATA,
NULL AS USER_DATA1,
NULL AS  USER_DATA2 ,
NULL AS MONTANT_VAS,
NULL AS DESTINATION_TYPE,
NULL AS DESTINATION,
NULL AS TRAFFIC_DATA,
NULL AS RATED_SMS_TOTAL_COUNT,
NULL AS IN_DURATION,
NULL AS RATED_DURATION,
NULL AS LOC_SITE_NAME,
NULL AS ACTIF_90,
NULL AS INACTIF_90,
NULL AS GROSS_ADD 
FROM
(SELECT SDATE AS PERIOD, SUB_MSISDN AS MSISDN, OFFER_NAME AS BDLE_NAME,
SUM(RECHARGE_AMOUNT) AS BDLE_COST
FROM MON.SPARK_FT_VAS_RETAILLER_IRIS
WHERE UPPER(OFFER_TYPE) NOT IN ('TOPUP') AND SDATE ='###SLICE_VALUE###' AND PRETUPS_STATUSCODE = '200'
GROUP BY SDATE, SUB_MSISDN, OFFER_NAME) A
LEFT JOIN
(SELECT EVENT_DATE, MSISDN, SITE_NAME FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='###SLICE_VALUE###') B ON A.MSISDN=B.MSISDN AND A.PERIOD=B.EVENT_DATE
LEFT JOIN
(SELECT MSISDN, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.DT_REF_CBM) C ON A.MSISDN=C.MSISDN
GROUP BY A.PERIOD, B.SITE_NAME, BDLE_NAME, MULTI, OPERATEUR, TENURE, SEGMENT
UNION
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
NULL AS MOU_ONNET,
NULL AS MOU_OFNET,
NULL AS MOU_INTER,
NULL AS MA_VOICE_ONNET,
NULL AS MA_VOICE_OFNET,
NULL AS MA_VOICE_INTER,
NULL AS MA_SMS_OFNET,
NULL AS MA_SMS_INTER,
NULL AS MA_SMS_ONNET,
NULL AS MA_DATA,
NULL AS MA_VAS,
NULL AS  MA_GOS_SVA,
NULL AS MA_VOICE_ROAMING,
NULL AS MA_SMS_ROAMING,
NULL AS MA_SMS_SVA,
NULL AS MA_VOICE_SVA,
NULL AS  USER_VOICE,
NULL AS BDLE_NAME,
NULL AS BDLE_COST_RETAIL,
NULL AS BDLE_COST_SUBS,
SUM(NBYTEST)/(1024*1024) AS VOLUME_DATA,
SUM(IS_USER_DATA1) AS USER_DATA1,
SUM(IS_USER_DATA2) AS USER_DATA2  ,
NULL AS MONTANT_VAS,
NULL AS DESTINATION_TYPE,
NULL AS DESTINATION,
NULL AS TRAFFIC_DATA,
NULL AS RATED_SMS_TOTAL_COUNT,
NULL AS IN_DURATION,
NULL AS RATED_DURATION,
NULL AS LOC_SITE_NAME,
NULL AS ACTIF_90,
NULL AS INACTIF_90,
NULL AS GROSS_ADD 
FROM
(SELECT TRANSACTION_DATE AS PERIOD,
MSISDN, NBYTEST, CASE WHEN NBYTEST/(1024*1024)>1  THEN 1 ELSE 0 END AS IS_USER_DATA1,
CASE WHEN NBYTEST/(1024*1024)>5  THEN 1 ELSE 0 END AS IS_USER_DATA2  FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
WHERE TRANSACTION_DATE ='###SLICE_VALUE###') A
LEFT JOIN
(SELECT EVENT_DATE, MSISDN, SITE_NAME FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###') B ON A.MSISDN=B.MSISDN AND A.PERIOD=B.EVENT_DATE
LEFT JOIN
(SELECT MSISDN, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.DT_REF_CBM) C ON A.MSISDN=C.MSISDN
GROUP BY A.PERIOD, B.SITE_NAME, MULTI, OPERATEUR, TENURE, SEGMENT
UNION
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
NULL AS MOU_ONNET,
NULL AS MOU_OFNET,
NULL AS MOU_INTER,
NULL AS MA_VOICE_ONNET,
NULL AS MA_VOICE_OFNET,
NULL AS MA_VOICE_INTER,
NULL AS MA_SMS_OFNET,
NULL AS MA_SMS_INTER,
NULL AS MA_SMS_ONNET,
NULL AS MA_DATA,
NULL AS MA_VAS,
NULL AS  MA_GOS_SVA,
NULL AS MA_VOICE_ROAMING,
NULL AS MA_SMS_ROAMING,
NULL AS MA_SMS_SVA,
NULL AS MA_VOICE_SVA,
NULL AS  USER_VOICE,
NULL AS BDLE_NAME,
NULL AS BDLE_COST_RETAIL,
NULL AS BDLE_COST_SUBS,
NULL AS VOLUME_DATA,
NULL AS USER_DATA1,
NULL AS USER_DATA2 ,
sum(case when MONTANT<0 then -1*MONTANT else MONTANT end ) as MONTANT_VAS,
NULL AS DESTINATION_TYPE,
NULL AS DESTINATION,
NULL AS TRAFFIC_DATA,
NULL AS RATED_SMS_TOTAL_COUNT,
NULL AS IN_DURATION,
NULL AS RATED_DURATION,
NULL AS LOC_SITE_NAME,
NULL AS ACTIF_90,
NULL AS INACTIF_90,
NULL AS GROSS_ADD 
FROM
(
SELECT
A.CREATE_DATE,
A.MSISDN,
A.SERVICE,
CASE WHEN C.ACCT_RES_RATING_UNIT = 'QM' THEN -A.CHARGE/100 ELSE -A.CHARGE END AS MONTANT,
C.ACCT_RES_NAME
FROM
(
SELECT
CREATE_DATE,
IF(SUBSTR(TRIM(ACC_NBR),1,3)=237,SUBSTR(TRIM(ACC_NBR),4,9),TRIM(ACC_NBR)) MSISDN,
CHANNEL_ID SERVICE,
ACCT_RES_CODE,
COUNT(1) NOMBRE_TRANSACTION,
SUM(NVL(CHARGE,0)) CHARGE
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT
WHERE CREATE_DATE ='###SLICE_VALUE###'
GROUP BY CREATE_DATE, IF(SUBSTR(TRIM(ACC_NBR),1,3)=237,SUBSTR(TRIM(ACC_NBR),4,9),TRIM(ACC_NBR)), CHANNEL_ID, ACCT_RES_CODE
) A
LEFT JOIN DIM.SPARK_DT_ZTE_USAGE_TYPE B ON A.SERVICE = B.USAGE_CODE
LEFT JOIN
(
SELECT DISTINCT
ACCT_RES_ID,
UPPER(ACCT_RES_NAME) ACCT_RES_NAME,
ACCT_RES_RATING_TYPE,
ACCT_RES_RATING_UNIT
FROM DIM.SPARK_DT_BALANCE_TYPE_ITEM
) C ON A.ACCT_RES_CODE = C.ACCT_RES_ID) FI
LEFT JOIN
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###') EN on FI.msisdn=EN.msisdn and FI.CREATE_DATE=EN.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.DT_REF_CBM) ES on FI.msisdn=ES.msisdn
WHERE SERVICE IN (9,13,28,29,33,37) and ACCT_RES_NAME in ("MAIN BALANCE")
group by FI.CREATE_DATE, EN.SITE_NAME, multi, operateur, tenure, segment
UNION
SELECT
NULL AS SITE_NAME,
NULL AS MULTI,
NULL AS OPERATEUR,
NULL AS TENURE,
NULL AS SEGMENT,
NULL AS MOU_ONNET,
NULL AS MOU_OFNET,
NULL AS MOU_INTER,
NULL AS MA_VOICE_ONNET,
NULL AS MA_VOICE_OFNET,
NULL AS MA_VOICE_INTER,
NULL AS MA_SMS_OFNET,
NULL AS MA_SMS_INTER,
NULL AS MA_SMS_ONNET,
NULL AS MA_DATA,
NULL AS MA_VAS,
NULL AS MA_GOS_SVA,
NULL AS MA_VOICE_ROAMING,
NULL AS MA_SMS_ROAMING,
NULL AS MA_SMS_SVA,
NULL AS MA_VOICE_SVA,
NULL AS USER_VOICE,
NULL AS BDLE_NAME,
NULL AS BDLE_COST_RETAIL,
NULL AS BDLE_COST_SUBS,
NULL AS VOLUME_DATA,
NULL AS USER_DATA1,
NULL AS USER_DATA2 ,
NULL AS MONTANT_VAS,
NULL AS  DESTINATION_TYPE,
NULL AS  DESTINATION,
NULL AS  TRAFFIC_DATA,
NULL AS  RATED_SMS_TOTAL_COUNT,
NULL AS  IN_DURATION,
NULL AS  RATED_DURATION,
LOC_SITE_NAME,
COUNT(DISTINCT CASE WHEN GRP_GP_STATUS='ACTIF' THEN MSISDN ELSE NULL END) AS ACTIF_90,
COUNT(DISTINCT CASE WHEN ACTIVATION_DATE=date_sub(EVENT_DATE,89) THEN MSISDN ELSE NULL END) AS INACTIF_90,
COUNT(DISTINCT CASE WHEN ACTIVATION_DATE=EVENT_DATE THEN MSISDN ELSE NULL END) AS GROSS_ADD
FROM MON.SPARK_FT_MARKETING_DATAMART
WHERE EVENT_DATE ='###SLICE_VALUE###'
GROUP BY EVENT_DATE,LOC_SITE_NAME
) T