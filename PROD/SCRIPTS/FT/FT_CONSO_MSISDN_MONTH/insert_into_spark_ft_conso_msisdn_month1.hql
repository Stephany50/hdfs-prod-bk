INSERT INTO MON.SPARK_FT_CONSO_MSISDN_MONTH
(MSISDN, FORMULE, CONSO, SMS_COUNT, TEL_COUNT, TEL_DURATION, BILLED_SMS_COUNT, BILLED_TEL_COUNT, BILLED_TEL_DURATION, CONSO_SMS, CONSO_TEL, CONSO_TEL_MAIN, PROMOTIONAL_CALL_COST, MAIN_CALL_COST,
OTHERS_VAS_TOTAL_COUNT, OTHERS_VAS_DURATION, OTHERS_VAS_MAIN_COST, OTHERS_VAS_PROMO_COST, NATIONAL_TOTAL_COUNT, NATIONAL_SMS_COUNT, NATIONAL_DURATION, NATIONAL_MAIN_COST, NATIONAL_PROMO_COST,
NATIONAL_SMS_MAIN_COST, NATIONAL_SMS_PROMO_COST, MTN_TOTAL_COUNT, MTN_SMS_COUNT, MTN_DURATION, MTN_TOTAL_CONSO, MTN_SMS_CONSO, CAMTEL_TOTAL_COUNT, CAMTEL_SMS_COUNT, CAMTEL_DURATION, CAMTEL_TOTAL_CONSO,
CAMTEL_SMS_CONSO, INTERNATIONAL_TOTAL_COUNT, INTERNATIONAL_SMS_COUNT, INTERNATIONAL_DURATION, INTERNATIONAL_TOTAL_CONSO, INTERNATIONAL_SMS_CONSO
-- Nouveaux champs
, ONNET_SMS_COUNT, ONNET_DURATION, ONNET_TOTAL_CONSO, ONNET_MAIN_CONSO, ONNET_MAIN_TEL_CONSO, ONNET_PROMO_TEL_CONSO, ONNET_SMS_CONSO, MTN_MAIN_CONSO, CAMTEL_MAIN_CONSO, SET_TOTAL_COUNT, SET_SMS_COUNT
, SET_DURATION, SET_TOTAL_CONSO, SET_SMS_CONSO, SET_MAIN_CONSO, INTERNATIONAL_MAIN_CONSO, ROAM_TOTAL_COUNT, ROAM_SMS_COUNT, ROAM_DURATION, ROAM_TOTAL_CONSO, ROAM_MAIN_CONSO, ROAM_SMS_CONSO, INROAM_TOTAL_COUNT
, INROAM_SMS_COUNT, INROAM_DURATION, INROAM_TOTAL_CONSO, INROAM_MAIN_CONSO, INROAM_SMS_CONSO, OPERATOR_CODE
, NEXTTEL_TOTAL_COUNT, NEXTTEL_SMS_COUNT, NEXTTEL_DURATION, NEXTTEL_TOTAL_CONSO, NEXTTEL_SMS_CONSO, NEXTTEL_MAIN_CONSO
, BUNDLE_SMS_COUNT, BUNDLE_TEL_DURATION
-- Nouveaux champs 20150526 par GPE: guy.penda@orange.com
, SET_MAIN_TEL_CONSO, MTN_MAIN_TEL_CONSO, NEXTTEL_MAIN_TEL_CONSO, CAMTEL_MAIN_TEL_CONSO, INTERNATIONAL_MAIN_TEL_CONSO, ROAM_MAIN_TEL_CONSO, INROAM_MAIN_TEL_CONSO
--
, SET_PROMO_TEL_CONSO, MTN_PROMO_TEL_CONSO, NEXTTEL_PROMO_TEL_CONSO, CAMTEL_PROMO_TEL_CONSO, INTERNATIONAL_PROMO_TEL_CONSO, ROAM_PROMO_TEL_CONSO, INROAM_PROMO_TEL_CONSO
--
, ACTIVE_DAYS_COUNT, FIRST_ACTIVE_DAY, LAST_ACTIVE_DAY
--Fin nouveau champs 20150526
-- Nouveaux champs 20151019 par dimitri.happi@orange.com
, ONNET_BILLED_TEL_DURATION,SET_BILLED_TEL_DURATION,MTN_BILLED_TEL_DURATION,NEXTTEL_BILLED_TEL_DURATION,
CAMTEL_BILLED_TEL_DURATION,INTERNATIONAL_BIL_TEL_DURATION,ROAM_BILLED_TEL_DURATION,INROAM_BILLED_TEL_DURATION,
ONNET_BILLED_TEL_COUNT,SET_BILLED_TEL_COUNT,MTN_BILLED_TEL_COUNT,NEXTTEL_BILLED_TEL_COUNT,CAMTEL_BILLED_TEL_COUNT,
INTERNATIONAL_BILLED_TEL_COUNT,ROAM_BILLED_TEL_COUNT,INROAM_BILLED_TEL_COUNT,ONNET_TEL_COUNT,SET_TEL_COUNT,
MTN_TEL_COUNT,NEXTTEL_TEL_COUNT,CAMTEL_TEL_COUNT,INTERNATIONAL_TEL_COUNT,ROAM_TEL_COUNT,INROAM_TEL_COUNT
-- Fin nouveaux champs 20151019
-- Nouveaux champs 20151117 par dimitri.happi@orange.com
,SVA_COUNT,SVA_DURATION,SVA_MAIN_CONSO,SVA_PROMO_CONSO,SVA_TEL_COUNT,SVA_BILLED_DURATION,SVA_BILLED_TEL_CONSO,SVA_SMS_COUNT,SVA_SMS_CONSO
, SRC_TABLE,INSERT_DATE,EVENT_MONTH)
SELECT
a.MSISDN  MSISDN
, MAX (b.FORMULE) FORMULE
, SUM (  NVL (a.PROMOTIONAL_CALL_COST,0) + NVL (a.MAIN_CALL_COST,0) ) CONSO
, SUM (NVL (a.SMS_COUNT,0) ) SMS_COUNT
, SUM ( NVL (a.TEL_COUNT,0) ) TEL_COUNT
, SUM ( NVL (a.TEL_DURATION,0)  ) TEL_DURATION
, SUM (NVL (a.BILLED_SMS_COUNT,0)  ) BILLED_SMS_COUNT
, SUM ( NVL (a.BILLED_TEL_COUNT,0) ) BILLED_TEL_COUNT
, SUM (NVL (a.BILLED_TEL_DURATION,0) ) BILLED_TEL_DURATION
, SUM (NVL (a.CONSO_SMS,0) ) CONSO_SMS
, SUM (NVL (a.CONSO_TEL,0) ) CONSO_TEL
, SUM (NVL (a.CONSO_TEL_MAIN,0) ) CONSO_TEL_MAIN
, SUM (NVL (a.PROMOTIONAL_CALL_COST,0)) PROMOTIONAL_CALL_COST
, SUM (NVL (a.MAIN_CALL_COST,0)) MAIN_CALL_COST
, SUM (NVL (a.OTHERS_VAS_TOTAL_COUNT,0)) OTHERS_VAS_TOTAL_COUNT
, SUM (NVL (a.OTHERS_VAS_DURATION,0)) OTHERS_VAS_DURATION
, SUM(NVL(OTHERS_VAS_MAIN_COST,0))OTHERS_VAS_MAIN_COST
, SUM(NVL(OTHERS_VAS_PROMO_COST,0))OTHERS_VAS_PROMO_COST
, SUM(NVL(NATIONAL_TOTAL_COUNT,0))NATIONAL_TOTAL_COUNT
, SUM(NVL(NATIONAL_SMS_COUNT,0))NATIONAL_SMS_COUNT
, SUM(NVL(NATIONAL_DURATION,0))NATIONAL_DURATION
, SUM(NVL(NATIONAL_MAIN_COST,0))NATIONAL_MAIN_COST
, SUM(NVL(NATIONAL_PROMO_COST,0))NATIONAL_PROMO_COST
, SUM(NVL(NATIONAL_SMS_MAIN_COST,0))NATIONAL_SMS_MAIN_COST
, SUM(NVL(NATIONAL_SMS_PROMO_COST,0))NATIONAL_SMS_PROMO_COST
, SUM(NVL(MTN_TOTAL_COUNT,0))MTN_TOTAL_COUNT
, SUM(NVL(MTN_SMS_COUNT,0))MTN_SMS_COUNT
, SUM(NVL(MTN_DURATION,0))MTN_DURATION
, SUM(NVL(MTN_TOTAL_CONSO,0))MTN_TOTAL_CONSO
, SUM(NVL(MTN_SMS_CONSO,0))MTN_SMS_CONSO
, SUM(NVL(CAMTEL_TOTAL_COUNT,0))CAMTEL_TOTAL_COUNT
, SUM(NVL(CAMTEL_SMS_COUNT,0))CAMTEL_SMS_COUNT
, SUM(NVL(CAMTEL_DURATION,0))CAMTEL_DURATION
, SUM(NVL(CAMTEL_TOTAL_CONSO,0))CAMTEL_TOTAL_CONSO
, SUM(NVL(CAMTEL_SMS_CONSO,0))CAMTEL_SMS_CONSO
, SUM(NVL(INTERNATIONAL_TOTAL_COUNT,0))INTERNATIONAL_TOTAL_COUNT
, SUM(NVL(INTERNATIONAL_SMS_COUNT,0))INTERNATIONAL_SMS_COUNT
, SUM(NVL(INTERNATIONAL_DURATION,0))INTERNATIONAL_DURATION
, SUM(NVL(INTERNATIONAL_TOTAL_CONSO,0))INTERNATIONAL_TOTAL_CONSO
, SUM(NVL(INTERNATIONAL_SMS_CONSO,0))INTERNATIONAL_SMS_CONSO
-- Nouveaux champs
, SUM(NVL(ONNET_SMS_COUNT,0)) ONNET_SMS_COUNT
, SUM(NVL(ONNET_DURATION,0)) ONNET_DURATION
, SUM(NVL(ONNET_TOTAL_CONSO,0)) ONNET_TOTAL_CONSO
, SUM(NVL(ONNET_MAIN_CONSO,0)) ONNET_MAIN_CONSO
, SUM(NVL(ONNET_MAIN_TEL_CONSO,0)) ONNET_MAIN_TEL_CONSO
, SUM(NVL(ONNET_PROMO_TEL_CONSO,0)) ONNET_PROMO_TEL_CONSO
, SUM(NVL(ONNET_SMS_CONSO,0)) ONNET_SMS_CONSO
, SUM(NVL(MTN_MAIN_CONSO,0)) MTN_MAIN_CONSO
, SUM(NVL(CAMTEL_MAIN_CONSO,0)) CAMTEL_MAIN_CONSO
, SUM(NVL(SET_TOTAL_COUNT,0)) SET_TOTAL_COUNT
, SUM(NVL(SET_SMS_COUNT,0)) SET_SMS_COUNT
, SUM(NVL(SET_DURATION,0)) SET_DURATION
, SUM(NVL(SET_TOTAL_CONSO,0)) SET_TOTAL_CONSO
, SUM(NVL(SET_SMS_CONSO,0)) SET_SMS_CONSO
, SUM(NVL(SET_MAIN_CONSO,0)) SET_MAIN_CONSO
, SUM(NVL(INTERNATIONAL_MAIN_CONSO,0)) INTERNATIONAL_MAIN_CONSO
, SUM(NVL(ROAM_TOTAL_COUNT,0)) ROAM_TOTAL_COUNT
, SUM(NVL(ROAM_SMS_COUNT,0)) ROAM_SMS_COUNT
, SUM(NVL(ROAM_DURATION,0)) ROAM_DURATION
, SUM(NVL(ROAM_TOTAL_CONSO,0)) ROAM_TOTAL_CONSO
, SUM(NVL(ROAM_MAIN_CONSO,0)) ROAM_MAIN_CONSO
, SUM(NVL(ROAM_SMS_CONSO,0)) ROAM_SMS_CONSO
, SUM(NVL(INROAM_TOTAL_COUNT,0)) INROAM_TOTAL_COUNT
, SUM(NVL(INROAM_SMS_COUNT,0)) INROAM_SMS_COUNT
, SUM(NVL(INROAM_DURATION,0)) INROAM_DURATION
, SUM(NVL(INROAM_TOTAL_CONSO,0)) INROAM_TOTAL_CONSO
, SUM(NVL(INROAM_MAIN_CONSO,0)) INROAM_MAIN_CONSO
, SUM(NVL(INROAM_SMS_CONSO,0)) INROAM_SMS_CONSO
, MAX(OPERATOR_CODE) OPERATOR_CODE
, SUM(NVL(NEXTTEL_TOTAL_COUNT,0)) NEXTTEL_TOTAL_COUNT
, SUM(NVL(NEXTTEL_SMS_COUNT,0)) NEXTTEL_SMS_COUNT
, SUM(NVL(NEXTTEL_DURATION,0)) NEXTTEL_DURATION
, SUM(NVL(NEXTTEL_TOTAL_CONSO,0)) NEXTTEL_TOTAL_CONSO
, SUM(NVL(NEXTTEL_SMS_CONSO,0)) NEXTTEL_SMS_CONSO
, SUM(NVL(NEXTTEL_MAIN_CONSO,0)) NEXTTEL_MAIN_CONSO
, SUM(NVL(BUNDLE_SMS_COUNT,0)) BUNDLE_SMS_COUNT
, SUM(NVL(BUNDLE_TEL_DURATION,0)) BUNDLE_TEL_DURATION
-- Nouveaux champs 20150526 par GPE: guy.penda@orange.com
, SUM(NVL(SET_MAIN_TEL_CONSO,0)) SET_MAIN_TEL_CONSO
, SUM(NVL(MTN_MAIN_TEL_CONSO,0)) MTN_MAIN_TEL_CONSO
, SUM(NVL(NEXTTEL_MAIN_TEL_CONSO,0)) NEXTTEL_MAIN_TEL_CONSO
, SUM(NVL(CAMTEL_MAIN_TEL_CONSO,0)) CAMTEL_MAIN_TEL_CONSO
, SUM(NVL(INTERNATIONAL_MAIN_TEL_CONSO,0)) INTERNATIONAL_MAIN_TEL_CONSO
, SUM(COALESCE(ROAM_MAIN_TEL_CONSO, 0)) AS ROAM_MAIN_TEL_CONSO
, SUM(COALESCE(INROAM_MAIN_TEL_CONSO, 0)) AS INROAM_MAIN_TEL_CONSO
--
, SUM(NVL(SET_PROMO_TEL_CONSO,0)) SET_PROMO_TEL_CONSO
, SUM(NVL(MTN_PROMO_TEL_CONSO,0)) MTN_PROMO_TEL_CONSO
, SUM(NVL(NEXTTEL_PROMO_TEL_CONSO,0)) NEXTTEL_PROMO_TEL_CONSO
, SUM(NVL(CAMTEL_PROMO_TEL_CONSO,0)) CAMTEL_PROMO_TEL_CONSO
, SUM(NVL(INTERNATIONAL_PROMO_TEL_CONSO,0)) INTERNATIONAL_PROMO_TEL_CONSO
, SUM(COALESCE(ROAM_PROMO_TEL_CONSO, 0)) AS ROAM_PROMO_TEL_CONSO
, SUM(COALESCE(INROAM_PROMO_TEL_CONSO, 0)) AS INROAM_PROMO_TEL_CONSO
--
, COUNT(DISTINCT a.EVENT_DATE) ACTIVE_DAYS_COUNT
, MIN(a.EVENT_DATE) FIRST_ACTIVE_DAY
, MAX(a.EVENT_DATE) LAST_ACTIVE_DAY
,
--Fin nouveau champs 20150526
--- Ajout Nouveaux champs 20151019 par dimitri.happi@orange.com
SUM(COALESCE(ONNET_BILLED_TEL_DURATION, 0)) AS ONNET_BILLED_TEL_DURATION,
SUM(COALESCE(SET_BILLED_TEL_DURATION, 0)) AS SET_BILLED_TEL_DURATION,
SUM(COALESCE(MTN_BILLED_TEL_DURATION, 0)) AS MTN_BILLED_TEL_DURATION,
SUM(COALESCE(NEXTTEL_BILLED_TEL_DURATION, 0)) AS NEXTTEL_BILLED_TEL_DURATION,
SUM(COALESCE(CAMTEL_BILLED_TEL_DURATION, 0)) AS CAMTEL_BILLED_TEL_DURATION,
SUM(COALESCE(INTERNATIONAL_BIL_TEL_DURATION, 0)) AS INTERNATIONAL_BIL_TEL_DURATION,
SUM(COALESCE(ROAM_BILLED_TEL_DURATION, 0)) AS ROAM_BILLED_TEL_DURATION,
SUM(COALESCE(INROAM_BILLED_TEL_DURATION, 0)) AS INROAM_BILLED_TEL_DURATION,
SUM(COALESCE(ONNET_BILLED_TEL_COUNT, 0)) AS ONNET_BILLED_TEL_COUNT,
SUM(COALESCE(SET_BILLED_TEL_COUNT, 0)) AS SET_BILLED_TEL_COUNT,
SUM(COALESCE(MTN_BILLED_TEL_COUNT, 0)) AS MTN_BILLED_TEL_COUNT,
SUM(COALESCE(NEXTTEL_BILLED_TEL_COUNT, 0)) AS NEXTTEL_BILLED_TEL_COUNT,
SUM(COALESCE(CAMTEL_BILLED_TEL_COUNT, 0)) AS CAMTEL_BILLED_TEL_COUNT,
SUM(COALESCE(INTERNATIONAL_BILLED_TEL_COUNT, 0)) AS INTERNATIONAL_BILLED_TEL_COUNT,
SUM(COALESCE(ROAM_BILLED_TEL_COUNT, 0)) AS ROAM_BILLED_TEL_COUNT,
SUM(COALESCE(INROAM_BILLED_TEL_COUNT, 0)) AS INROAM_BILLED_TEL_COUNT,
SUM(COALESCE(ONNET_TEL_COUNT, 0)) AS ONNET_TEL_COUNT,
SUM(COALESCE(SET_TEL_COUNT, 0)) AS SET_TEL_COUNT,
SUM(COALESCE(MTN_TEL_COUNT, 0)) AS MTN_TEL_COUNT,
SUM(COALESCE(NEXTTEL_TEL_COUNT, 0)) AS NEXTTEL_TEL_COUNT,
SUM(COALESCE(CAMTEL_TEL_COUNT, 0)) AS CAMTEL_TEL_COUNT,
SUM(COALESCE(INTERNATIONAL_TEL_COUNT, 0)) AS INTERNATIONAL_TEL_COUNT,
SUM(COALESCE(ROAM_TEL_COUNT, 0)) AS ROAM_TEL_COUNT,
SUM(COALESCE(INROAM_TEL_COUNT, 0)) AS INROAM_TEL_COUNT,
--- Ajout Nouveaux champs 20151117 par dimitri.happi@orange.com
SUM(COALESCE(SVA_COUNT, 0)) AS SVA_COUNT,
SUM(COALESCE(SVA_DURATION, 0)) AS SVA_DURATION,
SUM(COALESCE(SVA_MAIN_CONSO, 0)) AS SVA_MAIN_CONSO,
SUM(COALESCE(SVA_PROMO_CONSO, 0)) AS SVA_PROMO_CONSO,
SUM(COALESCE(SVA_TEL_COUNT, 0)) AS SVA_TEL_COUNT,
SUM(COALESCE(SVA_BILLED_DURATION, 0)) AS SVA_BILLED_DURATION,
SUM(COALESCE(SVA_BILLED_TEL_CONSO, 0)) AS SVA_BILLED_TEL_CONSO,
SUM(COALESCE(SVA_SMS_COUNT, 0)) AS SVA_SMS_COUNT,
SUM(COALESCE(SVA_SMS_CONSO, 0)) AS SVA_SMS_CONSO
, 'IT_BSCS_BILLED_TRANS_POSTPAID' SRC_TABLE,
CURRENT_TIMESTAMP INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM
MON.SPARK_FT_CONSO_MSISDN_DAY  a
LEFT JOIN (
SELECT
a.ACCESS_KEY MSISDN -- mon.fn_get_nnp_msisdn (a.ACCESS_KEY) MSISDN
,  UPPER (    NVL  (a.OSP_CUSTOMER_FORMULE,  SUBSTR(a.BSCS_COMM_OFFER, INSTR(a.BSCS_COMM_OFFER,'|')+1)) ) FORMULE
FROM
MON.SPARK_FT_CONTRACT_SNAPSHOT a
WHERE a.EVENT_DATE = ADD_MONTHS (TO_DATE ('###SLICE_VALUE###' || '-01'), 1)
-- elimination du risque posé par le statut "TERMINATED"
AND (CASE NVL(a.OSP_STATUS, a.CURRENT_STATUS)
WHEN 'ACTIVE' THEN 'ACTIVE'
WHEN 'a' THEN 'ACTIVE'
WHEN 'd' THEN 'DEACT'
WHEN 's' THEN 'INACTIVE'
WHEN 'DEACTIVATED' THEN 'DEACT'
WHEN 'INACTIVE' THEN 'INACTIVE'
WHEN 'VALID' THEN 'VALID'
ELSE 'NVL(a.OSP_STATUS, a.CURRENT_STATUS)'
END) <> 'TERMINATED'
-- GROUP BY   a.ACCESS_KEY
) b
ON a.MSISDN = b.MSISDN
INNER JOIN(
SELECT  DISTINCT MSISDN
FROM MON.SPARK_FT_CONSO_MSISDN_DAY
WHERE EVENT_DATE BETWEEN TO_DATE ('###SLICE_VALUE###' || '-01') AND DATE_SUB(ADD_MONTHS (TO_DATE ('###SLICE_VALUE###' || '-01'), 1), 1)
MINUS
SELECT  DISTINCT MSISDN
FROM MON.SPARK_FT_CONSO_MSISDN_DAY
WHERE EVENT_DATE BETWEEN TO_DATE ('###SLICE_VALUE###' || '-01') AND DATE_SUB(ADD_MONTHS (TO_DATE ('###SLICE_VALUE###' || '-01'), 1), 1)
AND SRC_TABLE in ('IT_CRA_ICC_TRADUIT','IT_ZTE_VOICE_SMS_DATA')
) c
ON a.MSISDN = c.MSISDN
WHERE
EVENT_DATE BETWEEN TO_DATE ('###SLICE_VALUE###' || '-01') AND DATE_SUB(ADD_MONTHS (TO_DATE ('###SLICE_VALUE###' || '-01'), 1), 1)
AND a.SRC_TABLE = 'IT_BSCS_BILLED_TRANS_POSTPAID'
-- and rownum < 1
GROUP BY
a.MSISDN
