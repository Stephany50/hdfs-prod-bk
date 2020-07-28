INSERT INTO MON.SPARK_FT_OVERDRAFT_MONTHLY
SELECT
    SERVICE_CODE,
    SERVED_PARTY_MSISDN MSISDN,
    MAX(b.FORMULE) PROFILE,
    REIMBURSMENT_CHANNEL,
    SUM(CASE WHEN OPERATION_TYPE = 'REIMBURSMENT' THEN 1 ELSE 0 END ) REIMBURSMENT_COUNT,
    SUM(CASE WHEN OPERATION_TYPE = 'REIMBURSMENT' THEN NVL(LOAN_AMOUNT,0) ELSE 0 END ) REIMBURSMENT_AMOUNT,
    SUM(CASE WHEN OPERATION_TYPE = 'LOAN' THEN 1 ELSE 0 END ) LOAN_COUNT,
    SUM(CASE WHEN OPERATION_TYPE = 'LOAN' THEN NVL(LOAN_AMOUNT,0) ELSE 0 END ) LOAN_AMOUNT,
    SUM(NVL(FEE,0)) FEE_AMOUNT,
    OPERATOR_CODE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    DATE_FORMAT(TRANSACTION_DATE, 'yyyy-MM') EVENT_MONTH
 FROM MON.SPARK_FT_OVERDRAFT a
 LEFT JOIN
 (
 SELECT
     a.ACCESS_KEY MSISDN,
     UPPER ( NVL  (a.PROFILE,  SUBSTR(a.BSCS_COMM_OFFER, INSTR(a.BSCS_COMM_OFFER,'|')+1)) )  FORMULE
 FROM
 MON.SPARK_FT_CONTRACT_SNAPSHOT a
 WHERE a.EVENT_DATE = ADD_MONTHS (TO_DATE(CONCAT("###SLICE_VALUE###", '-01')), 1)
 AND
 CASE NVL(a.OSP_STATUS, a.CURRENT_STATUS)
 WHEN 'ACTIVE' THEN 'ACTIVE'
 WHEN 'a' THEN 'ACTIVE'
 WHEN 'd' THEN 'DEACT'
 WHEN 's' THEN 'INACTIVE'
 WHEN 'DEACTIVATED' THEN 'DEACT'
 WHEN 'INACTIVE' THEN 'INACTIVE'
 WHEN 'VALID' THEN 'VALID'
 ELSE NVL(a.OSP_STATUS, a.CURRENT_STATUS)
 END <> 'TERMINATED'
) b
ON a.SERVED_PARTY_MSISDN = b.MSISDN
WHERE TRANSACTION_DATE BETWEEN TO_DATE(CONCAT("###SLICE_VALUE###", '-01')) AND LAST_DAY(TO_DATE(CONCAT("###SLICE_VALUE###", '-01')))
GROUP BY
    SERVICE_CODE,
    SERVED_PARTY_MSISDN,
    REIMBURSMENT_CHANNEL,
    OPERATOR_CODE,
    DATE_FORMAT(TRANSACTION_DATE, 'yyyy-MM')