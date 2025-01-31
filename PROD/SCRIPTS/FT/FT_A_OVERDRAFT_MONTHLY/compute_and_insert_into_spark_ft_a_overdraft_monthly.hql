INSERT INTO AGG.SPARk_FT_A_OVERDRAFT_MONTHLY
SELECT
    COUNT(DISTINCT MSISDN) MSISDN_COUNT,
    PROFILE,
    SUM(NVL(REIMBURSMENT_COUNT, 0)) REIMBURSMENT_COUNT,
    SUM(NVL(REIMBURSMENT_AMOUNT, 0)) REIMBURSMENT_AMOUNT,
    SUM(NVL(LOAN_COUNT, 0)) LOAN_COUNT,
    SUM(NVL(LOAN_AMOUNT, 0))  LOAN_AMOUNT,
    SUM(NVL(FEE_AMOUNT, 0)) FEE_AMOUNT,
    OPERATOR_CODE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    EVENT_MONTH
FROM MON.SPARK_FT_OVERDRAFT_MONTHLY
WHERE EVENT_MONTH = "###SLICE_VALUE###"
GROUP BY
    EVENT_MONTH,
    PROFILE,
    OPERATOR_CODE