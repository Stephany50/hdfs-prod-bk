CREATE TABLE tmp.msisdn_vue_360_4 AS
SELECT
    a.*,
    SOS_REIMBURSEMENT_AMOUNT,
    SOS_REIMBURSEMENT_COUNT,
    SOS_LOAN_AMOUNT,
    SOS_LOAN_COUNT,
    SOS_FEES
FROM tmp.msisdn_vue_360_3 a
LEFT JOIN
(
    SELECT
        SERVED_PARTY_MSISDN AS MSISDN,
        SUM(CASE WHEN OPERATION_TYPE = 'REIMBURSMENT' THEN REFILL_AMOUNT ELSE 0 END) AS SOS_REIMBURSEMENT_AMOUNT,
        SUM(CASE WHEN OPERATION_TYPE = 'REIMBURSMENT' THEN 1 ELSE 0 END) AS SOS_REIMBURSEMENT_COUNT,
        SUM(CASE WHEN OPERATION_TYPE = 'LOAN' THEN LOAN_AMOUNT ELSE 0 END) AS SOS_LOAN_AMOUNT,
        SUM(CASE WHEN OPERATION_TYPE = 'LOAN' THEN 1 ELSE 0 END) AS SOS_LOAN_COUNT,
        SUM(FEE) AS SOS_FEES
    FROM MON.FT_OVERDRAFT
    WHERE TRANSACTION_DATE = '2019-10-04'
    GROUP BY SERVED_PARTY_MSISDN
)b on a.MSISDN=b.MSISDN