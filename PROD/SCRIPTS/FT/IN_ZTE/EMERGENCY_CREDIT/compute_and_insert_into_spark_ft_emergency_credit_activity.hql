
INSERT INTO MON.SPARK_FT_EMERGENCY_CREDIT_ACTIVITY
SELECT DISTINCT
     a.MSISDN
    , a.ACCOUNT_STATUS
    , a.ACCOUNT_PROFILE
    , a.ACTIVATION_DATE
    , a.PROVISION_DATE
    , a.RESILIATION_DATE
    , a.MAIN_REMAINING_CREDIT
    , a.PROMO_REMAINING_CREDIT
    , (b.TOTAL_REIMBOURSMENT_AMOUNT + a.DAILY_REIMBOURSMENT_AMOUNT) TOTAL_REIMBOURSMENT_AMOUNT
    , (b.TOTAL_REIMBOURSMENT_COUNT + a.DAILY_REIMBOURSMENT_COUNT) TOTAL_REIMBOURSMENT_COUNT
    , (b.TOTAL_LOAN_AMOUNT + a.DAILY_LOAN_AMOUNT) TOTAL_LOAN_AMOUNT
    , (b.TOTAL_LOAN_COUNT + a.DAILY_LOAN_COUNT) TOTAL_LOAN_COUNT
    , (b.TOTAL_PAYED_FEE_AMOUNT + a.DAILY_PAYED_FEE_AMOUNT) TOTAL_PAYED_FEE_AMOUNT
    , (b.TOTAL_PAYED_FEE_COUNT + a.DAILY_PAYED_FEE_COUNT) TOTAL_PAYED_FEE_COUNT
    , a.DAILY_REIMBOURSMENT_AMOUNT
    , a.DAILY_REIMBOURSMENT_COUNT
    , a.DAILY_LOAN_AMOUNT
    , a.DAILY_LOAN_COUNT
    , a.DAILY_PAYED_FEE_AMOUNT
    , a.DAILY_PAYED_FEE_COUNT
    , a.LOAN_AMOUNT
    , a.LOAN_TO_PAY
    , a.LOAN_DATE
    , a.LAST_PAYMENT_DATE
    , a.DUE_FEE_AMOUNT
    , b.ACTIVITY_BEGIN_DATE
    , 'ZTE' SOURCE_PLATFORM
    , 'IT_EC_EXTRACT' SOURCE_DATA
    , CURRENT_TIMESTAMP INSERT_DATE
    , CAST('###SLICE_VALUE###' AS DATE) EVENT_DATE
FROM  MON.SPARK_TT_EMERGENCY_CREDIT_ACTIVITY a
INNER JOIN (SELECT * FROM MON.SPARK_FT_EMERGENCY_CREDIT_ACTIVITY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) b
ON a.MSISDN=b.MSISDN AND a.ACTIVATION_DATE = b.ACTIVATION_DATE
UNION
SELECT a.*
FROM  MON.SPARK_TT_EMERGENCY_CREDIT_ACTIVITY a
LEFT JOIN (SELECT DISTINCT MSISDN, ACTIVATION_DATE FROM MON.SPARK_FT_EMERGENCY_CREDIT_ACTIVITY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1) )b ON  a.MSISDN=b.MSISDN AND a.ACTIVATION_DATE = b.ACTIVATION_DATE
WHERE b.MSISDN IS  NULL
