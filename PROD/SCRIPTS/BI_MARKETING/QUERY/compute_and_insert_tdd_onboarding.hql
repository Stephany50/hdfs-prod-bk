SELECT 
    access_key,
    B.principal_in AS principal_in,
    B.principal_ussd AS principal_ussd,
    profile,
    activation_date
FROM 
(
    SELECT 
    access_key,
    profile,
    activation_date
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE event_date = DATE_ADD('###SLICE_VALUE###', 1)
    AND profile LIKE '%INFINITY%'
    AND activation_date = '###SLICE_VALUE###'
) A 
LEFT JOIN 
(
    SELECT * FROM MON.SPARK_PERSONAL_TDD
) B 
ON GET_NNP_MSISDN_9DIGITS(A.access_key) = GET_NNP_MSISDN_9DIGITS(B.acc_nbr)