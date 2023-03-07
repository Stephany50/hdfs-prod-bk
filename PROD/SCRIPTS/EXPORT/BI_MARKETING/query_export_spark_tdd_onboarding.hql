SELECT DISTINCT
    imei,
    A.msisdn AS box_msisdn,
    B.principal_in AS principal_in,
    B.principal_ussd AS principal_ussd,
    B.stat_date AS date_inscription,
    date_app_imei,
    profile_infinity
FROM
(
    SELECT DISTINCT
        imei,
        msisdn,
        date_app_imei,
        profile_infinity
    FROM MON.SPARK_FT_TDD_PARC
    WHERE event_date = '###SLICE_VALUE###'
) A 
LEFT JOIN 
(
    SELECT *
    FROM MON.SPARK_PERSONAL_TDD
) B
ON GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(B.acc_nbr)