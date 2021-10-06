INSERT INTO CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS PARTITION (ORIGINAL_FILE_DATE)
SELECT
    bdle_name,
    prix,
    coeff_onnet,
    coeff_offnet,
    coeff_inter,
    coeff_data,
    coef_sms,
    coef_sva,
    coeff_roaming,
    coeff_roaming_voix,
    coeff_roaming_data,
    coeff_roaming_sms,
    validite,
    type_forfait,
    destination,
    type_ocm,
    offre,
    offer,
    offer_1,
    offer_2,
    combo,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    split(split(ORIGINAL_FILE_NAME, '_')[size(split(ORIGINAL_FILE_NAME, '_'))-1], '.csv')[0] ORIGINAL_FILE_DATE
FROM CDR.tt_ref_subscriptions C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS WHERE INSERT_DATE BETWEEN DATE_SUB(CURRENT_DATE,100) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL


