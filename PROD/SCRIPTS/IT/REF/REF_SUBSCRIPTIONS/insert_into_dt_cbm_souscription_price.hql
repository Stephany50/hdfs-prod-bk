INSERT INTO DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
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
    combo
FROM CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS
WHERE ORIGINAL_FILE_DATE IN (
    select original_file_date from CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS where insert_date = (select max(insert_date) from CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS) limit 1
) 

