MERGE INTO DIM.DT_SERVICES A
USING (
    SELECT IPP_NAME,IPP_CODE,MIN(IPP_AMOUNT) IPP_AMOUNT,MIN(CREATED_DATE)
    FROM CDR.IT_IPP_EXTRACT
    WHERE ORIGINAL_FILE_DATE = (SELECT MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT )
    GROUP BY IPP_NAME,IPP_CODE
) B ON (upper(A.EVENT) = upper(B.IPP_NAME))
WHEN MATCHED THEN UPDATE SET A.PRIX = B.IPP_AMOUNT
WHEN NOT MATCHED THEN INSERT(A.EVENT, A.PRIX, A.INSERT_DATE) VALUES(B.IPP_NAME,B.IPP_AMOUNT, CURRENT_TIMESTAMP);

MERGE INTO DIM.DT_SERVICES A
USING (
    SELECT IPP_NAME,IPP_CODE,MIN(IPP_AMOUNT) IPP_AMOUNT,MIN(CREATED_DATE)
    FROM CDR.IT_IPP_EXTRACT
    WHERE ORIGINAL_FILE_DATE = (SELECT MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT where original_file_date <= '2019-08-15')
    GROUP BY IPP_NAME,IPP_CODE
) B ON (upper(A.EVENT) = upper(B.IPP_NAME))
WHEN MATCHED THEN UPDATE SET A.PRIX = B.IPP_AMOUNT
WHEN NOT MATCHED THEN INSERT(A.EVENT, A.PRIX) VALUES(B.IPP_NAME,B.IPP_AMOUNT);

merge into dim.dt_services a using (
    select bdle_name, b.prix, nvl(coeff_onnet_voice, 0.000)/100 coeff_onnet_voice, nvl(coeff_offnet_voice, 0.000)/100 coeff_offnet_voice, nvl(coeff_inter_voice, 0.000)/100 coeff_inter_voice, nvl(coeff_data, 0.000)/100 coeff_data,
        nvl(coeff_sms, 0.000)/100 coeff_sms, nvl(coeff_sva, 0.000)/100 coeff_sva, nvl(coeff_roaming, 0.000)/100 coeff_roaming, nvl(coeff_roaming_voice, 0.000)/100 coeff_roaming_voice, nvl(coeff_roaming_data, 0.000)/100 coeff_roaming_data,
        nvl(coeff_roaming_sms, 0.000)/100 coeff_roaming_sms, b.validite, Type_forfait, destination, b.type_ocm, Offre
    from dim.dt_ref_souscription2 b
) b on (a.EVENT = b.bdle_name)
when matched then
    update set voix_onnet = coeff_onnet_voice, voix_offnet = coeff_offnet_voice, voix_inter = coeff_inter_voice, voix_roaming = coeff_roaming_voice, sms_onnet = coeff_sms, --sms_offnet = , sms_inter =  ,
        sms_roaming = coeff_roaming_sms, data_bundle = coeff_data+coeff_roaming_data, sva = coeff_sva
--WHEN NOT MATCHED THEN INSERT ( EVENT, event_source, voix_onnet, voix_offnet, voix_inter, voix_roaming, sms_onnet, sms_roaming, data_bundle, sva, validite, insert_date )
    --values ( b.bdle_name, 'SUBSCRIPTION', coeff_onnet_voice, coeff_offnet_voice, coeff_inter_voice, coeff_roaming_voice, coeff_sms, coeff_roaming_sms, coeff_data+coeff_roaming_data, coeff_sva, b.validite, current_timestamp )


SELECT IF(T_1.S_RESULT = 0 and T_2.S_RESULT= 1  ,"OK","NOK") S_RESULT_EXIST
FROM
​
​
(SELECT COUNT(*) S_RESULT FROM (
SELECT DISTINCT UPPER(SENDER_CATEGORY_CODE) CODE
FROM CDR.IT_OM_APGL  X1
LEFT JOIN DIM.DT_OM_PARTNER_SETUP X2 ON UPPER(SENDER_CATEGORY_CODE) = UPPER(SR_CATEGORY_CODE)
WHERE original_file_date = '2019-07-07'
      and X2.SR_CATEGORY_CODE IS NULL

        UNION ALL
​
        (SELECT DISTINCT UPPER(RECEIVER_CATEGORY_CODE) CODE FROM CDR.IT_OM_APGL WHERE original_file_date = '2019-07-07') X3
         LEFT JOIN
        (SELECT DISTINCT UPPER(SR_CATEGORY_CODE) CODE FROM DIM.DT_OM_PARTNER_SETUP) X4
        ON (X3.CODE = X4.CODE)
        where X4.CODE IS NULL
    )
) T_1,
(SELECT COUNT(*)  S_RESULT
 FROM (
    SELECT  A.*,
        B1.SR_USER_TYPE SENDER_USER_TYPE,
        B2.SR_USER_TYPE RECEIVER_USER_TYPE
    FROM (
          SELECT *
          FROM CDR.IT_OM_APGL
          WHERE original_file_date = '2019-07-07'
         ) A
    LEFT JOIN DIM.DT_OM_PARTNER_SETUP B1 ON (UPPER(A.SENDER_CATEGORY_CODE) = UPPER(B1.SR_CATEGORY_CODE))
    LEFT JOIN DIM.DT_OM_PARTNER_SETUP B2 ON (UPPER(A.RECEIVER_CATEGORY_CODE) = UPPER(B2.SR_CATEGORY_CODE))
) C
LEFT JOIN DIM.DT_OM_TRANS_SETUP B ON (
    C.TRANSACTION_TAG = B.TRANSACTION_TYPE
    AND C.SENDER_USER_TYPE = B.SENDER_USER_TYPE
    AND C.RECEIVER_USER_TYPE = B.RECEIVER_USER_TYPE
)
WHERE ACCOUNT_NO IS NULL
) T_2