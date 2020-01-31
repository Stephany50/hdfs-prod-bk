INSERT INTO AGG.SPARK_REVENUE_SUMMARY_DAILY
SELECT
null  in_nbr_gsm_vox,
null  in_gsm_vox_vol,
null  in_gsm_vox_tax_amt,
null in_nbr_gsm_sms,
null  in_gsm_sms_tax_amt,
null in_nbr_gsm_vox_post,
null  in_gsm_vox_post_vol,
null  in_gsm_vox_post_tax_amt,
null  in_nbr_gsm_sms_post,
null  in_gsm_sms_post_tax_amt,
SUM(NBR_SUBSC_BUN_SMS)  in_nbr_subsc_bun_sms,
SUM(SUBSC_BUN_SMS_TAX_AMT)  in_subsc_bun_sms_tax_amt,
SUM(NBR_SUBSC_MOD_FNF)  in_nbr_subsc_mod_fnf,
SUM(SUBSC_MOD_FNF_TAX_AMT)  in_subsc_mod_fnf_tax_amt,
SUM(NBR_SUBSC_CHG_BRAND)  in_nbr_subsc_chg_brand,
SUM(NBR_SUBSC_USS)  in_subsc_chg_brand_tax_amt,
SUM(SUBSC_USS_TAX_AMT)  in_nbr_subsc_uss,
SUM(NBR_SUBSC_BUN_VOX)  in_subsc_uss_tax_amt,
SUM(SUBSC_BUN_VOX_TAX_AMT)  in_nbr_subsc_bun_vox,
null  in_subsc_bun_vox_tax_amt,
null  in_nbr_sos_credit,
null  in_sos_credit_tax_amt,
null  in_nbr_sos_data,
null  in_sos_data_tax_amt,
null IN_NBR_ADJ_RBT,
null IN_ADJ_RBT_TAX_AMT,
null IN_NBR_ADJ_USS,
null in_ADJ_USS_TAX_AMT,
null in_NBR_ADJ_VOI_SMS,
null in_ADJ_VOI_SMS_TAX_AMT,
null in_NBR_ADJ_VEXT,
null in_ADJ_VEXT_TAX_AMT,
null in_NBR_ADJ_PAR,
null in_ADJ_PAR_TAX_AMT,
null in_NBR_ADJ_FBO,
null in_ADJ_FBO_TAX_AMT,
null in_NBR_ADJ_CEL,
null in_ADJ_CEL_TAX_AMT,
null in_NBR_ADJ_SIG,
null in_ADJ_SIG_TAX_AMT,
null in_NBR_DEAC_ACCT_BAL,
null in_DEAC_ACCT_BAL_TAX_AMT,
null  in_nbr_gprs_sva,
null  in_gprs_sva_vol,
null  in_gprs_sva_tax_amt,
null  in_nbr_gprs_paygo,
null  in_gprs_paygo_vol,
null  in_gprs_paygo_tax_amt,
null  in_nbr_gprs_sva_post,
null in_gprs_sva_post_vol,
null  in_gprs_sva_post_tax_amt,
null  in_nbr_gprs_paygo_post,
null  in_gprs_paygo_post_vol,
null in_gprs_paygo_post_tax_amt,
null  in_nbr_refill_topup,
null in_refill_topup_tax_amt,
null  zebra_nbr_c2s,
null  zebra_c2s_tax_amt,
null in_nbr_data_trans,
null in_data_trans_tax_amt,
null in_nbr_vas_data,
null in_vas_data_tax_amt,
null  tango_nbr_om_data,
null tango_om_data_tax_amt,
null  p2p_nbr_trans_fees,
null  p2p_trans_fees_tax_amt,
null  p2p_nbr_credit_trans,
null  p2p_credit_trans_tax_amt,
'FT_A_SUBSCRIPTION'  source_data,
CURRENT_TIMESTAMP  INSERT_DATE,
EVENT_DATE
FROM(
        SELECT
            EVENT_DATE
             , SUM(IF(SERVICE_CODE = 'BUN_SMS', RATED_COUNT, 0)) NBR_SUBSC_BUN_SMS
             , SUM(IF(SERVICE_CODE = 'BUN_SMS',TAXED_AMOUNT, 0)) SUBSC_BUN_SMS_TAX_AMT
             , SUM(IF(SERVICE_CODE = '122', RATED_COUNT, 0)) NBR_SUBSC_MOD_FNF
             , SUM(IF(SERVICE_CODE = '122',TAXED_AMOUNT, 0)) SUBSC_MOD_FNF_TAX_AMT
             , SUM(IF(SERVICE_CODE = '35', RATED_COUNT, 0)) NBR_SUBSC_CHG_BRAND
             , SUM(IF(SERVICE_CODE = '35',TAXED_AMOUNT, 0)) SUBSC_CHG_BRAND_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_USS', RATED_COUNT, 0)) NBR_SUBSC_USS
             , SUM(IF(SERVICE_CODE = 'NVX_USS',TAXED_AMOUNT, 0)) SUBSC_USS_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'BUN_VOX', RATED_COUNT, 0)) NBR_SUBSC_BUN_VOX
             , SUM(IF(SERVICE_CODE = 'BUN_VOX',TAXED_AMOUNT, 0)) SUBSC_BUN_VOX_TAX_AMT
        FROM
            (SELECT
                 TRANSACTION_DATE EVENT_DATE
                  ,COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
                  ,(CASE
                        WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
                        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
                        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
                        WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
                        WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
                        ELSE 'BUN_VOX' /* New individual price plan*/
                    END) SERVICE_CODE
                  , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
                  , SUM (SUBS_AMOUNT) TAXED_AMOUNT
                  , OPERATOR_CODE OPERATOR_CODE
             FROM AGG.SPARK_FT_A_SUBSCRIPTION
             WHERE TRANSACTION_DATE = '###SLICE_VALUE###'  AND NVL(SUBS_AMOUNT,0) > 0 AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'
             GROUP BY
                 TRANSACTION_DATE
                    ,COMMERCIAL_OFFER
                    ,(CASE
                          WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
                          WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
                          WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
                          WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
                          WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
                          ELSE 'BUN_VOX' /* New individual price plan*/
                 END)
                    ,OPERATOR_CODE
            )A
                LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
        WHERE  A.OPERATOR_CODE = 'OCM'
        GROUP BY
            EVENT_DATE
               ,SERVICE_CODE
    ) T
GROUP BY
    EVENT_DATE

