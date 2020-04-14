INSERT INTO AGG.SPARK_REVENUE_SUMMARY_DAILY
SELECT
null  in_nbr_gsm_vox,
null  in_gsm_vox_vol,
null  in_gsm_vox_tax_amt,
null  in_nbr_gsm_sms,
null  in_gsm_sms_tax_amt,
null  in_nbr_gsm_vox_post,
null  in_gsm_vox_post_vol,
null  in_gsm_vox_post_tax_amt,
null  in_nbr_gsm_sms_post,
null  in_gsm_sms_post_tax_amt,
null  in_nbr_subsc_bun_sms,
null  in_subsc_bun_sms_tax_amt,
null  in_nbr_subsc_mod_fnf,
null  in_subsc_mod_fnf_tax_amt,
null  in_nbr_subsc_chg_brand,
null  in_subsc_chg_brand_tax_amt,
null  in_nbr_subsc_uss,
null  in_subsc_uss_tax_amt,
null  in_nbr_subsc_bun_vox,
null  in_subsc_bun_vox_tax_amt,
null  in_nbr_sos_credit,
null  in_sos_credit_tax_amt,
null  in_nbr_sos_data,
null  in_sos_data_tax_amt,
SUM(NBR_ADJ_RBT) IN_NBR_ADJ_RBT,
SUM(ADJ_RBT_TAX_AMT) IN_ADJ_RBT_TAX_AMT,
SUM(NBR_ADJ_USS) IN_NBR_ADJ_USS,
SUM(ADJ_USS_TAX_AMT) in_ADJ_USS_TAX_AMT,
SUM(NBR_ADJ_VOI_SMS) in_NBR_ADJ_VOI_SMS,
SUM(ADJ_VOI_SMS_TAX_AMT) in_ADJ_VOI_SMS_TAX_AMT,
SUM(NBR_ADJ_VEXT) in_NBR_ADJ_VEXT,
SUM(ADJ_VEXT_TAX_AMT) in_ADJ_VEXT_TAX_AMT,
SUM(NBR_ADJ_PAR) in_NBR_ADJ_PAR,
SUM(ADJ_PAR_TAX_AMT) in_ADJ_PAR_TAX_AMT,
SUM(NBR_ADJ_FBO) in_NBR_ADJ_FBO,
SUM(ADJ_FBO_TAX_AMT) in_ADJ_FBO_TAX_AMT,
SUM(NBR_ADJ_CEL) in_NBR_ADJ_CEL,
SUM(ADJ_CEL_TAX_AMT) in_ADJ_CEL_TAX_AMT,
SUM(NBR_ADJ_SIG) in_NBR_ADJ_SIG,
SUM(ADJ_SIG_TAX_AMT) in_ADJ_SIG_TAX_AMT,
null  in_nbr_deac_acct_bal,
null  in_deac_acct_bal_tax_amt,
null  in_nbr_gprs_sva,
null  in_gprs_sva_vol,
null  in_gprs_sva_tax_amt,
null  in_nbr_gprs_paygo,
null  in_gprs_paygo_vol,
null  in_gprs_paygo_tax_amt,
null  in_nbr_gprs_sva_post,
null  in_gprs_sva_post_vol,
null  in_gprs_sva_post_tax_amt,
null  in_nbr_gprs_paygo_post,
null  in_gprs_paygo_post_vol,
null  in_gprs_paygo_post_tax_amt,
null  in_nbr_refill_topup,
null  in_refill_topup_tax_amt,
null  in_nbr_data_trans,
null  in_data_trans_tax_amt,
null  in_nbr_vas_data,
null  in_vas_data_tax_amt,
null  zebra_nbr_c2s,
null  zebra_c2s_tax_amt,
null  tango_nbr_om_data,
null  tango_om_data_tax_amt,
null  p2p_nbr_trans_fees,
null  p2p_trans_fees_tax_amt,
null  p2p_nbr_credit_trans,
null  p2p_credit_trans_tax_amt,
'ADJUSTMENT'  source_data,
CURRENT_TIMESTAMP  INSERT_DATE,
event_date
FROM
    (
        SELECT
            EVENT_DATE
             , SUM(IF(SERVICE_CODE = 'NVX_RBT', RATED_COUNT, 0)) NBR_ADJ_RBT
             , SUM(IF(SERVICE_CODE = 'NVX_RBT', TAXED_AMOUNT, 0)) ADJ_RBT_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_USS', RATED_COUNT, 0)) NBR_ADJ_USS
             , SUM(IF(SERVICE_CODE = 'NVX_USS', TAXED_AMOUNT, 0)) ADJ_USS_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'VOI_SMS', RATED_COUNT, 0)) NBR_ADJ_VOI_SMS
             , SUM(IF(SERVICE_CODE = 'VOI_SMS', TAXED_AMOUNT, 0)) ADJ_VOI_SMS_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_VEXT', RATED_COUNT, 0)) NBR_ADJ_VEXT
             , SUM(IF(SERVICE_CODE = 'NVX_VEXT', TAXED_AMOUNT, 0)) ADJ_VEXT_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_PAR', RATED_COUNT, 0)) NBR_ADJ_PAR
             , SUM(IF(SERVICE_CODE = 'NVX_PAR', TAXED_AMOUNT, 0)) ADJ_PAR_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_FBO', RATED_COUNT, 0)) NBR_ADJ_FBO
             , SUM(IF(SERVICE_CODE = 'NVX_FBO', TAXED_AMOUNT, 0)) ADJ_FBO_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_CEL', RATED_COUNT, 0)) NBR_ADJ_CEL
             , SUM(IF(SERVICE_CODE = 'NVX_CEL', TAXED_AMOUNT, 0)) ADJ_CEL_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_SIG', RATED_COUNT, 0)) NBR_ADJ_SIG
             , SUM(IF(SERVICE_CODE = 'NVX_SIG', TAXED_AMOUNT, 0)) ADJ_SIG_TAX_AMT
        FROM
            (SELECT
                 A.CREATE_DATE EVENT_DATE
                  , C.PROFILE COMMERCIAL_OFFER_CODE
                  , B.GLOBAL_USAGE_CODE SERVICE_CODE
                  , SUM (IF(CHARGE > 0, 1, 0)) RATED_COUNT
                  , SUM (IF(CHARGE > 0, CAST(CHARGE AS DOUBLE)/100, 0)) TAXED_AMOUNT
                  , C.OPERATOR_CODE OPERATOR_CODE
             FROM CDR.SPARK_IT_ZTE_ADJUSTMENT A
                      LEFT JOIN (SELECT USAGE_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
                      LEFT JOIN (SELECT A.ACCESS_KEY ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE)  OPERATOR_CODE
                                 FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
                                          LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                                                     WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                                                     GROUP BY ACCESS_KEY) B
                                                    ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
                                 WHERE B.ACCESS_KEY IS NOT NULL
                                 GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE ) C
                                ON C.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
             WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37')
             GROUP BY
                 CREATE_DATE
                    , C.PROFILE
                    , B.GLOBAL_USAGE_CODE
                    , OPERATOR_CODE
            )A
                LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
        WHERE  A.OPERATOR_CODE = 'OCM'
        GROUP BY
            EVENT_DATE
               ,SERVICE_CODE
    ) T
GROUP BY
    EVENT_DATE