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
null  in_refill_topup_tax_amt,
null in_nbr_data_trans,
null in_data_trans_tax_amt,
null  in_nbr_vas_data,
null  in_vas_data_tax_amt,
null  zebra_nbr_c2s,
null  zebra_c2s_tax_amt,

SUM(RATED_COUNT)  tango_nbr_om_data,
SUM(TAXED_AMOUNT)  tango_om_data_tax_amt,

null  p2p_nbr_trans_fees,
null  p2p_trans_fees_tax_amt,
null  p2p_nbr_credit_trans,
null  p2p_credit_trans_tax_amt,
'OM_TRANSACTIONS'  source_data,
CURRENT_TIMESTAMP  INSERT_DATE,
EVENT_DATE
FROM
    (SELECT
         TO_DATE(TRANSFER_DATETIME) EVENT_DATE
          , B.PROFILE COMMERCIAL_OFFER_CODE
          , 'NVX_OM_DATA' SERVICE_CODE
          , SUM (1) RATED_COUNT
          , SUM (TRANSACTION_AMOUNT) TAXED_AMOUNT
          , B.OPERATOR_CODE OPERATOR_CODE
     FROM CDR.SPARK_IT_OMNY_TRANSACTIONS A
              LEFT JOIN (SELECT ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
                         FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
                                  LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                                             WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                                             GROUP BY ACCESS_KEY) B
                                            ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
                         WHERE B.ACCESS_KEY IS NOT NULL
                         GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE) B
                        ON A.SENDER_MSISDN = B.ACCESS_KEY
     WHERE  TRANSACTION_DATE = '###SLICE_VALUE###'
       AND  TO_DATE(TRANSFER_DATETIME) = '###SLICE_VALUE###' AND TRANSFER_DONE = 'Y' AND RECEIVER_MSISDN IN ('698066666', '658101010', '658121212')
     GROUP BY
         TO_DATE(TRANSFER_DATETIME)
            , B.PROFILE
            , B.OPERATOR_CODE
    )A
        LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE  A.OPERATOR_CODE = 'OCM'
GROUP BY
    EVENT_DATE
       ,SERVICE_CODE

