INSERT INTO AGG.SPARK_REVENUE_SUMMARY_DAILY
SELECT
SUM(NBR_GSM_VOX)  in_nbr_gsm_vox,
SUM(GSM_VOX_VOL)  in_gsm_vox_vol,
SUM(GSM_VOX_TAX_AMT)  in_gsm_vox_tax_amt,
SUM(NBR_GSM_SMS)  in_nbr_gsm_sms,
SUM(GSM_SMS_TAX_AMT)  in_gsm_sms_tax_amt,
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
null  tango_nbr_om_data,
null  tango_om_data_tax_amt,
null  p2p_nbr_trans_fees,
null  p2p_trans_fees_tax_amt,
null  p2p_nbr_credit_trans,
null  p2p_credit_trans_tax_amt,
'FT_GSM_TRAFFIC_REVENUE_DAILY'  source_data,
CURRENT_TIMESTAMP  INSERT_DATE,
EVENT_DATE
FROM (
         SELECT
             EVENT_DATE
              , SUM(IF(SERVICE_CODE = 'VOI_VOX', RATED_COUNT, 0)) NBR_GSM_VOX
              , SUM(IF(SERVICE_CODE = 'VOI_VOX',RATED_VOLUME, 0)) GSM_VOX_VOL
              , SUM(IF(SERVICE_CODE = 'VOI_VOX',TAXED_AMOUNT, 0)) GSM_VOX_TAX_AMT
              , SUM(IF(SERVICE_CODE = 'NVX_SMS', RATED_COUNT, 0)) NBR_GSM_SMS
              , SUM(IF(SERVICE_CODE = 'NVX_SMS',TAXED_AMOUNT, 0)) GSM_SMS_TAX_AMT
         FROM
             (SELECT
                  TRANSACTION_DATE EVENT_DATE
                   ,OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
                   ,(case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end) SERVICE_CODE
                   , SUM (TOTAL_COUNT) RATED_COUNT
                   , SUM ( CASE SERVICE_CODE
                               WHEN 'VOI_VOX' THEN DURATION
                               WHEN 'NVX_SMS' THEN TOTAL_COUNT
                               WHEN 'NVX_USS' THEN TOTAL_COUNT
                               ELSE TOTAL_COUNT END) RATED_VOLUME
                   , SUM (MAIN_RATED_AMOUNT) TAXED_AMOUNT
                   , OPERATOR_CODE OPERATOR_CODE
              FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
              WHERE TRANSACTION_DATE = '###SLICE_VALUE###'  AND NVL(MAIN_RATED_AMOUNT,0)>0
              GROUP BY
                  TRANSACTION_DATE
                     ,OFFER_PROFILE_CODE
                     ,(case when SERVICE_CODE='NVX_USS' then 'NVX_SMS' else SERVICE_CODE end)
                     ,OPERATOR_CODE
             )A
                 LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
         WHERE A.OPERATOR_CODE = 'OCM'
         GROUP BY
             EVENT_DATE
                ,SERVICE_CODE
     ) T
GROUP BY
    EVENT_DATE
