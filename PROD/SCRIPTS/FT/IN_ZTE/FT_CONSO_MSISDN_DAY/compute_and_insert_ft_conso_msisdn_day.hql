-- INSERT INTO mon.FT_CONSO_MSISDN_DAY
--     (MSISDN, FORMULE, CONSO, SMS_COUNT, TEL_COUNT, TEL_DURATION, BILLED_SMS_COUNT, BILLED_TEL_COUNT, BILLED_TEL_DURATION, CONSO_SMS, CONSO_TEL, PROMOTIONAL_CALL_COST, MAIN_CALL_COST,SRC_TABLE,others_vas_total_count, others_vas_duration, others_vas_main_cost, others_vas_promo_cost, national_total_count, national_sms_count, national_duration, national_main_cost, national_promo_cost, national_sms_main_cost, national_sms_promo_cost, mtn_total_count, mtn_sms_count, mtn_duration,mtn_total_conso, mtn_sms_conso, camtel_total_count, camtel_sms_count, camtel_duration, camtel_total_conso, camtel_sms_conso,international_total_count, international_sms_count, international_duration, international_total_conso, international_sms_conso, INSERT_DATE, EVENT_DATE)

SELECT
     a.SERVED_PARTY  MSISDN,
   MAX (a.COMMERCIAL_PROFILE) FORMULE,
   SUM ((PROMOTIONAL_CALL_COST + MAIN_CALL_COST)) CONSO,
   SUM (CASE    WHEN  a.SERVICE_CODE = 'NVX_SMS' THEN  EVENT_COUNT   ELSE  0    END ) SMS_COUNT,
   SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' THEN  EVENT_COUNT   ELSE  0    END ) TEL_COUNT,
   SUM (DURATION ) TEL_DURATION,
   SUM (CASE    WHEN  a.SERVICE_CODE = 'NVX_SMS' AND (PROMOTIONAL_CALL_COST + MAIN_CALL_COST) > 0 THEN  EVENT_COUNT   ELSE  0    END ) BILLED_SMS_COUNT,
   SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' AND (PROMOTIONAL_CALL_COST + MAIN_CALL_COST) > 0 THEN   EVENT_COUNT   ELSE  0    END ) BILLED_TEL_COUNT,
   SUM (CASE    WHEN (PROMOTIONAL_CALL_COST + MAIN_CALL_COST) > 0 THEN  a.DURATION ELSE  0     END ) BILLED_TEL_DURATION,
   SUM (CASE    WHEN  a.SERVICE_CODE = 'NVX_SMS' THEN  (PROMOTIONAL_CALL_COST + MAIN_CALL_COST)   ELSE  0    END ) CONSO_SMS,
   SUM (CASE    WHEN  a.SERVICE_CODE = 'VOI_VOX' THEN  (PROMOTIONAL_CALL_COST + MAIN_CALL_COST)   ELSE  0    END ) CONSO_TEL,
   SUM (PROMOTIONAL_CALL_COST) PROMOTIONAL_CALL_COST,
   'IT_CRA_ICC_TRADUIT' SRC_TABLE,
   SUM (MAIN_CALL_COST) MAIN_CALL_COST,
   SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN EVENT_COUNT ELSE 0 END ) others_vas_total_count,
   SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN DURATION ELSE 0 END ) others_vas_duration,
   SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN MAIN_CALL_COST ELSE 0 END ) others_vas_main_cost,
   SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN PROMOTIONAL_CALL_COST ELSE 0 END ) others_vas_promo_cost,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') THEN EVENT_COUNT ELSE 0 END ) national_total_count,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) national_sms_count,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') THEN DURATION ELSE 0 END ) national_duration,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') THEN MAIN_CALL_COST ELSE 0 END ) national_main_cost,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') THEN PROMOTIONAL_CALL_COST ELSE 0 END ) national_promo_cost,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') AND SERVICE_CODE = 'NVX_SMS' THEN MAIN_CALL_COST ELSE 0 END ) national_sms_main_cost,
   SUM (CASE WHEN DEST_OPERATOR NOT IN ('BELG','FTLD','BICS') AND SERVICE_CODE = 'NVX_SMS' THEN PROMOTIONAL_CALL_COST ELSE 0 END ) national_sms_promo_cost,
   SUM (CASE WHEN DEST_OPERATOR IN ('MTN') THEN EVENT_COUNT ELSE 0 END ) mtn_total_count,
   SUM (CASE WHEN DEST_OPERATOR IN ('MTN') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) mtn_sms_count,
   SUM (CASE WHEN DEST_OPERATOR IN ('MTN') THEN DURATION ELSE 0 END ) mtn_duration,
   SUM (CASE WHEN DEST_OPERATOR IN ('MTN') THEN (MAIN_CALL_COST + PROMOTIONAL_CALL_COST) ELSE 0 END ) mtn_total_conso,
   SUM (CASE WHEN DEST_OPERATOR IN ('MTN') AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_CALL_COST + PROMOTIONAL_CALL_COST) ELSE 0 END ) mtn_sms_conso,
   SUM (CASE WHEN DEST_OPERATOR IN ('CAMTEL_FIX', 'CTPHONE') THEN EVENT_COUNT ELSE 0 END ) camtel_total_count,
   SUM (CASE WHEN DEST_OPERATOR IN ('CAMTEL_FIX', 'CTPHONE') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) camtel_sms_count,
   SUM (CASE WHEN DEST_OPERATOR IN ('CAMTEL_FIX', 'CTPHONE') THEN DURATION ELSE 0 END ) camtel_duration,
   SUM (CASE WHEN DEST_OPERATOR IN ('CAMTEL_FIX', 'CTPHONE') THEN (MAIN_CALL_COST + PROMOTIONAL_CALL_COST) ELSE 0 END ) camtel_total_conso,
   SUM (CASE WHEN DEST_OPERATOR IN ('CAMTEL_FIX', 'CTPHONE') AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_CALL_COST + PROMOTIONAL_CALL_COST) ELSE 0 END) camtel_sms_conso,
   SUM (CASE WHEN DEST_OPERATOR IN ('BELG','FTLD','BICS') THEN EVENT_COUNT ELSE 0 END ) international_total_count,
   SUM (CASE WHEN DEST_OPERATOR IN ('BELG','FTLD','BICS') AND SERVICE_CODE = 'NVX_SMS' THEN EVENT_COUNT ELSE 0 END ) international_sms_count,
   SUM (CASE WHEN DEST_OPERATOR IN ('BELG','FTLD','BICS') THEN DURATION ELSE 0 END ) international_duration,
   SUM (CASE WHEN DEST_OPERATOR IN ('BELG','FTLD','BICS') THEN (MAIN_CALL_COST + PROMOTIONAL_CALL_COST) ELSE 0 END ) international_total_conso,
   SUM (CASE WHEN DEST_OPERATOR IN ('BELG','FTLD','BICS') AND SERVICE_CODE = 'NVX_SMS' THEN (MAIN_CALL_COST + PROMOTIONAL_CALL_COST) ELSE 0 END ) international_sms_conso,
   current_timestamp INSERT_DATE,
   '2019-09-12' EVENT_DATE
  FROM
         (
           SELECT
                  SERVED_PARTY,
                  fn_interco_destination (OTHER_PARTY) DEST_OPERATOR,
                  (CASE
                         WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                         WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                         WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                         WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                         WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                         WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                         WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                         WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                         WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                         WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                         WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                         WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                         ELSE 'AUT'
                     END ) SERVICE_CODE,
                    MAX (COMMERCIAL_PROFILE) COMMERCIAL_PROFILE,
                    SUM (NVL ( (CASE WHEN teleservice_indicator IS NULL AND UPPER(NETWORK_EVENT_TYPE) NOT IN ('TOPUP','REFILL VIA MENU','DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') THEN RAW_EVENT_COST
                         WHEN teleservice_indicator NOT IN ('4','7','8') THEN RAW_EVENT_COST
                         WHEN teleservice_indicator IN ('4','8','7') AND UPPER(NETWORK_EVENT_TYPE) NOT IN ('TOPUP','REFILL VIA MENU','DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') THEN RAW_EVENT_COST
                         ELSE 0 END) , 0 )) MAIN_CALL_COST,
                    SUM (NVL ( (CASE WHEN teleservice_indicator IS NULL AND UPPER(NETWORK_EVENT_TYPE) NOT IN ('TOPUP','REFILL VIA MENU','DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') AND BUNDLE_UNIT = 'UM' AND BUNDLE_IDENTIFIER IN ('PROMO','PROMO_ONNET','PROMO_XPRESS') THEN RATED_AMOUNT_IN_BUNDLE
                         WHEN teleservice_indicator NOT IN ('4','7','8') AND BUNDLE_UNIT = 'UM' AND BUNDLE_IDENTIFIER IN ('PROMO','PROMO_ONNET','PROMO_XPRESS') THEN RATED_AMOUNT_IN_BUNDLE
                         WHEN teleservice_indicator IN ('4','8','7') AND UPPER(NETWORK_EVENT_TYPE) NOT IN ('TOPUP','REFILL VIA MENU','DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') AND BUNDLE_UNIT = 'UM' AND BUNDLE_IDENTIFIER IN ('PROMO','PROMO_ONNET','PROMO_XPRESS') THEN RATED_AMOUNT_IN_BUNDLE
                         ELSE 0 END) , 0 ))  PROMOTIONAL_CALL_COST,
                    SUM (NVL ( (CASE WHEN Unit_of_Measurement = 'QT' THEN Rated_Volume
                         WHEN Unit_of_Measurement = 'QM' AND LOWER(RAW_TARIFF_PLAN) LIKE '%izo%' THEN Rated_Volume*100
                         WHEN Unit_of_Measurement = 'QM' AND LOWER(RAW_TARIFF_PLAN) LIKE '%illimite%' THEN Rated_Volume*100
                         ELSE 0 END) , 0 )) DURATION,
                    SUM (1) EVENT_COUNT
					    FROM
                            mon.FT_BILLED_TRANSACTION_PREPAID
                        WHERE
                            TRANSACTION_DATE = '2019-09-01'
                           -- AND  NVL (Rated_Volume, 0) > 0 OR  NVL (RATED_AMOUNT_IN_BUNDLE, 0) > 0 AND (NVL (RAW_EVENT_COST, 0) > 0 OR BILLING_TERM_INDICATOR = 0)

                        GROUP BY
						SERVED_PARTY,
                             (CASE
                                      WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                                      WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                                      WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                                      WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                                      WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                                      WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                                      WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                                      WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                                      WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                                      WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                                      WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                                      WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                                      ELSE 'AUT'
                                      END ),
						fn_interco_destination (OTHER_PARTY)
                    ) a
                    GROUP BY
                        a.SERVED_PARTY
                    ;