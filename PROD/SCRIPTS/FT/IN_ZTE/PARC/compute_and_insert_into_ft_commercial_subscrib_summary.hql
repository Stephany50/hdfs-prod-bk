
-- CALCULER LES DONNÉES 'PURE PREPAID' et 'HYBRID'
INSERT INTO  mon.FT_COMMERCIAL_SUBSCRIB_SUMMARY
      (DATECODE, NETWORK_DOMAIN, NETWORK_TECHNOLOGY, SUBSCRIBER_CATEGORY, CUSTOMER_ID, SUBSCRIBER_TYPE, COMMERCIAL_OFFER, ACCOUNT_STATUS, LOCK_STATUS, ACTIVATION_MONTH, CITYZONE, USAGE_TYPE, TOTAL_COUNT, TOTAL_ACTIVATION, TOTAL_DEACTIVATION, TOTAL_EXPIRATION, TOTAL_PROVISIONNED, TOTAL_MAIN_CREDIT, TOTAL_PROMO_CREDIT, TOTAL_SMS_CREDIT, TOTAL_DATA_CREDIT, SOURCE, REFRESH_DATE, PROFILE_NAME
      , platform_account_status, platform_activation_month )
      (
      SELECT
          datecode, network_domain, network_technology
          , UPPER (d.CRM_SEGMENTATION) subscriber_category
          -- , NULL CUSTOMER_ID
          , CUSTOMER_ID
          -- , UPPER (d.CONTRACT_TYPE)  subscriber_type
          , UPPER (a.OSP_CONTRACT_TYPE)  subscriber_type
          , commercial_offer, account_status , lock_status, activation_month
          , cityzone
          , usage_type
          , SUM ( NVL (total_count, 0) ) total_count , SUM ( NVL (total_activation, 0) ) total_activation
          , SUM ( NVL (total_deactivation, 0) ) total_deactivation , SUM ( NVL (total_expiration, 0) ) total_expiration
          , SUM ( NVL (total_provisionned, 0) ) total_provisionned , SUM ( NVL (total_main_credit, 0) ) total_main_credit
          , SUM ( NVL (total_promo_credit, 0) ) total_promo_credit , SUM ( NVL (total_sms_credit, 0) ) total_sms_credit
          , SUM ( NVL (total_data_credit, 0) ) total_data_credit
          , source
          , SYSDATE  REFRESH_DATE
          , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
          , platform_account_status, platform_activation_month
      FROM
          (
              SELECT d_slice_value_moins_1 datecode
                  , NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE
                   , 'GSM'  network_domain
                   , 'Telephony' network_technology
                   , a.OSP_CUSTOMER_CGLIST customer_id
                   , a.PROFILE commercial_offer
                   , b.COMGP_STATUS account_status
                   , DECODE (NVL (BLOCKED, '0'), '1', 'BLOCKED', 'NOT BLOCKED') lock_status -- blocked
                   , TO_CHAR (b.COMGP_FIRST_ACTIVE_DATE,'yyyymm') activation_month
                   , NULL  cityzone
                   , 'Telephony' usage_type
                   , SUM (1) total_count
                   , SUM(CASE WHEN acquisition_date= d_slice_value THEN 1 ELSE 0 END) total_activation
                   , SUM(CASE WHEN desacquisition_date= d_slice_value THEN 1 ELSE 0 END) total_deactivation
                   , 0 total_expiration
                   , SUM (CASE WHEN PROVISIONING_DATE = d_slice_value THEN 1 ELSE 0 END) total_provisionned
                   -- :: 20101208_152506 :: @change: 20101208_151938 by achille.tchapi : eliminer l'effet de bord dû à la presence des credits en compte negatif.
                   , SUM((CASE WHEN MAIN_CREDIT IS NULL THEN 0 WHEN MAIN_CREDIT < 0 THEN 0 ELSE MAIN_CREDIT END )) total_main_credit
                   , SUM((CASE WHEN PROMO_CREDIT IS NULL THEN 0 WHEN PROMO_CREDIT < 0 THEN 0 ELSE PROMO_CREDIT END )) total_promo_credit
                   , 0 total_sms_credit
                   , 0 total_data_credit
                   , a.SRC_TABLE source
                   , DECODE (a.OSP_STATUS, 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
                      , 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
                      , a.OSP_STATUS) platform_account_status
                   , (CASE WHEN a.ACTIVATION_DATE IS NULL THEN NULL
                        WHEN a.ACTIVATION_DATE <  TO_DATE('19880101','yyyymmdd') THEN NULL
                        WHEN a.activation_date < ADD_MONTHS (  TO_DATE (SUBSTR (s_slice_value, 1, 6) || '01' , 'yyyymmdd')   , -5) THEN  TO_CHAR (ACTIVATION_DATE, 'yyyy')
                        WHEN a.activation_date = d_slice_value THEN  s_slice_value
                        ELSE TO_CHAR (ACTIVATION_DATE, 'yyyymm') END ) platform_activation_month
              FROM mon.FT_CONTRACT_SNAPSHOT a
                          , (SELECT  MSISDN, COMGP_STATUS_DATE, COMGP_STATUS, COMGP_FIRST_ACTIVE_DATE
                                      , (CASE
                                              WHEN (NVL (COMGP_STATUS, 'ND') = 'ACTIF'
                                                  AND NVL (COMGP_STATUS_DATE, SYSDATE + 1000) = d_slice_value ) THEN d_slice_value
                                              ELSE NULL  END) acquisition_date
                                      , (CASE
                                              WHEN (NVL (COMGP_STATUS, 'ND') = 'INACT'
                                                  AND NVL (COMGP_STATUS_DATE, SYSDATE + 1000) = d_slice_value ) THEN d_slice_value
                                              ELSE NULL  END) desacquisition_date
                                      FROM MON.FT_ACCOUNT_ACTIVITY  WHERE -- ROWNUM < 1 AND
                                              EVENT_DATE = d_account_activity_date  ) b
              WHERE  -- ROWNUM < 1 AND
                  a.EVENT_DATE= d_contract_snapshot_date -- ( d_slice_value + 1)
                  AND ( a.ACTIVATION_DATE <= d_slice_value )
                  -- Filtrer sur les 'PURE PREPAID' :: 20110126_175705
                  --AND a.SRC_TABLE = 'IT_ICC_ACCOUNT'
                  AND NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) IN ('PURE PREPAID', 'HYBRID')
                  AND a.ACCESS_KEY = b.MSISDN (+)
              GROUP BY
                     a.PROFILE
                   , b.COMGP_STATUS
                   , DECODE (NVL (BLOCKED, '0'), '1', 'BLOCKED', 'NOT BLOCKED')
                   , TO_CHAR (COMGP_FIRST_ACTIVE_DATE,'yyyymm')
                   , a.location , a.OSP_CUSTOMER_CGLIST, NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' )
                   , a.SRC_TABLE
                   , DECODE (a.OSP_STATUS, 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
                      , 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
                      , a.OSP_STATUS)
                   , (CASE WHEN a.ACTIVATION_DATE IS NULL THEN NULL
                        WHEN a.ACTIVATION_DATE <  TO_DATE('19880101','yyyymmdd') THEN NULL
                        WHEN a.activation_date < ADD_MONTHS (  TO_DATE (SUBSTR (s_slice_value, 1, 6) || '01' , 'yyyymmdd')   , -5) THEN  TO_CHAR (ACTIVATION_DATE, 'yyyy')
                        WHEN a.activation_date = d_slice_value THEN  s_slice_value
                        ELSE TO_CHAR (ACTIVATION_DATE, 'yyyymm') END )
          ) a
          , mon.VW_DT_OFFER_PROFILES d
          WHERE
              UPPER (a.commercial_offer) = d.PROFILE_CODE (+)
          GROUP BY
              datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (a.OSP_CONTRACT_TYPE)
              , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source
              , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) , CUSTOMER_ID
              , platform_account_status, platform_activation_month
  );
  -- ne pas faire de commit ici

-- AJOUT DES MSISDN QUI DISPARAISSENT DE LA PHOTO
          INSERT INTO TT_TMP_MISSING_MSISDN
          SELECT MSISDN FROM MON.FT_ACCOUNT_ACTIVITY  WHERE EVENT_DATE = d_account_activity_date
          AND NVL (COMGP_STATUS, 'INACT')='ACTIF'
          MINUS
          SELECT access_key MSISDN FROM mon.FT_CONTRACT_SNAPSHOT a WHERE a.EVENT_DATE = d_contract_snapshot_date    ;

          COMMIT;

-- CALCULER LES DONNÉES 'PURE PREPAID' et 'HYBRID'
INSERT INTO  mon.FT_COMMERCIAL_SUBSCRIB_SUMMARY
      (DATECODE, NETWORK_DOMAIN, NETWORK_TECHNOLOGY, SUBSCRIBER_CATEGORY, CUSTOMER_ID, SUBSCRIBER_TYPE, COMMERCIAL_OFFER, ACCOUNT_STATUS, LOCK_STATUS, ACTIVATION_MONTH, CITYZONE, USAGE_TYPE, TOTAL_COUNT, TOTAL_ACTIVATION, TOTAL_DEACTIVATION, TOTAL_EXPIRATION, TOTAL_PROVISIONNED, TOTAL_MAIN_CREDIT, TOTAL_PROMO_CREDIT, TOTAL_SMS_CREDIT, TOTAL_DATA_CREDIT, SOURCE, REFRESH_DATE, PROFILE_NAME
      , platform_account_status, platform_activation_month )
      (
      SELECT
          datecode, network_domain, network_technology
          , UPPER (d.CRM_SEGMENTATION) subscriber_category
          -- , NULL CUSTOMER_ID
          , CUSTOMER_ID
          -- , UPPER (d.CONTRACT_TYPE)  subscriber_type
          , UPPER (d.CONTRACT_TYPE)  subscriber_type
          , commercial_offer, account_status , lock_status, activation_month
          , cityzone
          , usage_type
          , SUM ( NVL (total_count, 0) ) total_count , SUM ( NVL (total_activation, 0) ) total_activation
          , SUM ( NVL (total_deactivation, 0) ) total_deactivation , SUM ( NVL (total_expiration, 0) ) total_expiration
          , SUM ( NVL (total_provisionned, 0) ) total_provisionned , SUM ( NVL (total_main_credit, 0) ) total_main_credit
          , SUM ( NVL (total_promo_credit, 0) ) total_promo_credit , SUM ( NVL (total_sms_credit, 0) ) total_sms_credit
          , SUM ( NVL (total_data_credit, 0) ) total_data_credit
          , source
          , SYSDATE  REFRESH_DATE
          , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
          , platform_account_status, platform_activation_month
      FROM
          (
              SELECT d_slice_value_moins_1 datecode
                  , NVL (c.CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE
                   , 'GSM'  network_domain
                   , 'Telephony' network_technology
                   , '' customer_id --, a.OSP_CUSTOMER_CGLIST customer_id
                   , b.PROFILE commercial_offer
                   , b.COMGP_STATUS account_status
                   , DECODE (NVL (BLOCKED, '0'), '1', 'BLOCKED', 'NOT BLOCKED') lock_status -- blocked
                   , TO_CHAR (b.COMGP_FIRST_ACTIVE_DATE,'yyyymm') activation_month
                   , NULL  cityzone
                   , 'Telephony' usage_type
                   , SUM (1) total_count
                   , SUM(CASE WHEN acquisition_date= d_slice_value THEN 1 ELSE 0 END) total_activation
                   , SUM(CASE WHEN desacquisition_date= d_slice_value THEN 1 ELSE 0 END) total_deactivation
                   , 0 total_expiration
                   , SUM (CASE WHEN PROVISION_DATE = d_slice_value THEN 1 ELSE 0 END) total_provisionned
                   -- :: 20101208_152506 :: @change: 20101208_151938 by achille.tchapi : eliminer l'effet de bord dû à la presence des credits en compte negatif.
                   , SUM((CASE WHEN REMAIN_CREDIT_MAIN IS NULL THEN 0 WHEN REMAIN_CREDIT_MAIN < 0 THEN 0 ELSE REMAIN_CREDIT_MAIN END )) total_main_credit
                   , SUM((CASE WHEN REMAIN_CREDIT_PROMO IS NULL THEN 0 WHEN REMAIN_CREDIT_PROMO < 0 THEN 0 ELSE REMAIN_CREDIT_PROMO END )) total_promo_credit
                   , 0 total_sms_credit
                   , 0 total_data_credit
                   , b.SRC_TABLE source
                   , DECODE (b.OSP_STATUS, 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
                      , 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
                      , b.OSP_STATUS) platform_account_status
                   , (CASE WHEN b.ACTIVATION_DATE IS NULL THEN NULL
                        WHEN b.ACTIVATION_DATE <  TO_DATE('19880101','yyyymmdd') THEN NULL
                        WHEN b.ACTIVATION_DATE < ADD_MONTHS (  TO_DATE (SUBSTR (s_slice_value, 1, 6) || '01' , 'yyyymmdd')   , -5) THEN  TO_CHAR (ACTIVATION_DATE, 'yyyy')
                        WHEN b.ACTIVATION_DATE = d_slice_value THEN  s_slice_value
                        ELSE TO_CHAR (ACTIVATION_DATE, 'yyyymm') END ) platform_activation_month
              FROM (select access_key, '' OSP_CUSTOMER_CGLIST, '' location from mon.TT_TMP_MISSING_MSISDN
                          where rownum < nb_to_inject -- INTECTION PROGRESSIVE
                          ) a
                          , (SELECT MSISDN, COMGP_STATUS_DATE, COMGP_STATUS, COMGP_FIRST_ACTIVE_DATE , FORMULE PROFILE, SRC_TABLE, PLATFORM_STATUS OSP_STATUS, '' BLOCKED,ACTIVATION_DATE,REMAIN_CREDIT_MAIN, REMAIN_CREDIT_PROMO, PROVISION_DATE
                                      , (CASE
                                              WHEN (NVL (COMGP_STATUS, 'ND') = 'ACTIF'
                                                  AND NVL (COMGP_STATUS_DATE, SYSDATE + 1000) = d_slice_value ) THEN d_slice_value
                                              ELSE NULL  END) acquisition_date
                                      , (CASE
                                              WHEN (NVL (COMGP_STATUS, 'ND') = 'INACT'
                                                  AND NVL (COMGP_STATUS_DATE, SYSDATE + 1000) = d_slice_value ) THEN d_slice_value
                                              ELSE NULL  END) desacquisition_date
                                      FROM MON.FT_ACCOUNT_ACTIVITY  WHERE -- ROWNUM < 1 AND
                                              EVENT_DATE = d_account_activity_date  ) b, mon.VW_DT_OFFER_PROFILES c
              WHERE  -- ROWNUM < 1 AND
                  --a.EVENT_DATE= d_contract_snapshot_date -- ( d_slice_value + 1)
                   ( b.ACTIVATION_DATE <= d_slice_value )
                  -- Filtrer sur les 'PURE PREPAID' :: 20110126_175705
                  --AND b.SRC_TABLE = 'IT_ICC_ACCOUNT'
                  AND NVL (c.CONTRACT_TYPE, 'PURE PREPAID' ) IN ('PURE PREPAID', 'HYBRID')
                  AND a.ACCESS_KEY = b.MSISDN (+)
                  AND b.PROFILE=c.PROFILE_CODE(+)
              GROUP BY
                     b.PROFILE
                   , b.COMGP_STATUS
                   , DECODE (NVL (BLOCKED, '0'), '1', 'BLOCKED', 'NOT BLOCKED')
                   , TO_CHAR (COMGP_FIRST_ACTIVE_DATE,'yyyymm')
                   , a.location , a.OSP_CUSTOMER_CGLIST, NVL (c.CONTRACT_TYPE, 'PURE PREPAID' )
                   , b.SRC_TABLE
                   , DECODE (b.OSP_STATUS, 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
                      , 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
                      , b.OSP_STATUS)
                   , (CASE WHEN b.ACTIVATION_DATE IS NULL THEN NULL
                        WHEN b.ACTIVATION_DATE <  TO_DATE('19880101','yyyymmdd') THEN NULL
                        WHEN b.ACTIVATION_DATE < ADD_MONTHS (  TO_DATE (SUBSTR (s_slice_value, 1, 6) || '01' , 'yyyymmdd')   , -5) THEN  TO_CHAR (ACTIVATION_DATE, 'yyyy')
                        WHEN b.ACTIVATION_DATE = d_slice_value THEN  s_slice_value
                        ELSE TO_CHAR (ACTIVATION_DATE, 'yyyymm') END )
          ) a
          , mon.VW_DT_OFFER_PROFILES d
          WHERE
              UPPER (a.commercial_offer) = d.PROFILE_CODE (+)
          GROUP BY
              datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (d.CONTRACT_TYPE)
              , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source
              , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) , CUSTOMER_ID
              , platform_account_status, platform_activation_month
  );
  -- ne pas faire de commit ici

-- CALCULER LES DONNÉES 'PURE POSTPAID'
INSERT INTO  mon.FT_COMMERCIAL_SUBSCRIB_SUMMARY
      (DATECODE, NETWORK_DOMAIN, NETWORK_TECHNOLOGY, SUBSCRIBER_CATEGORY, CUSTOMER_ID, SUBSCRIBER_TYPE, COMMERCIAL_OFFER, ACCOUNT_STATUS, LOCK_STATUS, ACTIVATION_MONTH, CITYZONE, USAGE_TYPE, TOTAL_COUNT, TOTAL_ACTIVATION, TOTAL_DEACTIVATION, TOTAL_EXPIRATION, TOTAL_PROVISIONNED, TOTAL_MAIN_CREDIT, TOTAL_PROMO_CREDIT, TOTAL_SMS_CREDIT, TOTAL_DATA_CREDIT, SOURCE, REFRESH_DATE, PROFILE_NAME
      , platform_account_status, platform_activation_month )
      (
      SELECT
          datecode, network_domain, network_technology
          , UPPER (d.CRM_SEGMENTATION) subscriber_category
          , CUSTOMER_ID
          , UPPER (a.OSP_CONTRACT_TYPE) subscriber_type
          , commercial_offer, account_status , lock_status, activation_month, cityzone
          , usage_type
          , SUM ( NVL (total_count, 0) ) total_count , SUM ( NVL (total_activation, 0) ) total_activation
          , SUM ( NVL (total_deactivation, 0) ) total_deactivation , SUM ( NVL (total_expiration, 0) ) total_expiration
          , SUM ( NVL (total_provisionned, 0) ) total_provisionned , SUM ( NVL (total_main_credit, 0) ) total_main_credit
          , SUM ( NVL (total_promo_credit, 0) ) total_promo_credit , SUM ( NVL (total_sms_credit, 0) ) total_sms_credit
          , SUM ( NVL (total_data_credit, 0) ) total_data_credit
          , source
          , SYSDATE  REFRESH_DATE
          , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
          , platform_account_status, platform_activation_month
      FROM
          (
              SELECT d_slice_value_moins_1 datecode
                   , 'PURE POSTPAID' OSP_CONTRACT_TYPE
                   , 'GSM'  network_domain
                   , 'Telephony' network_technology
                   , a.OSP_CUSTOMER_CGLIST customer_id
                   , UPPER(a.PROFILE) commercial_offer--UPPER (NVL  ( SUBSTR(a.BSCS_COMM_OFFER, INSTR(a.BSCS_COMM_OFFER,'|',1,1)+1), a.OSP_CUSTOMER_FORMULE) ) commercial_offer
                   ,  (CASE
                          WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
                          ELSE    'INACT'
                       END  ) account_status
                   , DECODE (NVL (BLOCKED, '0'), '1', 'BLOCKED', 'NOT BLOCKED') lock_status -- blocked
                   , TO_CHAR ( NVL (b.COMGP_FIRST_ACTIVE_DATE, a.BSCS_ACTIVATION_DATE) ,'yyyymm') activation_month
                   , NULL cityzone -- location
                   , 'Telephony' usage_type
                   , SUM (1) total_count
                   , SUM(CASE WHEN NVL (acquisition_date, a.BSCS_ACTIVATION_DATE)  = d_slice_value THEN 1 ELSE 0 END) total_activation
                   , SUM(CASE WHEN NVL (desacquisition_date, a.BSCS_DEACTIVATION_DATE) = d_slice_value THEN 1 ELSE 0 END) total_deactivation
                   , 0 total_expiration
                   , SUM (CASE WHEN PROVISIONING_DATE = d_slice_value THEN 1 ELSE 0 END) total_provisionned
                   -- :: 20101208_152506 :: @change: 20101208_151938 by achille.tchapi : eliminer l'effet de bord dû à la presence des credits en compte negatif.
                   , SUM((CASE WHEN MAIN_CREDIT IS NULL THEN 0 WHEN MAIN_CREDIT < 0 THEN 0 ELSE MAIN_CREDIT END )) total_main_credit
                   , SUM((CASE WHEN PROMO_CREDIT IS NULL THEN 0 WHEN PROMO_CREDIT < 0 THEN 0 ELSE PROMO_CREDIT END )) total_promo_credit
                   , 0 total_sms_credit
                   , 0 total_data_credit
                   , 'FT_BSCS_CONTRACT' source
                   , DECODE (NVL(a.CURRENT_STATUS, a.OSP_STATUS), 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
                      , 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
                      , NVL(a.CURRENT_STATUS, a.OSP_STATUS)) platform_account_status
                   , (CASE WHEN a.BSCS_ACTIVATION_DATE IS NULL THEN NULL
                        WHEN a.BSCS_ACTIVATION_DATE <  TO_DATE('19880101','yyyymmdd') THEN NULL
                        WHEN a.bscs_activation_date < (d_slice_value - 365) THEN  TO_CHAR (BSCS_ACTIVATION_DATE, 'yyyy')
                        WHEN a.BSCS_ACTIVATION_DATE = d_slice_value THEN  s_slice_value
                        ELSE TO_CHAR (BSCS_ACTIVATION_DATE, 'yyyymm') END ) platform_activation_month
              FROM mon.FT_CONTRACT_SNAPSHOT a
                          , (SELECT  MSISDN, COMGP_STATUS_DATE, COMGP_STATUS, COMGP_FIRST_ACTIVE_DATE
                                      , (CASE
                                              WHEN (NVL (COMGP_STATUS, 'ND') = 'ACTIF'
                                                  AND NVL (COMGP_STATUS_DATE, SYSDATE + 1000) = d_slice_value ) THEN d_slice_value
                                              ELSE NULL  END) acquisition_date
                                      , (CASE
                                              WHEN (NVL (COMGP_STATUS, 'ND') = 'INACT'
                                                  AND NVL (COMGP_STATUS_DATE, SYSDATE + 1000) = d_slice_value ) THEN d_slice_value
                                              ELSE NULL  END) desacquisition_date
                                      FROM MON.FT_ACCOUNT_ACTIVITY  WHERE -- ROWNUM < 1 AND
                                              EVENT_DATE = d_account_activity_date  ) b
              WHERE  -- ROWNUM < 1 AND
                  EVENT_DATE= d_contract_snapshot_date -- ( d_slice_value + 1)
                  AND ( NVL (a.BSCS_ACTIVATION_DATE, a.ACTIVATION_DATE) <= d_slice_value )
                  -- Filtrer sur les 'PURE PREPAID' :: 20110126_175705
                  --AND a.SRC_TABLE = 'IT_ICC_ACCOUNT'
                  AND NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) = 'PURE POSTPAID'
                  AND a.ACCESS_KEY = b.MSISDN (+)
              GROUP BY
                     UPPER(a.PROFILE) --UPPER (NVL  ( SUBSTR(a.BSCS_COMM_OFFER, INSTR(a.BSCS_COMM_OFFER,'|',1,1)+1), a.OSP_CUSTOMER_FORMULE) )
                   ,  (CASE
                          WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
                          ELSE    'INACT'
                       END  )
                   , DECODE (NVL (BLOCKED, '0'), '1', 'BLOCKED', 'NOT BLOCKED')
                    , TO_CHAR ( NVL (b.COMGP_FIRST_ACTIVE_DATE, a.BSCS_ACTIVATION_DATE) ,'yyyymm')
                   , a.location  , a.OSP_CUSTOMER_CGLIST
                   , SRC_TABLE
                   , DECODE (NVL(a.CURRENT_STATUS, a.OSP_STATUS), 'ACTIVE', 'ACTIF', 'a' , 'ACTIF', 'd', 'DEACT'
                      , 's', 'INACT', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACT', 'VALID', 'VALIDE'
                      , NVL(a.CURRENT_STATUS, a.OSP_STATUS))
                   , (CASE WHEN a.BSCS_ACTIVATION_DATE IS NULL THEN NULL
                        WHEN a.BSCS_ACTIVATION_DATE <  TO_DATE('19880101','yyyymmdd') THEN NULL
                        WHEN a.bscs_activation_date < (d_slice_value - 365) THEN  TO_CHAR (BSCS_ACTIVATION_DATE, 'yyyy')
                        WHEN a.BSCS_ACTIVATION_DATE = d_slice_value THEN  s_slice_value
                        ELSE TO_CHAR (BSCS_ACTIVATION_DATE, 'yyyymm') END )
          ) a
          , mon.VW_DT_OFFER_PROFILES d
          WHERE
              UPPER (a.commercial_offer) = d.PROFILE_CODE (+)
          GROUP BY
              datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (a.OSP_CONTRACT_TYPE)
              , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source, CUSTOMER_ID
              , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer ))
              , platform_account_status, platform_activation_month
  );