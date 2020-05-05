  INSERT INTO AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY PARTITION(DATECODE)
  SELECT
  network_domain, network_technology
  , UPPER (d.CRM_SEGMENTATION) subscriber_category
  , CUSTOMER_ID
  , UPPER (a.OSP_CONTRACT_TYPE)  subscriber_type
  , commercial_offer, account_status , lock_status, activation_month, cityzone
  , usage_type
  , SUM ( NVL (total_count, 0) ) total_count
  , SUM ( NVL (total_activation, 0) ) total_activation
  , SUM ( NVL (total_deactivation, 0) ) total_deactivation
  , SUM ( NVL (total_expiration, 0) ) total_expiration
  , SUM ( NVL (total_provisionned, 0) ) total_provisionned
  , SUM ( NVL (total_main_credit, 0) ) total_main_credit
  , SUM ( NVL (total_promo_credit, 0) ) total_promo_credit
  , SUM ( NVL (total_sms_credit, 0) ) total_sms_credit
  , SUM ( NVL (total_data_credit, 0) ) total_data_credit
  , source
  , CURRENT_TIMESTAMP  REFRESH_DATE
  , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
  , NULL process_name
  ,location_ci
  , datecode
  FROM
  (
    SELECT '###SLICE_VALUE###' datecode
        , NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE
                , 'GSM'  network_domain
                , 'Telephony' network_technology
                , a.OSP_CUSTOMER_CGLIST customer_id
                , a.PROFILE commercial_offer
         , CASE
              WHEN a.OSP_STATUS='ACTIVE' THEN 'ACTIF'
              WHEN a.OSP_STATUS='a' THEN 'ACTIF'
              WHEN a.OSP_STATUS='d' THEN 'DEACT'
              WHEN a.OSP_STATUS='s' THEN 'INACT'
              WHEN a.OSP_STATUS='INACTIVE' THEN 'INACT'
              WHEN a.OSP_STATUS='DEACTIVATED' THEN 'DEACT'
              WHEN a.OSP_STATUS='VALID' THEN 'VALIDE'
              ELSE a.OSP_STATUS
           END  account_status
         , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED') lock_status        , DATE_FORMAT (NVL (a.ACTIVATION_DATE,a.BSCS_ACTIVATION_DATE),'yyyyMM') activation_month
         , a.location cityzone               , 'Telephony' usage_type
         , SUM (1) total_count
                , SUM(CASE
                WHEN NVL (a.ACTIVATION_DATE,a.BSCS_ACTIVATION_DATE)= '###SLICE_VALUE###' AND
                   (CASE
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='ACTIVE' THEN 'ACTIF'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='a' THEN 'ACTIF'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='d' THEN 'DEACT'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='s' THEN 'INACT'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='s' THEN 'INACT'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='INACTIVE' THEN 'INACT'
                      WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='VALID' THEN 'VALIDE'
                      ELSE NVL(a.OSP_STATUS,a.CURRENT_STATUS)
                  END)='ACTIF' THEN 1
                ELSE 0
            END) total_activation        , SUM(CASE
                  WHEN DEACTIVATION_DATE= '###SLICE_VALUE###' AND
                     (CASE
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='ACTIVE' THEN 'ACTIF'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='a' THEN 'ACTIF'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='d' THEN 'DEACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='s' THEN 'INACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='s' THEN 'INACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='INACTIVE' THEN 'INACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='VALID' THEN 'VALIDE'
                        ELSE NVL(a.OSP_STATUS,a.CURRENT_STATUS)
                    END)='DEACT' THEN 1
                  ELSE 0
              END) total_deactivation
         , 0 total_expiration
         , SUM (CASE
                  WHEN PROVISIONING_DATE = '###SLICE_VALUE###' AND
                   (CASE
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='ACTIVE' THEN 'ACTIF'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='a' THEN 'ACTIF'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='d' THEN 'DEACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='s' THEN 'INACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='s' THEN 'INACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='INACTIVE' THEN 'INACT'
                        WHEN NVL(a.OSP_STATUS,a.CURRENT_STATUS)='VALID' THEN 'VALIDE'
                        ELSE NVL(a.OSP_STATUS,a.CURRENT_STATUS)
                    END)='VALIDE' THEN 1
                  ELSE 0
              END) total_provisionned
                , SUM((CASE WHEN MAIN_CREDIT IS NULL THEN 0 WHEN MAIN_CREDIT < 0 THEN 0 WHEN OSP_CONTRACT_TYPE='PURE POSTPAID' THEN 0 ELSE MAIN_CREDIT END )) total_main_credit
         , SUM((CASE WHEN PROMO_CREDIT IS NULL THEN 0 WHEN PROMO_CREDIT < 0 THEN 0 WHEN OSP_CONTRACT_TYPE='PURE POSTPAID' THEN 0 ELSE PROMO_CREDIT END )) total_promo_credit
         , 0 total_sms_credit
         , 0 total_data_credit
         , SRC_TABLE source
         ,location_ci
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT a
    WHERE
        a.EVENT_DATE= DATE_SUB('###SLICE_VALUE###',-1)
        AND ( NVL (a.ACTIVATION_DATE,a.BSCS_ACTIVATION_DATE) <= '###SLICE_VALUE###' )
    GROUP BY
           a.PROFILE
         , a.OSP_STATUS
         , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED')
         , DATE_FORMAT (NVL (a.ACTIVATION_DATE,a.BSCS_ACTIVATION_DATE),'yyyyMM')
         , a.location , a.OSP_CUSTOMER_CGLIST, NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' )
         , SRC_TABLE
         ,location_ci
  ) a
  LEFT JOIN mon.VW_DT_OFFER_PROFILES d ON  UPPER (a.commercial_offer) = d.PROFILE_CODE
  GROUP BY
    datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (a.OSP_CONTRACT_TYPE)
    , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source
    , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) , CUSTOMER_ID,location_ci