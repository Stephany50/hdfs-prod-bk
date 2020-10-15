INSERT INTO MON.SPARK_FT_COMMERCIAL_SUBSCRIB_SUMMARY PARTITION(DATECODE)
-- CALCULER LES DONNÉES 'PURE PREPAID' et 'HYBRID' QUI REMONTE DE LA PHOTO
SELECT
    network_domain, network_technology
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
    , CURRENT_TIMESTAMP  REFRESH_DATE
    , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
    , platform_account_status
    , platform_activation_month
    , location_ci
    , datecode
FROM
    (
        SELECT DATE_SUB('###SLICE_VALUE###',1) datecode
            , NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE
             , 'GSM'  network_domain
             , 'Telephony' network_technology
             , a.OSP_CUSTOMER_CGLIST customer_id
             , a.PROFILE commercial_offer
             , b.COMGP_STATUS account_status
             , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED') lock_status -- blocked
             , DATE_FORMAT (b.COMGP_FIRST_ACTIVE_DATE,'yyyyMM') activation_month
             , NULL  cityzone
             , 'Telephony' usage_type
             , SUM (1) total_count
             , SUM(CASE WHEN acquisition_date= '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_activation
             , SUM(CASE WHEN desacquisition_date= '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_deactivation
             , 0 total_expiration
             , SUM (CASE WHEN PROVISIONING_DATE = '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_provisionned
             -- :: 20101208_152506 :: @change: 20101208_151938 by achille.tchapi : eliminer l'effet de bord dû à la presence des credits en compte negatif.
             , SUM((CASE WHEN MAIN_CREDIT IS NULL THEN 0 WHEN MAIN_CREDIT < 0 THEN 0 ELSE MAIN_CREDIT END )) total_main_credit
             , SUM((CASE WHEN PROMO_CREDIT IS NULL THEN 0 WHEN PROMO_CREDIT < 0 THEN 0 ELSE PROMO_CREDIT END )) total_promo_credit
             , 0 total_sms_credit
             , 0 total_data_credit
             , a.SRC_TABLE source
             , CASE
                  WHEN a.OSP_STATUS='ACTIVE' THEN 'ACTIF'
                  WHEN a.OSP_STATUS='a' THEN 'ACTIF'
                  WHEN a.OSP_STATUS='d' THEN 'DEACT'
                  WHEN a.OSP_STATUS='s' THEN 'INACT'
                  WHEN a.OSP_STATUS='INACTIVE' THEN 'INACT'
                  WHEN a.OSP_STATUS='DEACTIVATED' THEN 'DEACT'
                  WHEN a.OSP_STATUS='VALID' THEN 'VALIDE'
                  ELSE a.OSP_STATUS
              END platform_account_status
             , (CASE
                  WHEN a.ACTIVATION_DATE IS NULL THEN NULL
                  WHEN a.ACTIVATION_DATE < TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('19880101','yyyyMMdd'))) THEN NULL
                  WHEN a.activation_date < ADD_MONTHS (  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR ('###SLICE_VALUE###', 1,7 ),'01'),'yyyy-MMdd'))), -5) THEN  DATE_FORMAT (ACTIVATION_DATE, 'yyyy')
                  WHEN a.activation_date = '###SLICE_VALUE###' THEN DATE_FORMAT('###SLICE_VALUE###','yyyyMMdd')
                  ELSE DATE_FORMAT (ACTIVATION_DATE, 'yyyyMM')
               END ) platform_activation_month
             , location_ci
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT a
        LEFT JOIN (SELECT  MSISDN, COMGP_STATUS_DATE, COMGP_STATUS, COMGP_FIRST_ACTIVE_DATE
                  , (CASE
                          WHEN (NVL (COMGP_STATUS, 'ND') = 'ACTIF'
                              AND NVL (COMGP_STATUS_DATE, DATE_SUB(CURRENT_TIMESTAMP,-1000)) = '###SLICE_VALUE###' ) THEN '###SLICE_VALUE###'
                          ELSE NULL  END) acquisition_date
                  , (CASE
                          WHEN (NVL (COMGP_STATUS, 'ND') = 'INACT'
                              AND NVL (COMGP_STATUS_DATE, DATE_SUB(CURRENT_TIMESTAMP,-1000)) = '###SLICE_VALUE###' ) THEN '###SLICE_VALUE###'
                          ELSE NULL  END) desacquisition_date
                  FROM MON.SPARK_FT_ACCOUNT_ACTIVITY   -- ROWNUM < 1 AND
                  WHERE EVENT_DATE = '###SLICE_VALUE###'
        ) b ON a.ACCESS_KEY = b.MSISDN
        WHERE
            a.EVENT_DATE= '###SLICE_VALUE###'
            AND a.ACTIVATION_DATE <= '###SLICE_VALUE###'
            AND NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) IN ('PURE PREPAID', 'HYBRID')
        GROUP BY
               a.PROFILE
             , b.COMGP_STATUS
             , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED')
             , DATE_FORMAT (b.COMGP_FIRST_ACTIVE_DATE,'yyyyMM')
             , a.location , a.OSP_CUSTOMER_CGLIST, NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' )
             , a.SRC_TABLE
             , CASE
                  WHEN a.OSP_STATUS='ACTIVE' THEN 'ACTIF'
                  WHEN a.OSP_STATUS='a' THEN 'ACTIF'
                  WHEN a.OSP_STATUS='d' THEN 'DEACT'
                  WHEN a.OSP_STATUS='s' THEN 'INACT'
                  WHEN a.OSP_STATUS='INACTIVE' THEN 'INACT'
                  WHEN a.OSP_STATUS='DEACTIVATED' THEN 'DEACT'
                  WHEN a.OSP_STATUS='VALID' THEN 'VALIDE'
                  ELSE a.OSP_STATUS
              END
             , (CASE WHEN a.ACTIVATION_DATE IS NULL THEN NULL
                  WHEN a.ACTIVATION_DATE <  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('19880101','yyyyMMdd'))) THEN NULL
                  WHEN a.activation_date < ADD_MONTHS (  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR ('###SLICE_VALUE###', 1,7 ),'01'),'yyyy-MMdd'))), -5) THEN  DATE_FORMAT (ACTIVATION_DATE, 'yyyy')
                  WHEN a.activation_date = '###SLICE_VALUE###' THEN DATE_FORMAT('###SLICE_VALUE###','yyyyMMdd')
                  ELSE DATE_FORMAT (ACTIVATION_DATE, 'yyyyMM')
               END )
             , location_ci
) a
LEFT JOIN MON.VW_DT_OFFER_PROFILES d ON UPPER (a.commercial_offer) = d.PROFILE_CODE
GROUP BY
    datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (a.OSP_CONTRACT_TYPE)
    , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source
    , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) , CUSTOMER_ID
    , platform_account_status, platform_activation_month,location_ci

UNION ALL

-- CALCULER LES DONNÉES 'PURE PREPAID' et 'HYBRID' DES MSISDN QUI DISPARAISSENT DE LA PHOTO
SELECT
    network_domain, network_technology
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
    , CURRENT_TIMESTAMP  REFRESH_DATE
    , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
    , platform_account_status
    , platform_activation_month
    , location_ci
    , datecode
FROM (
  SELECT DATE_SUB('###SLICE_VALUE###',1) datecode
      , NVL (c.CONTRACT_TYPE, 'PURE PREPAID' ) OSP_CONTRACT_TYPE
       , 'GSM'  network_domain
       , 'Telephony' network_technology
       , NULL customer_id --, a.OSP_CUSTOMER_CGLIST customer_id
       , b.PROFILE commercial_offer
       , b.COMGP_STATUS account_status
       , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED') lock_status -- blocked
       , DATE_FORMAT (b.COMGP_FIRST_ACTIVE_DATE,'yyyyMM') activation_month
       , NULL  cityzone
       , 'Telephony' usage_type
       , SUM (1) total_count
       , SUM(CASE WHEN acquisition_date= '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_activation
       , SUM(CASE WHEN desacquisition_date= '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_deactivation
       , 0 total_expiration
       , SUM (CASE WHEN PROVISION_DATE = '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_provisionned
       -- :: 20101208_152506 :: @change: 20101208_151938 by achille.tchapi : eliminer l'effet de bord dû à la presence des credits en compte negatif.
       , SUM((CASE WHEN REMAIN_CREDIT_MAIN IS NULL THEN 0 WHEN REMAIN_CREDIT_MAIN < 0 THEN 0 ELSE REMAIN_CREDIT_MAIN END )) total_main_credit
       , SUM((CASE WHEN REMAIN_CREDIT_PROMO IS NULL THEN 0 WHEN REMAIN_CREDIT_PROMO < 0 THEN 0 ELSE REMAIN_CREDIT_PROMO END )) total_promo_credit
       , 0 total_sms_credit
       , 0 total_data_credit
       , b.SRC_TABLE source
       , CASE
            WHEN b.OSP_STATUS='ACTIVE' THEN 'ACTIF'
            WHEN b.OSP_STATUS='a' THEN 'ACTIF'
            WHEN b.OSP_STATUS='d' THEN 'DEACT'
            WHEN b.OSP_STATUS='s' THEN 'INACT'
            WHEN b.OSP_STATUS='INACTIVE' THEN 'INACT'
            WHEN b.OSP_STATUS='DEACTIVATED' THEN 'DEACT'
            WHEN b.OSP_STATUS='VALID' THEN 'VALIDE'
            ELSE b.OSP_STATUS
        END platform_account_status
       , (CASE WHEN b.ACTIVATION_DATE IS NULL THEN NULL
            WHEN b.ACTIVATION_DATE < TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('19880101','yyyyMMdd'))) THEN NULL
            WHEN b.ACTIVATION_DATE < ADD_MONTHS (  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR ('###SLICE_VALUE###', 1,7 ),'01'),'yyyy-MMdd'))), -5) THEN  DATE_FORMAT (ACTIVATION_DATE, 'yyyy')
            WHEN b.ACTIVATION_DATE = '###SLICE_VALUE###' THEN DATE_FORMAT('###SLICE_VALUE###','yyyyMMdd')
            ELSE DATE_FORMAT (ACTIVATION_DATE, 'yyyyMM')
        END ) platform_activation_month
       , location_ci
  FROM (
      SELECT
        MSISDN access_key,
        '' OSP_CUSTOMER_CGLIST,
        '' location
      FROM (
        SELECT
          a.MSISDN
        FROM MON.SPARK_FT_ACCOUNT_ACTIVITY a
        LEFT JOIN (SELECT access_key MSISDN FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' ) b ON a.MSISDN=b.MSISDN
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND NVL (COMGP_STATUS, 'INACT')='ACTIF' AND b.MSISDN IS NULL
      ) TT_TMP_MISSING_MSISDN
      --where rownum < greatest(DATEDIFF( '###SLICE_VALUE###', TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('20140420','yyyyMMdd'))))*3000,0) -- INTECTION PROGRESSIVE
  ) a
  LEFT JOIN (
    SELECT
      MSISDN,
      COMGP_STATUS_DATE,
      COMGP_STATUS,
      COMGP_FIRST_ACTIVE_DATE ,
      FORMULE PROFILE, SRC_TABLE,
      PLATFORM_STATUS OSP_STATUS,
      '' BLOCKED,
      ACTIVATION_DATE,
      REMAIN_CREDIT_MAIN,
      REMAIN_CREDIT_PROMO,
      PROVISION_DATE
      , (CASE
              WHEN (NVL (COMGP_STATUS, 'ND') = 'ACTIF'
                  AND NVL (COMGP_STATUS_DATE, DATE_SUB(CURRENT_TIMESTAMP,-1000)) = '###SLICE_VALUE###' ) THEN '###SLICE_VALUE###'
              ELSE NULL
         END) acquisition_date
      , (CASE
              WHEN (NVL (COMGP_STATUS, 'ND') = 'INACT'
                  AND NVL (COMGP_STATUS_DATE, DATE_SUB(CURRENT_TIMESTAMP,-1000)) = '###SLICE_VALUE###' ) THEN '###SLICE_VALUE###'
              ELSE NULL
        END) desacquisition_date
      , location_ci
    FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  WHERE EVENT_DATE = '###SLICE_VALUE###'
  ) b ON a.ACCESS_KEY = b.MSISDN
  LEFT JOIN  mon.VW_DT_OFFER_PROFILES c ON b.PROFILE=c.PROFILE_CODE
  WHERE
      b.ACTIVATION_DATE <= '###SLICE_VALUE###'
      AND NVL (c.CONTRACT_TYPE, 'PURE PREPAID' ) IN ('PURE PREPAID', 'HYBRID')
  GROUP BY
         b.PROFILE
       , b.COMGP_STATUS
       , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED')
       , DATE_FORMAT (b.COMGP_FIRST_ACTIVE_DATE,'yyyyMM')
       , a.location , a.OSP_CUSTOMER_CGLIST, NVL (c.CONTRACT_TYPE, 'PURE PREPAID' )
       , b.SRC_TABLE
       , CASE
            WHEN b.OSP_STATUS='ACTIVE' THEN 'ACTIF'
            WHEN b.OSP_STATUS='a' THEN 'ACTIF'
            WHEN b.OSP_STATUS='d' THEN 'DEACT'
            WHEN b.OSP_STATUS='s' THEN 'INACT'
            WHEN b.OSP_STATUS='INACTIVE' THEN 'INACT'
            WHEN b.OSP_STATUS='DEACTIVATED' THEN 'DEACT'
            WHEN b.OSP_STATUS='VALID' THEN 'VALIDE'
            ELSE b.OSP_STATUS
        END
       , (CASE WHEN b.ACTIVATION_DATE IS NULL THEN NULL
            WHEN b.ACTIVATION_DATE < TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('19880101','yyyyMMdd'))) THEN NULL
            WHEN b.ACTIVATION_DATE < ADD_MONTHS (  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR ('###SLICE_VALUE###', 1,7 ),'01'),'yyyy-MMdd'))), -5) THEN  DATE_FORMAT (ACTIVATION_DATE, 'yyyy')
            WHEN b.ACTIVATION_DATE = '###SLICE_VALUE###' THEN DATE_FORMAT('###SLICE_VALUE###','yyyyMMdd')
            ELSE DATE_FORMAT (ACTIVATION_DATE, 'yyyyMM')
        END)
       ,location_ci
) a
LEFT JOIN mon.VW_DT_OFFER_PROFILES d ON UPPER (a.commercial_offer) = d.PROFILE_CODE
GROUP BY
    datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (d.CONTRACT_TYPE)
    , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source
    , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) , CUSTOMER_ID
    , platform_account_status, platform_activation_month,location_ci

UNION ALL
-- CALCULER LES DONNÉES 'PURE POSTPAID'

SELECT
    network_domain, network_technology
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
    , CURRENT_TIMESTAMP  REFRESH_DATE
    , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer )) PROFILE_NAME
    , platform_account_status
    , platform_activation_month
    , location_ci
    , datecode
FROM(
  SELECT DATE_SUB('###SLICE_VALUE###',1)datecode
       , 'PURE POSTPAID' OSP_CONTRACT_TYPE
       , 'GSM'  network_domain
       , 'Telephony' network_technology
       , a.OSP_CUSTOMER_CGLIST customer_id
       , UPPER(a.PROFILE) commercial_offer--UPPER (NVL  ( SUBSTR(a.BSCS_COMM_OFFER, INSTR(a.BSCS_COMM_OFFER,'|',1,1)+1), a.OSP_CUSTOMER_FORMULE) ) commercial_offer
       ,  (CASE
              WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
              ELSE    'INACT'
           END  ) account_status
       , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED') lock_status -- blocked
       , DATE_FORMAT ( NVL (b.COMGP_FIRST_ACTIVE_DATE, a.BSCS_ACTIVATION_DATE) ,'yyyyMM') activation_month
       , NULL cityzone -- location
       , 'Telephony' usage_type
       , SUM (1) total_count
       , SUM(CASE WHEN NVL (acquisition_date, a.BSCS_ACTIVATION_DATE)  = '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_activation
       , SUM(CASE WHEN NVL (desacquisition_date, a.BSCS_DEACTIVATION_DATE) = '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_deactivation
       , 0 total_expiration
       , SUM (CASE WHEN PROVISIONING_DATE = '###SLICE_VALUE###' THEN 1 ELSE 0 END) total_provisionned
       -- :: 20101208_152506 :: @change: 20101208_151938 by achille.tchapi : eliminer l'effet de bord dû à la presence des credits en compte negatif.
       , SUM((CASE WHEN MAIN_CREDIT IS NULL THEN 0 WHEN MAIN_CREDIT < 0 THEN 0 ELSE MAIN_CREDIT END )) total_main_credit
       , SUM((CASE WHEN PROMO_CREDIT IS NULL THEN 0 WHEN PROMO_CREDIT < 0 THEN 0 ELSE PROMO_CREDIT END )) total_promo_credit
       , 0 total_sms_credit
       , 0 total_data_credit
       , 'FT_BSCS_CONTRACT' source
       , CASE
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='ACTIVE' THEN 'ACTIF'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='a' THEN 'ACTIF'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='d' THEN 'DEACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='s' THEN 'INACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='INACTIVE' THEN 'INACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='DEACTIVATED' THEN 'DEACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='VALID' THEN 'VALIDE'
            ELSE NVL(a.CURRENT_STATUS, a.OSP_STATUS)
        END platform_account_status
       , (CASE WHEN a.BSCS_ACTIVATION_DATE IS NULL THEN NULL
            WHEN a.BSCS_ACTIVATION_DATE < TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('19880101','yyyyMMdd'))) THEN NULL
            WHEN a.bscs_activation_date < DATE_SUB('###SLICE_VALUE###',365) THEN  DATE_FORMAT (BSCS_ACTIVATION_DATE, 'yyyy')
            WHEN a.BSCS_ACTIVATION_DATE = '###SLICE_VALUE###' THEN DATE_FORMAT('###SLICE_VALUE###','yyyyMMdd')
            ELSE DATE_FORMAT (BSCS_ACTIVATION_DATE, 'yyyyMM')
        END ) platform_activation_month
       , location_ci
  FROM MON.SPARK_FT_CONTRACT_SNAPSHOT a
  LEFT JOIN (
      SELECT
        MSISDN,
        COMGP_STATUS_DATE,
        COMGP_STATUS,
        COMGP_FIRST_ACTIVE_DATE
        , (CASE
                WHEN (NVL (COMGP_STATUS, 'ND') = 'ACTIF'
                    AND NVL (COMGP_STATUS_DATE, DATE_SUB(CURRENT_TIMESTAMP,-1000)) = '###SLICE_VALUE###' ) THEN '###SLICE_VALUE###'
                ELSE NULL
            END) acquisition_date
        , (CASE
                WHEN (NVL (COMGP_STATUS, 'ND') = 'INACT'
                    AND NVL (COMGP_STATUS_DATE, DATE_SUB(CURRENT_TIMESTAMP,-1000)) = '###SLICE_VALUE###' ) THEN '###SLICE_VALUE###'
                ELSE NULL
           END) desacquisition_date
        FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  WHERE  EVENT_DATE = '###SLICE_VALUE###'
  ) b ON  a.ACCESS_KEY = b.MSISDN
  WHERE  -- ROWNUM < 1 AND
      EVENT_DATE= '###SLICE_VALUE###'
      AND ( NVL (a.BSCS_ACTIVATION_DATE, a.ACTIVATION_DATE) <= '###SLICE_VALUE###' )
      AND NVL (a.OSP_CONTRACT_TYPE, 'PURE PREPAID' ) = 'PURE POSTPAID'
  GROUP BY
         UPPER(a.PROFILE) --UPPER (NVL  ( SUBSTR(a.BSCS_COMM_OFFER, INSTR(a.BSCS_COMM_OFFER,'|',1,1)+1), a.OSP_CUSTOMER_FORMULE) )
       ,  (CASE
              WHEN  a.CURRENT_STATUS IN  ('a', 's')  THEN 'ACTIF'
              ELSE    'INACT'
           END  )
       , IF (NVL (BLOCKED, '0')='1', 'BLOCKED', 'NOT BLOCKED')
        , DATE_FORMAT ( NVL (b.COMGP_FIRST_ACTIVE_DATE, a.BSCS_ACTIVATION_DATE) ,'yyyyMM')
       , a.location  , a.OSP_CUSTOMER_CGLIST
       , SRC_TABLE
       , CASE
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='ACTIVE' THEN 'ACTIF'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='a' THEN 'ACTIF'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='d' THEN 'DEACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='s' THEN 'INACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='INACTIVE' THEN 'INACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='DEACTIVATED' THEN 'DEACT'
            WHEN NVL(a.CURRENT_STATUS, a.OSP_STATUS)='VALID' THEN 'VALIDE'
            ELSE NVL(a.CURRENT_STATUS, a.OSP_STATUS)
        END
       , (CASE WHEN a.BSCS_ACTIVATION_DATE IS NULL THEN NULL
            WHEN a.BSCS_ACTIVATION_DATE <  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('19880101','yyyyMMdd'))) THEN NULL
            WHEN a.bscs_activation_date < DATE_SUB('###SLICE_VALUE###',365) THEN  DATE_FORMAT (BSCS_ACTIVATION_DATE, 'yyyy')
            WHEN a.BSCS_ACTIVATION_DATE = '###SLICE_VALUE###' THEN  DATE_FORMAT('###SLICE_VALUE###','yyyyMMdd')
            ELSE DATE_FORMAT (BSCS_ACTIVATION_DATE, 'yyyyMM') END )
       , location_ci
) a
LEFT JOIN MON.VW_DT_OFFER_PROFILES d ON UPPER (a.commercial_offer) = d.PROFILE_CODE
GROUP BY
  datecode, network_domain, network_technology , UPPER (d.CRM_SEGMENTATION)  , UPPER (a.OSP_CONTRACT_TYPE)
  , commercial_offer , account_status , lock_status , activation_month, cityzone, usage_type , source, CUSTOMER_ID
  , UPPER ( NVL ( d.PROFILE_NAME, commercial_offer ))
  , platform_account_status, platform_activation_month
  , location_ci