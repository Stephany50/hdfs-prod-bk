create view MON.VW_DT_OFFER_PROFILES as
(
    SELECT
    UPPER (b.PROFILE_CODE) PROFILE_CODE
    , MAX (UPPER (b.CUSTOMER_TYPE)) CUSTOMER_TYPE
    , MAX (UPPER (b.OFFER_NAME))  OFFER_NAME
    , MAX (UPPER (b.PROFILE_NAME)) PROFILE_NAME
    , MAX (UPPER (b.CRM_SEGMENTATION)) CRM_SEGMENTATION
    , MAX (UPPER (b.CUSTOMER_PROFILE)) CUSTOMER_PROFILE
    , MAX (UPPER (DECILE_TYPE)) DECILE_TYPE
    , MAX (UPPER (CONTRACT_TYPE)) CONTRACT_TYPE
    , MAX (NVL (INITIAL_CREDIT, 0)) INITIAL_CREDIT
    , MAX (UPPER (ESSBASE_RATEPLAN)) ESSBASE_OFFRE
    , MAX ( UPPER ( NVL (ESSBASE_SEGMENTATION,  TRIM (CONCAT(
              CASE
                  WHEN customer_profile='Business' THEN  'BUS'
                  WHEN customer_profile='Grand public' THEN 'GP'
                  ELSE ' '
              END
            , CASE
                  WHEN customer_profile='Prepaid' THEN  'PREP'
                  WHEN customer_profile='Postpaid' THEN 'POST'
                  ELSE 'ND'
              END) ))))  ESSBASE_MARCHE
    , MAX (UPPER (OPERATOR_CODE)) OPERATOR_CODE
    FROM  dim.dt_offer_profiles b
    GROUP BY UPPER (b.PROFILE_CODE)
)

