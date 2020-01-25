INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    COMMERCIAL_OFFER_CODE
     , TRANSACTION_TYPE
     , SUB_ACCOUNT
     , TRANSACTION_SIGN
     , SOURCE_PLATFORM
     , SOURCE_DATA
     , SERVED_SERVICE
     , SERVICE_CODE
     , DESTINATION_CODE
     , SERVED_LOCATION
     , MEASUREMENT_UNIT
     , RATED_COUNT
     , RATED_VOLUME
     , TAXED_AMOUNT
     , UNTAXED_AMOUNT
     , INSERT_DATE
     , TRAFFIC_MEAN
     , OPERATOR_CODE
     , NULL LOCATION_CI
     , TRANSACTION_DATE
FROM(
        SELECT
            RECEIVER_PROFILE COMMERCIAL_OFFER_CODE
             ,(Case when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
                    when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
                    when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
                    when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
                    when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
                    else REFILL_MEAN end) TRANSACTION_TYPE
             ,(case when REFILL_MEAN='SCRATCH' then 'MAIN'
                    when REFILL_MEAN='C2S' then 'MAIN'
                    when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
                    else 'ZEBRA' end) SUB_ACCOUNT
             ,'0' TRANSACTION_SIGN
             , IF(REFILL_MEAN = 'SCRATCH','ICC','ZEBRA') SOURCE_PLATFORM
             , 'FT_REFILL' SOURCE_DATA
             , IF(REFILL_MEAN='SCRATCH','ICC_TRAFFIC','ZEBRA_TRAFFIC') SERVED_SERVICE
             , (Case when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
                     else ( CASE REFILL_MEAN
                                WHEN 'C2S' THEN 'NVX_C2S'
                                WHEN 'C2C' THEN 'NVX_C2C'
                                WHEN 'O2C' THEN 'NVX_O2C'
                                WHEN 'SCRATCH' THEN 'NVX_TOPUP'
                                ELSE REFILL_MEAN
                         END )
            end   ) SERVICE_CODE
             , 'DEST_ND' DESTINATION_CODE
             , NULL SERVED_LOCATION
             , 'HIT' MEASUREMENT_UNIT
             , SUM (1) RATED_COUNT
             , SUM (1) RATED_VOLUME
             , SUM (IF(refill_type='RETURN', -REFILL_AMOUNT, REFILL_AMOUNT)) TAXED_AMOUNT
             , SUM (IF(refill_type='RETURN', 0, (1-0.1925) * REFILL_AMOUNT)) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             ,'AIRTIME' TRAFFIC_MEAN
             , RECEIVER_OPERATOR_CODE OPERATOR_CODE
             , REFILL_DATE TRANSACTION_DATE
        FROM  MON.SPARK_FT_REFILL
        WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '200' AND REFILL_AMOUNT > 0
        GROUP BY
            RECEIVER_PROFILE
               ,(Case when REFILL_MEAN='SCRATCH' then 'VALEUR_FACIALE'
                      when REFILL_TYPE='RETURN' then 'RETRAIT_VALEUR_FACIALE'
                      when SENDER_MSISDN = '694010631' then 'RECHARGE_WHA'
                      when RECEIVER_MSISDN ='694010631' then 'TRANSFERT_WHA'
                      when REFILL_MEAN='C2S' and REFILL_TYPE='RC' and SENDER_MSISDN <>'694010631' and RECEIVER_MSISDN <>'694010631' then 'VALEUR_FACIALE'
                      else REFILL_MEAN end)
               ,(case when REFILL_MEAN='SCRATCH' then 'MAIN'
                      when REFILL_MEAN='C2S' then 'MAIN'
                      when REFILL_MEAN in ('C2C','O2C') and RECEIVER_MSISDN = '694010631' then 'WHA'
                      else 'ZEBRA' end)
               , IF(REFILL_MEAN = 'SCRATCH','ICC','ZEBRA')
               , IF(REFILL_MEAN='SCRATCH','ICC_TRAFFIC','ZEBRA_TRAFFIC')
               , (Case when (SENDER_MSISDN = '694010631' or RECEIVER_MSISDN = '694010631') then 'NVX_WHA'
                       else ( CASE REFILL_MEAN
                                  WHEN 'C2S' THEN 'NVX_C2S'
                                  WHEN 'C2C' THEN 'NVX_C2C'
                                  WHEN 'O2C' THEN 'NVX_O2C'
                                  WHEN 'SCRATCH' THEN 'NVX_TOPUP'
                                  ELSE REFILL_MEAN
                           END )
            end   )
               , RECEIVER_OPERATOR_CODE
               , REFILL_DATE
        UNION
        SELECT
            RECEIVER_PROFILE COMMERCIAL_OFFER_CODE
             ,'BONUS_RECHARGE' TRANSACTION_TYPE
             ,'PROMO' SUB_ACCOUNT
             ,'0' TRANSACTION_SIGN
             , IF(REFILL_MEAN = 'SCRATCH','ICC','ZEBRA') SOURCE_PLATFORM
             , 'FT_REFILL' SOURCE_DATA
             , IF(REFILL_MEAN='SCRATCH','ICC_TRAFFIC','ZEBRA_TRAFFIC') SERVED_SERVICE
             , IF(REFILL_MEAN='SCRATCH','NVX_TOPUP','NVX_C2S') SERVICE_CODE
             , 'DEST_ND' DESTINATION_CODE
             , NULL SERVED_LOCATION
             , 'HIT' MEASUREMENT_UNIT
             , SUM (0) RATED_COUNT
             , SUM (0) RATED_VOLUME
             , SUM (REFILL_BONUS) TAXED_AMOUNT
             , SUM ((1-0.1925) * REFILL_BONUS) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             ,'AIRTIME' TRAFFIC_MEAN
             , RECEIVER_OPERATOR_CODE OPERATOR_CODE
             , REFILL_DATE TRANSACTION_DATE
        FROM  MON.SPARK_FT_REFILL
        WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '200' AND REFILL_BONUS > 0  AND REFILL_MEAN in ('C2S','SCRATCH')
        GROUP BY
            RECEIVER_PROFILE
               , IF(REFILL_MEAN = 'SCRATCH','ICC','ZEBRA')
               , IF(REFILL_MEAN='SCRATCH','ICC_TRAFFIC','ZEBRA_TRAFFIC')
               , IF(REFILL_MEAN='SCRATCH','ICC_TRAFFIC','ZEBRA_TRAFFIC')
               , IF(REFILL_MEAN='SCRATCH','NVX_TOPUP','NVX_C2S')
               , RECEIVER_OPERATOR_CODE
               , REFILL_DATE

    )A
