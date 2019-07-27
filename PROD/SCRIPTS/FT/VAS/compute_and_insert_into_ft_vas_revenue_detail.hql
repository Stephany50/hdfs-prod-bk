INSERT INTO MON.FT_VAS_REVENUE_DETAIL PARTITION(TRANSACTION_DATE)
SELECT
    from_unixtime(unix_timestamp(concat(TRANSACTION_DATE, lpad(TRANSACTION_TIME, 6, 0)), 'yyyy-MM-ddHHmmss')) NQ_TRANSACTION_DATE,
    SERVED_PARTY,
    OTHER_PARTY,
    RATED_DURATION,
    CALL_PROCESS_TOTAL_DURATION,
    RATED_VOLUME,
    UNIT_OF_MEASUREMENT,
    SERVICE_CODE,
    TELESERVICE_INDICATOR,
    NETWORK_EVENT_TYPE,
    NETWORK_ELEMENT_ID,
    OTHER_PARTY_ZONE,
    CALL_DESTINATION_CODE,
    BILLING_TERM_INDICATOR,
    NETWORK_TERM_INDICATOR,
    SERVED_PARTY_IMSI,
    RAW_SPECIFIC_CHARGINGINDICATOR,
    RAW_LOAD_LEVEL_INDICATOR,
    FEE_NAME,
    REFILL_TOPUP_PROFILE,
    MAIN_REFILL_AMOUNT,
    BUNDLE_REFILL_AMOUNT,
    RAW_TRANSNUM_USED_FOR_REFILL,
    COMMERCIAL_OFFER,
    COMMERCIAL_PROFILE,
    LOCATION_MCC,
    LOCATION_MNC,
    LOCATION_LAC,
    LOCATION_CI,
    MAIN_RATED_AMOUNT,
    PROMO_RATED_AMOUNT,
    BUNDLE_IDENTIFIER,
    BUNDLE_UNIT,
    BUNDLE_CONSUMED_VOLUME,
    BUNDLE_DISCARDED_VOLUME,
    BUNDLE_REMAINING_VOLUME,
    BUNDLE_REFILL_VOLUME,
    RATED_AMOUNT_IN_BUNDLE,
    MAIN_REMAINING_CREDIT,
    PROMO_REMAINING_CREDIT,
    SPECIFIC_TARIFF_INDICATOR,
    LOCATION_NUMBER,
    MAIN_DISCARDED_CREDIT,
    PROMO_DISCARDED_CREDIT,
    SMS_DISCARDED_VOLUME,
    SMS_USED_VOLUME,
    RAW_TARIFF_PLAN,
    RAW_EVENT_COST,
    RAW_REFILL_MEANS,
    RAW_CALL_TYPE,
    ORIGINAL_FILE_NAME,
    'IN' SOURCE_PLATEFORM,
    'IN' SOURCE_DATA,
    CURRENT_TIMESTAMP INSERT_DATE,
    TRANSACTION_DATE
FROM
    mon.FT_BILLED_TRANSACTION_PREPAID  a
    LEFT JOIN (
        SELECT 
            FN_FORMAT_MSISDN_TO_9DIGITS(vas_number)vas_number
        FROM
        (
            SELECT NUMERO_COURT  vas_number FROM dim.dt_service_offert
            UNION SELECT NUMERO_LONG  vas_number  FROM dim.dt_service_offert
            UNION SELECT SHORT_LONG_NUMBER vas_number FROM dim.DT_VAS_OPERATOR
            UNION SELECT Vas_Number  FROM DIM.DT_VAS_PARTNER
            UNION SELECT REPLACE (SHORT_NUMBER, '*', '') Vas_Number FROM DIM.DMP_SHORT_CODES
            UNION SELECT REPLACE (LONG_NUMBER, '*', '') Vas_Number FROM DIM.DMP_SHORT_CODES
        )  T
        WHERE 
            vas_number IS NOT NULL
        GROUP BY 
            FN_FORMAT_MSISDN_TO_9DIGITS(vas_number)
    ) b on (a.OTHER_PARTY = b.vas_number)
WHERE
    a.TRANSACTION_DATE = "###SLICE_VALUE###"
    AND Main_Rated_Amount >= 0
    AND Promo_Rated_Amount >= 0
    AND
    (CASE
        WHEN NVL (b.vas_number,  'ND') <> 'ND' THEN 1
        WHEN Call_Destination_Code IN ('VAS') THEN 1    
        WHEN (Call_Destination_Code IN ('EMERG') AND  NVL (MAIN_RATED_AMOUNT, 0) + NVL (PROMO_RATED_AMOUNT, 0) > 0)   THEN 1    
        WHEN (LENGTH (a.OTHER_PARTY) <= 5 AND NVL (MAIN_RATED_AMOUNT, 0)  + NVL (PROMO_RATED_AMOUNT, 0) > 0) THEN 1
        WHEN UPPER (Other_Party_Zone) LIKE '%INDIGO%' THEN 1
        WHEN UPPER (Other_Party_Zone) LIKE '%AZUR%' THEN 1
        WHEN UPPER (Other_Party_Zone) LIKE '%PREMIUM%' THEN 1
        WHEN UPPER (Other_Party_Zone) LIKE '%GRATUIT%' THEN 1
        WHEN UPPER (Other_Party_Zone) LIKE '%ACCESYOR%' THEN 1
        WHEN UPPER (Other_Party_Zone) IN  ('OCM_O' , 'TCHAT')  THEN 1 
        WHEN (UPPER (Other_Party_Zone) LIKE '%URGENCE%' AND NVL (MAIN_RATED_AMOUNT, 0)  + NVL (PROMO_RATED_AMOUNT, 0) > 0)   THEN 1
        ELSE 0
    END ) = 1     