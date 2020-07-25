INSERT INTO MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY PARTITION(PERIOD)
            SELECT
            SERVED_PARTY_MSISDN AS MSISDN,
            SUM(RATED_AMOUNT) AS BDLE_COST, --Prise en compte des couts souscriptions via OM: Channel Id 32
            COUNT(*) AS NBER_PURCHASE,
            SUBSCRIPTION_SERVICE_DETAILS AS BDLE_NAME,

            (CASE WHEN UPPER(SUBSCRIPTION_SERVICE_DETAILS) LIKE '%JRS' then cast(SUBSTR(SUBSCRIPTION_SERVICE_DETAILS, -5, 2) AS BIGINT)
                ELSE  datediff(EXPIRE_DATE,ACTIVE_DATE)
                end) Validity,
            SUBSCRIPTION_CHANNEL,
            SUM(AMOUNT_VOICE_ONNET) AS AMOUNT_VOICE_ONNET,
            SUM(AMOUNT_VOICE_OFFNET)  AS AMOUNT_VOICE_OFFNET,
            SUM(AMOUNT_VOICE_INTER)  AS AMOUNT_VOICE_INTER,
            SUM(AMOUNT_VOICE_ROAMING)  AS AMOUNT_VOICE_ROAMING,
            SUM(AMOUNT_SMS_ONNET)  AS AMOUNT_SMS_ONNET,
            SUM(AMOUNT_SMS_OFFNET)  AS AMOUNT_SMS_OFFNET,
            SUM(AMOUNT_SMS_INTER)  AS AMOUNT_SMS_INTER,
            SUM(AMOUNT_SMS_ROAMING)  AS AMOUNT_SMS_ROAMING,
            SUM(AMOUNT_DATA) AS AMOUNT_DATA,
            SUM(AMOUNT_SVA)  AS AMOUNT_SVA,
            current_timestamp AS INSERT_DATE,
            BENEFIT_BAL_LIST,
            BAL_ID,
            '###SLICE_VALUE###' AS PERIOD


            FROM MON.SPARK_FT_SUBSCRIPTION
            LEFT JOIN (SELECT * FROM dim.ref_souscription_price) A
            ON UPPER(SUBSCRIPTION_SERVICE_DETAILS) = UPPER(A.IPP_NAME)
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
            AND INSERT_DATE IN ( SELECT MIN(INSERT_DATE) AS FIRST_DATE
                                FROM MON.SPARK_FT_SUBSCRIPTION
                                WHERE TRANSACTION_DATE = '###SLICE_VALUE###')
            GROUP BY
            SERVED_PARTY_MSISDN,
            SUBSCRIPTION_SERVICE_DETAILS,
            SUBSCRIPTION_CHANNEL,
            BENEFIT_BAL_LIST,
            (CASE WHEN UPPER(SUBSCRIPTION_SERVICE_DETAILS) LIKE '%JRS' then cast(SUBSTR(SUBSCRIPTION_SERVICE_DETAILS, -5, 2) AS BIGINT)
                ELSE  datediff(EXPIRE_DATE,ACTIVE_DATE)
                end),
            BAL_ID
