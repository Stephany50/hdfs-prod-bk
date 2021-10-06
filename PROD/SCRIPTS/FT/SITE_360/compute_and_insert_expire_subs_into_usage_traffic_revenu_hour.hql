INSERT INTO MON.SPARK_FT_XXX1_HOUR
SELECT
    T.MSISDN MSISDN,
    T.BAL_ID BAL_ID,
    T.HOUR_PERIOD HOUR_PERIOD,
    0 TRAFIC_VOIX, 
    0 TRAFIC_DATA, 
    0 TRAFIC_SMS, 
    0 REVENU_VOIX_PYG,
    CASE
        WHEN DA_TYPE='TEL' THEN VALUE
    ELSE 0 END REVENU_VOIX_SUBS,
    CASE
        WHEN DA_TYPE='DATA' THEN VALUE
        ELSE 0 END REVENU_DATA,
    0 REVENU_SMS_PYG,
    CASE
        WHEN DA_TYPE='SMS' THEN VALUE
    ELSE 0 END REVENU_SMS_SUBS,
    SITE_NAME,
    TOWN,
    REGION,
    COMMERCIAL_REGION,
    CURRENT_TIMESTAMP INSERT_DATE,
    EST_PARC_GROUPE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT 
        '23' HOUR_PERIOD,
        MSISDN,
        BAL_ID,
        DA_TYPE,
        EVENT_DATE,
        CASE
            WHEN NVL(VOLUME_REMAINING, 0)=0 THEN BAL_REVENU
            ELSE REVENU_FOR_BAL - REVENU_ALREADY_DISPATCHED END
        VALUE
    FROM
    (
        SELECT
            MSISDN,
            BAL_ID,
            DA_NAME,
            DA_TYPE,
            DA_UNIT,
            VOLUME_REMAINING,
            EVENT_DATE,
            REVENU_FOR_BAL,
            REVENU_ALREADY_DISPATCHED,
            BAL_REVENU
        FROM
        (
            SELECT
                A3.MSISDN MSISDN,
                A3.BAL_ID BAL_ID,
                A3.DA_NAME DA_NAME,
                A3.DA_TYPE DA_TYPE,
                A3.DA_UNIT DA_UNIT,
                A3.VOLUME_REMAINING VOLUME_REMAINING,
                A3.EVENT_DATE EVENT_DATE,
                NVL(B.REVENU_FOR_BAL, 0) REVENU_FOR_BAL,
                NVL(C.REVENU_ALREADY_DISPATCHED, 0) REVENU_ALREADY_DISPATCHED,
                NVL(D.BAL_REVENU, 0) BAL_REVENU,
                ROW_NUMBER() OVER(PARTITION BY A3.MSISDN, A3.BAL_ID ORDER BY TO_TIMESTAMP(B.TRANSACTION_TIME) DESC) RANG
            FROM
            (
                SELECT
                    MSISDN,
                    BAL_ID,
                    DA_NAME,
                    VOLUME_REMAINING,
                    DA_TYPE,
                    DA_UNIT,
                    EVENT_DATE
                FROM
                (
                    SELECT
                    A1.*,
                    ROW_NUMBER() OVER(PARTITION BY MSISDN, BAL_ID ORDER BY EVENT_DATE DESC) RANG
                    FROM
                    (
                        SELECT
                            A11.MSISDN MSISDN,
                            A11.BAL_ID BAL_ID,
                            A11.DA_NAME DA_NAME,
                            NVL(A12.VOLUME_REMAINING, 0) VOLUME_REMAINING,
                            A11.DA_TYPE DA_TYPE,
                            A11.DA_UNIT DA_UNIT,
                            A11.EXPIRY_DATE EXPIRY_DATE,
                            A11.EFF_DATE EFF_DATE,
                            A12.EVENT_DATE EVENT_DATE
                        FROM
                        (
                            SELECT 
                            *
                            FROM MON.SPARK_FT_MSISDN_DA_STATUS 
                            WHERE EVENT_DATE='###SLICE_VALUE###' AND
                                TO_DATE(EXPIRY_DATE)='###SLICE_VALUE###' AND
                                NVL(VOLUME_REMAINING, 0) <= 0
                        ) A11
                        LEFT JOIN MON.SPARK_FT_MSISDN_DA_STATUS A12 
                        ON 
                            A12.EVENT_DATE BETWEEN TO_DATE(A11.EFF_DATE) AND DATE_SUB(TO_DATE(A11.EXPIRY_DATE), 1) AND 
                            A11.MSISDN = A12.MSISDN AND 
                            A11.BAL_ID = A12.BAL_ID
                        WHERE NVL(A12.VOLUME_REMAINING, 0) >= 0
                    ) A1 
                ) A2 WHERE RANG = 1
            ) A3
            LEFT JOIN MON.SPARK_FT_MSISDN_SUBS_BAL B ON A3.EVENT_DATE=B.EVENT_DATE AND A3.MSISDN = B.MSISDN AND A3.BAL_ID = B.BAL_ID 
            LEFT JOIN MON.SPARK_FT_MSISDN_BAL_PPM C ON A3.EVENT_DATE=C.EVENT_DATE AND A3.MSISDN = C.MSISDN AND A3.BAL_ID = C.BAL_ID 
            LEFT JOIN MON.SPARK_FT_MSISDN_BAL_CONSTANTS D ON A3.EVENT_DATE=D.EVENT_DATE AND A3.MSISDN = D.MSISDN AND A3.BAL_ID = D.BAL_ID 

        ) T WHERE RANG = 1
    ) A 
    RIGHT JOIN
    (
        SELECT
            MSISDN,
            HOUR_PERIOD,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION,
            EST_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' 
    ) B 
    ON A.MSISDN = B.MSISDN AND A.HOUR_PERIOD = B.HOUR_PERIOD
) T

