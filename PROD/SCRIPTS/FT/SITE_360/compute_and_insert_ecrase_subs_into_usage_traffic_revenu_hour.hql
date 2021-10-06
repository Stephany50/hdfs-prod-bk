INSERT INTO MON.SPARK_FT_XXX2_HOUR
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
        substr(transaction_time, 1, 2) HOUR_PERIOD,
        MSISDN,
        BAL_ID,
        DA_TYPE,
        EVENT_DATE,
        (REVENU_FOR_BAL - REVENU_ALREADY_DISPATCHED) VALUE
    FROM
    (
        SELECT
            MSISDN,
            BAL_ID,
            TRANSACTION_TIME,
            EVENT_DATE,
            DA_TYPE,
            REVENU_FOR_BAL,
            REVENU_ALREADY_DISPATCHED,
            BAL_REVENU
        FROM
        (
            select
                B.MSISDN MSISDN,
                B.BAL_ID BAL_ID,
                B.transaction_time transaction_time,
                B.EVENT_DATE EVENT_DATE,
                E.DA_TYPE DA_TYPE,
                NVL(B.REVENU_FOR_BAL, 0) REVENU_FOR_BAL,
                NVL(C.REVENU_ALREADY_DISPATCHED, 0) REVENU_ALREADY_DISPATCHED,
                NVL(D.BAL_REVENU, 0) BAL_REVENU,
                ROW_NUMBER() OVER(PARTITION BY B.MSISDN, B.BAL_ID ORDER BY TO_TIMESTAMP(C.TRANSACTION_TIME) DESC) RANG
            from
            (

                select
                    msisdn,
                    bal_id,
                    bdle_name,
                    revenu_for_bal,
                    event_date,
                    ben_acct_id,
                    acct_res_rating_unit,
                    ben_acct_add_val,
                    transaction_time
                from
                (
                    SELECT
                        A1.msisdn msisdn,
                        A1.bal_id bal_id,
                        A1.bdle_name bdle_name,
                        A1.revenu_for_bal revenu_for_bal,
                        A1.event_date event_date,
                        A1.ben_acct_id ben_acct_id,
                        A1.acct_res_rating_unit acct_res_rating_unit,
                        A1.ben_acct_add_val ben_acct_add_val,
                        A1.transaction_time transaction_time,
                        ROW_NUMBER() over(partition by msisdn, bal_id order by TO_TIMESTAMP(TRANSACTION_TIME) desc) rang
                    FROM
                    (
                        SELECT
                        *
                        FROM MON.SPARK_FT_MSISDN_SUBS_BAL 
                        WHERE EVENT_DATE='###SLICE_VALUE###'
                    ) A1
                    LEFT JOIN DIM.DT_POLITIQUE_FORFAITS B 
                    ON NVL(UPPER(A1.BDLE_NAME), 'ND') = NVL(UPPER(B.OFFER_NAME), 'ND')
                    WHERE NVL(UPPER(B.POLITIC), 'ND') = 'ECRASE' 
                ) T
                where rang >= 2

            ) B
            LEFT JOIN MON.SPARK_FT_MSISDN_BAL_PPM C ON A3.EVENT_DATE=C.EVENT_DATE AND A3.MSISDN = C.MSISDN AND A3.BAL_ID = C.BAL_ID 
            LEFT JOIN MON.SPARK_FT_MSISDN_BAL_CONSTANTS D ON A3.EVENT_DATE=D.EVENT_DATE AND A3.MSISDN = D.MSISDN AND A3.BAL_ID = D.BAL_ID
            LEFT JOIN MON.SPARK_FT_MSISDN_DA_STATUS E ON A3.EVENT_DATE=E.EVENT_DATE AND A3.MSISDN = E.MSISDN AND A3.BAL_ID = E.BAL_ID 
        
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