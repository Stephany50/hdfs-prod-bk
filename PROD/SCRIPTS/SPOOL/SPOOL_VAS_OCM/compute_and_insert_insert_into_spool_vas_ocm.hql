INSERT INTO SPOOL.SPOOL_VAS_OCM

SELECT

    MSISDN,
    SERVICE,
    CODE,
    PARTNER,
    BILLING,
    'DAILY' BUNDLE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' TRANSACTION_DATE

FROM

    (SELECT
        A.MSISDN AS MSISDN,
        B.SERVICE AS SERVICE,
        A.CODE AS CODE,
        B.PARTNER AS PARTNER,
        NVL(A.BILLING,0) AS BILLING

    FROM
        (
        SELECT
            served_party MSISDN,
            other_party CODE,
            main_rated_amount BILLING

        FROM MON.SPARK_FT_VAS_REVENUE_DETAIL
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
        ) A
        LEFT JOIN
        (
        SELECT
            SERVICE,
            PARTNER,
            CODE

        FROM
        (
            SELECT
                NUMERO_COURT CODE,
                INTITULE_SERVICE SERVICE,
                NULL PARTNER

            FROM dim.dt_service_offert


            UNION

            SELECT
                NUMERO_LONG CODE,
                INTITULE_SERVICE  SERVICE,
                NULL PARTNER

            FROM dim.dt_service_offert

            UNION

            SELECT
                SHORT_LONG_NUMBER CODE,
                NULL SERVICE,
                operator_name PARTNER

            FROM dim.DT_VAS_OPERATOR

            UNION

            SELECT
                Vas_Number CODE,
                SERVICE_NAME SERVICE,
                PARTNER_NAME PARTNER

            FROM DIM.DT_VAS_PARTNER

            UNION

            SELECT
                REPLACE (SHORT_NUMBER, '*', '') CODE,
                SERVICE_NAME SERVICE,
                NULL PARTNER

            FROM DIM.DMP_SHORT_CODES

            UNION

            SELECT
                REPLACE (LONG_NUMBER, '*', '') CODE,
                SERVICE_NAME SERVICE,
                NULL PARTNER

            FROM DIM.DMP_SHORT_CODES
            )TTT
        ) B
        ON A.CODE = B.CODE
    ) TTTTT



