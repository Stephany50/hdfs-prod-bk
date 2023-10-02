INSERT INTO DD.SMS_RUPT_COMMERCIAL_RETAILER_TELCO
SELECT
    COMMERCIAL MOBILE_NUMBER,
    -- '699947294' MOBILE_NUMBER,
    CONCAT(
        -- ' \n' , 'MSISDN ', COMMERCIAL
        ' \n' , 'Date et Heure ',  CONCAT(DATE_FORMAT(EVENT_DATE, 'dd/MM/yyyy'), ' ', date_format(EVENT_TIME,'HH'), 'h00')
        , ' \n' ,'Récapitulatif de vos points de vente en rupture de stock '
        , ' \n' ,
        CONCAT_WS('\n', LISTE_POINT_DE_VENTE)) sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    EVENT_DATE,
    EVENT_TIME
FROM
(
    SELECT
        C.COMMERCIAL COMMERCIAL,
        COLLECT_LIST(CONCAT_WS(' : ', A.MOBILE_NUMBER, A.NBR)) LISTE_POINT_DE_VENTE,
        MAX(A.EVENT_DATE) EVENT_DATE,
        MAX(A.EVENT_TIME) EVENT_TIME
    FROM(
        SELECT
            MOBILE_NUMBER,
            COUNT(*) NBR,
            MAX(EVENT_DATE) EVENT_DATE,
            MAX(EVENT_TIME) EVENT_TIME
        FROM  DD.SPARK_FT_RUPT_RETAILER_TELCO
        WHERE EVENT_DATE=CURRENT_DATE() AND rupt_hour_msisdn = 1
        GROUP BY MOBILE_NUMBER
    )A
    INNER JOIN
    (
        SELECT distinct EVENT_DATE, event_time FROM DD.SPARK_FT_RUPT_RETAILER_TELCO where event_date=CURRENT_DATE() order by event_date desc, event_time desc LIMIT 1
    ) B
    ON A.EVENT_DATE=B.EVENT_DATE AND A.EVENT_TIME=B.EVENT_TIME
    LEFT JOIN
    (
        SELECT
            DISTINCT MSISDN POINT_DE_VENTE,
            PARENT COMMERCIAL
        FROM 
        (
            SELECT
                E.PRIMARY_MSISDN  MSISDN,
                E.CAT_CODE CATEGORY_CODE,
                E.CATEGORY_NAME,
                E.GEOGRAPHICAL_DOMAIN_CODE,
                E.GEOGRAPHICAL_DOMAIN_NAME,
                E.CHANNEL_USER_NAME,
                F.PRIMARY_MSISDN PARENT,
                G.PRIMARY_MSISDN GRDPARENT,
                E.TRANSACTION_DATE  ACTIV_BEGIN_DATE
            FROM
            (
                SELECT
                    PRIMARY_MSISDN,
                    MAX(CHANNEL_USER_ID) CHANNEL_USER_ID,
                    MAX(PARENT_USER_ID) PARENT_USER_ID,
                    MAX(OWNER_USER_ID) OWNER_USER_ID,
                    MAX(CHANNEL_USER_NAME) CHANNEL_USER_NAME,
                    MAX(CATEGORY_CODE) CAT_CODE,
                    MAX(CATEGORY_NAME) CATEGORY_NAME,
                    MAX(GEOGRAPHICAL_DOMAIN_CODE) GEOGRAPHICAL_DOMAIN_CODE,
                    MAX(GEOGRAPHICAL_DOMAIN_NAME) GEOGRAPHICAL_DOMAIN_NAME,
                    MAX(TRANSACTION_DATE) TRANSACTION_DATE
                FROM CDR.SPARK_IT_ZEBRA_MASTER
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                    AND USER_STATUS = 'Active'
                GROUP BY PRIMARY_MSISDN
            ) E
            LEFT JOIN
            (
                SELECT
                    PRIMARY_MSISDN,
                    MAX(CHANNEL_USER_ID) CHANNEL_USER_ID,
                    MAX(PARENT_USER_ID) PARENT_USER_ID,
                    MAX(OWNER_USER_ID) OWNER_USER_ID,
                    MAX(CHANNEL_USER_NAME) CHANNEL_USER_NAME,
                    MAX(GEOGRAPHICAL_DOMAIN_CODE) GEOGRAPHICAL_DOMAIN_CODE,
                    MAX(GEOGRAPHICAL_DOMAIN_NAME) GEOGRAPHICAL_DOMAIN_NAME
                FROM CDR.SPARK_IT_ZEBRA_MASTER
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                    AND USER_STATUS = 'Active'
                GROUP BY PRIMARY_MSISDN
            ) F
            ON E.PARENT_USER_ID = F.CHANNEL_USER_ID
            LEFT JOIN
            (
                SELECT
                    PRIMARY_MSISDN,
                    MAX(CHANNEL_USER_ID) CHANNEL_USER_ID,
                    MAX(PARENT_USER_ID) PARENT_USER_ID,
                    MAX(OWNER_USER_ID) OWNER_USER_ID,
                    MAX(CHANNEL_USER_NAME) CHANNEL_USER_NAME,
                    MAX(GEOGRAPHICAL_DOMAIN_CODE) GEOGRAPHICAL_DOMAIN_CODE,
                    MAX(GEOGRAPHICAL_DOMAIN_NAME) GEOGRAPHICAL_DOMAIN_NAME
                FROM CDR.SPARK_IT_ZEBRA_MASTER
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                    AND USER_STATUS = 'Active'
                GROUP BY PRIMARY_MSISDN
            ) G
            ON E.OWNER_USER_ID = G.CHANNEL_USER_ID
        ) zebra
    ) C
    ON A.MOBILE_NUMBER = C.POINT_DE_VENTE
    GROUP BY C.COMMERCIAL
) D
