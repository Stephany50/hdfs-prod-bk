INSERT INTO DD.SMS_RUPT_COMMERCIAL_RETAILER_OM
SELECT
    COMMERCIAL MOBILE_NUMBER,
    -- '699947294' MOBILE_NUMBER,
    CONCAT(
        -- ' \n' , 'MSISDN ', COMMERCIAL
        ' \n' , 'Date et Heure ',  CONCAT(DATE_FORMAT(EVENT_DATE, 'dd/MM'), ' ', SUBSTR(EVENT_TIME, 1, 2), 'h00')
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
        FROM  DD.SPARK_FT_RUPT_RETAILER_OM
        WHERE EVENT_DATE=CURRENT_DATE() AND RUPTURE_HOUR_MSISDN = 1
        GROUP BY MOBILE_NUMBER
    )A
    LEFT JOIN
    (
        SELECT
            DISTINCT MSISDN POINT_DE_VENTE,
            PARENT_USER_MSISDN COMMERCIAL,
            OWNER_MSISDN PARTENAIRE,
            OWNER_LAST_NAME NOMS_PARTENAIRE
        FROM CDR.SPARK_IT_OM_ALL_USERS
        WHERE ORIGINAL_FILE_DATE IN (SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.SPARK_IT_OM_ALL_USERS)
    ) C
    ON A.MOBILE_NUMBER = C.POINT_DE_VENTE
    GROUP BY C.COMMERCIAL
) D
