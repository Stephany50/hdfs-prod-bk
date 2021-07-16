INSERT INTO DD.SMS_RUPT_RETAILER_OM
SELECT
--     A.MOBILE_NUMBER,
    '699942700' MOBILE_NUMBER,
    A.sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    A.EVENT_DATE,
    A.EVENT_TIME
FROM(
    SELECT *,
        CONCAT(
        DATE_FORMAT(EVENT_DATE,'dd/MM')
        , ' \n' ,event_time
        , ' \n' , 'MSISDN ',  MOBILE_NUMBER
        , ' \n' , 'Bonjour ',  partenaire
        , ' \n' ,'Vous êtes en rupture de stock'
        , ' \n' , 'Stock restant(EN FCFA): ',  stock
        , ' \n' , 'Stock moyen(EN FCFA): ' , average_hour_amount
        , ' \n' ,'Bien vouloir recharcher ') sms
    FROM  DD.SPARK_FT_RUPT_RETAILER_OM
--     WHERE RUPTURE_HOUR_MSISDN = 1 and EVENT_DATE IN (SELECT MAX(EVENT_DATE) FROM DD.SPARK_FT_RUPT_RETAILER_OM)
--         AND EVENT_TIME IN (SELECT MAX(EVENT_TIME) FROM DD.SPARK_FT_RUPT_RETAILER_OM WHERE EVENT_DATE IN (SELECT MAX(EVENT_DATE) FROM DD.SPARK_FT_RUPT_RETAILER_OM))
)A
INNER JOIN
(
    SELECT distinct EVENT_DATE, event_time FROM DD.SPARK_FT_RUPT_RETAILER_OM order by event_date desc, event_time desc LIMIT 1
) B
ON A.EVENT_DATE=B.EVENT_DATE AND A.EVENT_TIME=B.EVENT_TIME
WHERE RUPTURE_HOUR_MSISDN = 1
limit 10