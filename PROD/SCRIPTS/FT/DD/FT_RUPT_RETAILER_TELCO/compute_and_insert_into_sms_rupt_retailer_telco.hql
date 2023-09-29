INSERT INTO DD.SMS_RUPT_RETAILER_TELCO
SELECT
    A.MOBILE_NUMBER,
    -- '699947294' MOBILE_NUMBER,
    A.sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    A.EVENT_DATE,
    A.EVENT_TIME
FROM(
    SELECT *,
        CONCAT(
        -- ' \n' , 'MSISDN ',  MOBILE_NUMBER
        ' \n' , 'Date et Heure ',  CONCAT(DATE_FORMAT(EVENT_DATE, 'dd/MM/yyyy'), ' ', date_format(EVENT_TIME,'HH'), 'h00')
        , ' \n' ,'Vous êtes en rupture de stock'
        , ' \n' , 'Stock actuel : ',  stock
        , ' \n' , 'Vente Moyenne/heure :  ' , avg_amount_hour) sms
    FROM  DD.SPARK_FT_RUPT_RETAILER_TELCO
    WHERE event_date=CURRENT_DATE()
)A
INNER JOIN
(
    SELECT distinct EVENT_DATE, event_time FROM DD.SPARK_FT_RUPT_RETAILER_TELCO where event_date=CURRENT_DATE() order by event_date desc, event_time desc LIMIT 1
) B
ON A.EVENT_DATE=B.EVENT_DATE AND A.EVENT_TIME=B.EVENT_TIME
WHERE RUPT_HOUR_MSISDN = 1